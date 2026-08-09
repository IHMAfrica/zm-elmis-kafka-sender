# ELMIS Kafka Sender

A reactive Spring Boot service that streams patient profiles and ARV prescription data from the
SmartCare **CarePro** SQL Server database to **ELMIS** (Electronic Logistics Management Information
System) over Kafka.

The service runs headlessly — there are no business HTTP endpoints. It continuously polls two source
tables, transforms unsent rows into ELMIS JSON messages, publishes them to Kafka, and marks the
source rows as synced.

---

## How it works

```
 SQL Server (CarePro)                    elmis-kafka-sender                       Kafka (ELMIS)
 ┌────────────────────┐    poll     ┌──────────────────────────┐   publish   ┌──────────────────┐
 │ dbo.ElmisLogs      │────────────>│ ElmisSenderService       │────────────>│ prescriptions    │
 │  (IsSynced = 0)    │             │  ├─ validate             │             │ topic            │
 │                    │<────────────│  ├─ group by prescription│             ├──────────────────┤
 │  mark IsSynced = 1 │   update    │  ├─ PayloadBuilderService│────────────>│ patient-profiles │
 ├────────────────────┤             │  └─ KafkaProducerService │             │ topic            │
 │ dbo.Clients        │────────────>│                          │             └──────────────────┘
 │ (ELMISSyncStatus=0)│    poll     └──────────────────────────┘
 └────────────────────┘
```

Two independent polling loops start on application boot (`@PostConstruct`) and shut down cleanly on
`@PreDestroy`:

### 1. Prescription loop

1. `ElmisLogRepository.findUnprocessedRecords()` selects up to `batch-size` rows from `dbo.ElmisLogs`
   where `IsSynced` is null/0 and `IsDeleted` is null/0, using `WITH (READPAST)` so concurrent
   replicas don't block each other on locked rows. Quarantined prescriptions (see below) are
   excluded.
2. Each row is validated. Only rows that cannot be routed at all — missing `Oid`,
   `PrescriptionUuid` or `PatientUuid` — are rejected and marked synced so they are skipped
   permanently. A missing `PatientId` (NUPN) or `RegistrationDateTime` does **not** reject the row:
   it only makes the *patient profile* unsendable, and the prescription still goes out.
3. Valid rows are grouped by `PrescriptionUuid` (one prescription spans multiple drug rows).
4. For each group: the **patient profile** message is published first, then the **prescription**
   message. The profile is strictly best-effort — if it is incomplete or its send fails, that is
   logged and the prescription is published anyway. Profiles are de-duplicated per batch via an
   in-memory `sentPatientProfiles` set.
5. Only rows whose prescription was acknowledged by Kafka are marked `IsSynced = 1`.

### Quarantine (head-of-line protection)

A prescription whose payload cannot be built, or whose Kafka send fails, is placed in an in-memory
quarantine keyed by `PrescriptionUuid` and excluded from subsequent poll queries until an
exponential backoff expires (`quarantine-base-backoff-ms` doubling up to `quarantine-max-backoff-ms`).

This exists because the poll query takes the *lowest* `batch-size` unsent rows by `Oid`. Without
quarantine, a prescription that can never be sent is re-read on every poll forever; once enough of
them accumulate to fill the batch, the pipeline stalls completely and no new prescriptions are sent.

Quarantine **defers work, it never discards it**: quarantined rows keep `IsSynced = 0`, are retried
when the backoff expires, and are retried from scratch after a restart. The same mechanism protects
the client loop, keyed by client `Oid`.

### 2. Client profile loop

Polls `dbo.Clients` (joined to `dbo.Facilities` for the HMIS code) where `ELMISSyncStatus` is null/0,
builds a patient-profile message per client, publishes it, and sets `ELMISSyncStatus = 1`. Runs at a
concurrency of 5. This covers patients registered without a prescription.

### Adaptive polling

Both loops poll every `interval-ms`. After more than 5 consecutive empty polls they back off to
`idle-interval-ms`. On a loop-level error the loop is restarted after a 5-second delay.

### Payload notes (`PayloadBuilderService`)

- Every message carries an `msh` header block: `timestamp`, `sendingApplication` (`CarePro`),
  `receivingApplication` (`elmis`), a random `messageId`, `hmisCode`/`mflCode`, and `messageType`
  (`profile` or `prescription`).
- Timestamps are formatted `yyyy-MM-dd HH:mm:ss` and **shifted +2 hours** (`LocalDateTime.now().plusHours(2)`)
  to compensate for the container running in UTC while ELMIS expects CAT.
- A prescription splits into a `regimen` block (the row flagged `SpecialDrug = 1`) and a
  `prescription.prescriptionDrugs` array (all other rows), plus a `vitals` block that is emitted as
  empty strings when no height, weight or blood pressure was recorded.
- PrEP/PEP prescriptions (regimen IDs resolved once at startup from `dbo.SpecialDrugs` joined to
  `dbo.DrugRegimens` where `Oid = 14`) get a generated pseudo ART number of the form `sp-XXXXX`
  instead of the patient's real ART number.
- `Sex` is mapped to `M` when the source value is `1`, otherwise `F`.

---

## Tech stack

| Concern        | Choice                                                          |
| -------------- | --------------------------------------------------------------- |
| Language       | Java 21                                                         |
| Framework      | Spring Boot 4.0.1 (WebFlux, Actuator)                           |
| Database       | SQL Server via R2DBC (`io.r2dbc:r2dbc-mssql`), raw `DatabaseClient` SQL |
| Messaging      | `reactor-kafka` 1.3.22 (`KafkaSender`), SASL_PLAINTEXT / SCRAM-SHA-256 |
| Serialization  | Jackson (`lowerCamelCase`, ISO dates, JSR-310)                  |
| Build          | Gradle Kotlin DSL + wrapper                                     |
| Boilerplate    | Lombok                                                          |
| Metrics        | Micrometer → Prometheus                                         |

---

## Project layout

```
src/main/java/zm/gov/moh/elmiskafkasender/
├── ElmisKafkaSenderApplication.java   # entry point (@EnableScheduling)
├── config/
│   ├── KafkaConfig.java               # reactor-kafka SenderOptions, ObjectMapper
│   └── R2dbcConfig.java               # DatabaseClient bean
├── entity/
│   ├── ElmisLogRecord.java            # row of dbo.ElmisLogs
│   └── ClientRecord.java              # row of dbo.Clients + facility HMIS code
├── repository/
│   ├── ElmisLogRepository.java        # prescription queries + mark-as-synced
│   └── ClientRepository.java          # client queries + mark-as-synced
├── service/
│   ├── ElmisSenderService.java        # polling loops, validation, orchestration
│   ├── PayloadBuilderService.java     # JSON payload construction
│   └── KafkaProducerService.java      # reactive publish to the two topics
└── dto/                               # Msh, PatientProfile, Prescription, Regimen, Vitals, …

bundles/                               # Fleet (Rancher) GitOps bundles
├── moh/                               # MoH deployment — namespace elmis-kafka-sender
└── dfz/                               # DFZ deployment — namespace elmis-kafka-sender-dfz
```

---

## Configuration

All connection details come from environment variables — nothing is hard-coded in
`application.yaml`.

| Variable                  | Description                                                   |
| ------------------------- | ------------------------------------------------------------- |
| `DB_HOST`                 | SQL Server host                                               |
| `DB_PORT`                 | SQL Server port                                               |
| `DB_NAME`                 | Database name                                                 |
| `DB_USERNAME`             | Database user                                                 |
| `DB_PASSWORD`             | Database password                                             |
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka bootstrap servers                                       |
| `KAFKA_JAAS_CONFIG`       | Full `sasl.jaas.config` string (SCRAM-SHA-256 credentials)    |
| `PRESCRIPTIONS_TOPIC`     | Topic for prescription messages                               |
| `PATIENT_PROFILES_TOPIC`  | Topic for patient profile messages                            |

Tunables in `src/main/resources/application.yaml`:

```yaml
elmis:
  polling:
    interval-ms: 500                    # poll interval while work is available
    idle-interval-ms: 2000              # back-off interval after >5 empty polls
    batch-size: 100                     # rows fetched per poll
    quarantine-base-backoff-ms: 30000   # first retry delay for a failing record
    quarantine-max-backoff-ms: 1800000  # ceiling on the exponential retry delay
```

Connection pool: initial size 5, max 20, max idle 30m. Kafka producer: `acks=1`,
`delivery.timeout.ms=280000`, `request.timeout.ms=30000`, `max.block.ms=80000`, unbounded retries
bounded by the delivery timeout, 500 ms retry backoff.

---

## Running locally

Requires JDK 21, a reachable SQL Server instance and a Kafka cluster.

```bash
export DB_HOST=localhost DB_PORT=1433 DB_NAME=SmartCare \
       DB_USERNAME=sa DB_PASSWORD='***' \
       KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
       KAFKA_JAAS_CONFIG='org.apache.kafka.common.security.scram.ScramLoginModule required username="user" password="pass";' \
       PRESCRIPTIONS_TOPIC=elmis.prescriptions \
       PATIENT_PROFILES_TOPIC=elmis.patient-profiles

./gradlew bootRun
```

Other useful tasks:

```bash
./gradlew build      # compile + test
./gradlew test       # tests only
./gradlew bootJar    # produce build/libs/*.jar
```

### Docker

Multi-stage build (`gradle:9.2.1-jdk21` → `eclipse-temurin:21-jre-alpine`):

```bash
docker build -t elmis-kafka-sender .
docker run --env-file .env elmis-kafka-sender
```

---

## Observability

Actuator exposes `health`, `info`, `metrics` and `prometheus`, with full health details enabled.

The service also tracks in-process counters, logged on shutdown: prescriptions sent, profiles sent
(from prescriptions), client profiles sent, errors, records skipped as invalid, and records skipped
as incomplete profiles.

---

## Deployment

### CI

`.github/workflows/ci-cd.yaml` runs on pushes and PRs to `main`: it builds the Docker image with
Buildx (GitHub Actions cache) and pushes it to GHCR tagged `latest`, the branch name, and the short
SHA.

### CD

Deployment is GitOps via Rancher Fleet. Each directory under `bundles/` is a self-contained bundle
with its own `fleet.yaml` and a copy of the Helm chart:

| Bundle       | Namespace                   | Release                  | Credentials secret                   |
| ------------ | --------------------------- | ------------------------ | ------------------------------------ |
| `bundles/moh` | `elmis-kafka-sender`       | `elmis-kafka-sender`     | `elmis-kafka-sender-credentials`     |
| `bundles/dfz` | `elmis-kafka-sender-dfz`   | `elmis-kafka-sender-dfz` | `elmis-kafka-sender-dfz-credentials` |

Every environment variable listed above is injected from the single Kubernetes secret named by
`dbCredentialsSecret`, so that secret must contain all nine keys before the pod will start. Image:
`ghcr.io/ihmafrica/zm-elmis-kafka-sender:latest`.

---

## Operational notes

- **At-least-once delivery.** Rows are marked synced only after Kafka acknowledges, so a crash
  between publish and update results in a duplicate message, never a lost one. Consumers should be
  idempotent on `prescriptionUuid` / `patientUuid`.
- **Prescriptions are never withheld for profile problems.** A prescription can be published without
  its patient profile. If ELMIS requires the profile to arrive first, watch the
  `sending prescription … without it` warnings — they mark prescriptions the consumer may reject.
- **Structurally unusable rows are not retried.** Rows with no `Oid`/`PrescriptionUuid`/`PatientUuid`
  are marked synced deliberately, to stop unroutable data from blocking the queue. Watch the
  `Skipping unusable ELMIS record …` warnings to spot upstream data problems.
- **Quarantine is in-memory.** It is not shared between replicas and is lost on restart, which is
  safe (rows stay unsynced and are retried) but means a restart re-attempts every poisoned record
  immediately.
- **Multiple replicas.** `WITH (READPAST)` lets replicas skip rows locked by another instance, but
  there is no reservation step — overlapping reads are possible, which the at-least-once contract
  already tolerates.
- **Batch boundaries can split a prescription.** The poll takes `TOP (batch-size)` rows, so a
  prescription with rows either side of the cut is sent twice, each time with a subset of its drugs.
  Pre-existing; consumers should merge on `prescriptionUuid`.
