package zm.gov.moh.elmiskafkasender.service;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import zm.gov.moh.elmiskafkasender.entity.ClientRecord;
import zm.gov.moh.elmiskafkasender.entity.ElmisLogRecord;
import zm.gov.moh.elmiskafkasender.repository.ClientRepository;
import zm.gov.moh.elmiskafkasender.repository.ElmisLogRepository;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

@Service
@Slf4j
public class ElmisSenderService {

    /**
     * Upper bound on how many UUIDs are pushed into the NOT IN clause of a poll query. Anything
     * beyond this stays visible to the query and simply gets retried - degraded, but never fatal.
     */
    private static final int MAX_EXCLUSIONS = 500;

    /** Hard cap on quarantine bookkeeping so a pathological run cannot grow the map without end. */
    private static final int MAX_QUARANTINE_ENTRIES = 5000;

    private final ElmisLogRepository elmisLogRepository;
    private final ClientRepository clientRepository;
    private final KafkaProducerService kafkaProducerService;
    private final PayloadBuilderService payloadBuilderService;

    @Value("${elmis.polling.interval-ms}")
    private long pollingIntervalMs;

    @Value("${elmis.polling.idle-interval-ms}")
    private long idleIntervalMs;

    @Value("${elmis.polling.batch-size}")
    private int batchSize;

    @Value("${elmis.polling.quarantine-base-backoff-ms:30000}")
    private long quarantineBaseBackoffMs;

    @Value("${elmis.polling.quarantine-max-backoff-ms:1800000}")
    private long quarantineMaxBackoffMs;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicInteger prescriptionEmptyPolls = new AtomicInteger(0);
    private final AtomicInteger clientEmptyPolls = new AtomicInteger(0);

    // Metrics
    private final AtomicLong totalPrescriptionsSent = new AtomicLong(0);
    private final AtomicLong totalProfilesSent = new AtomicLong(0);
    private final AtomicLong totalClientProfilesSent = new AtomicLong(0);
    private final AtomicLong totalErrors = new AtomicLong(0);
    private final AtomicLong totalSkippedInvalidRecords = new AtomicLong(0);
    private final AtomicLong totalSkippedIncompleteProfiles = new AtomicLong(0);
    private final AtomicLong totalPrescriptionsSentWithoutProfile = new AtomicLong(0);

    /**
     * Prescriptions and clients that failed to send, held out of the poll query until their
     * backoff expires. Entries are deliberately never marked as synced - quarantine defers work,
     * it does not discard it, so nothing is lost and a restart re-tries everything.
     */
    private final Map<UUID, Quarantine> quarantinedPrescriptions = new ConcurrentHashMap<>();
    private final Map<UUID, Quarantine> quarantinedClients = new ConcurrentHashMap<>();

    private Set<Integer> prepPepRegimenIds = Collections.emptySet();

    private Disposable prescriptionPollingDisposable;
    private Disposable clientPollingDisposable;

    public ElmisSenderService(
            ElmisLogRepository elmisLogRepository,
            ClientRepository clientRepository,
            KafkaProducerService kafkaProducerService,
            PayloadBuilderService payloadBuilderService) {
        this.elmisLogRepository = elmisLogRepository;
        this.clientRepository = clientRepository;
        this.kafkaProducerService = kafkaProducerService;
        this.payloadBuilderService = payloadBuilderService;
    }

    @PostConstruct
    public void start() {
        log.info("Starting ELMIS Kafka Sender Service");

        try {
            prepPepRegimenIds = elmisLogRepository.findPREPAndPEPRegimenIds()
                    .collect(Collectors.toSet())
                    .blockOptional()
                    .orElse(Collections.emptySet());
        } catch (Exception e) {
            prepPepRegimenIds = Collections.emptySet();
        }

        running.set(true);
        startPrescriptionPolling();
        startClientPolling();
    }

    @PreDestroy
    public void stop() {
        log.info("Stopping ELMIS Kafka Sender Service");
        log.info("Final stats - Prescriptions: {}, Profiles (from prescriptions): {}, Client Profiles: {}, " +
                        "Errors: {}, Skipped Invalid: {}, Skipped Incomplete Profiles: {}, " +
                        "Prescriptions sent without profile: {}, Quarantined (prescriptions/clients): {}/{}",
                totalPrescriptionsSent.get(), totalProfilesSent.get(), totalClientProfilesSent.get(),
                totalErrors.get(), totalSkippedInvalidRecords.get(), totalSkippedIncompleteProfiles.get(),
                totalPrescriptionsSentWithoutProfile.get(),
                quarantinedPrescriptions.size(), quarantinedClients.size());

        running.set(false);

        if (prescriptionPollingDisposable != null && !prescriptionPollingDisposable.isDisposed()) {
            prescriptionPollingDisposable.dispose();
        }
        if (clientPollingDisposable != null && !clientPollingDisposable.isDisposed()) {
            clientPollingDisposable.dispose();
        }

        kafkaProducerService.close();
    }

    // ==================== Prescription Polling ====================

    private void startPrescriptionPolling() {
        prescriptionPollingDisposable = Flux.defer(this::processPendingPrescriptions)
                .repeatWhen(completed -> completed.flatMap(v -> {
                    if (!running.get()) {
                        return Mono.empty();
                    }
                    return Mono.delay(Duration.ofMillis(nextDelay(prescriptionEmptyPolls)));
                }))
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe(
                        count -> {
                            if (count > 0) {
                                log.debug("Processed {} prescription records", count);
                            }
                        },
                        error -> {
                            log.error("Error in prescription polling loop", error);
                            totalErrors.incrementAndGet();
                            if (running.get()) {
                                Mono.delay(Duration.ofSeconds(5)).subscribe(v -> startPrescriptionPolling());
                            }
                        },
                        () -> log.info("Prescription polling stopped")
                );
    }

    private Mono<Integer> processPendingPrescriptions() {
        List<UUID> excluded = activeQuarantine(quarantinedPrescriptions, "prescription");

        return elmisLogRepository.findUnprocessedRecords(batchSize, excluded)
                .collectList()
                .flatMap(records -> {
                    if (records.isEmpty()) {
                        prescriptionEmptyPolls.incrementAndGet();
                        return Mono.just(0);
                    }

                    prescriptionEmptyPolls.set(0);
                    log.debug("Processing {} ELMIS prescription records", records.size());

                    List<ElmisLogRecord> validRecords = new ArrayList<>();
                    List<UUID> invalidRecordOids = new ArrayList<>();

                    for (ElmisLogRecord record : records) {
                        ValidationResult validation = validateElmisLogRecord(record);
                        if (validation.isValid()) {
                            validRecords.add(record);
                        } else {
                            log.warn("Skipping unusable ELMIS record Oid: {} - Reason: {}",
                                    record.getOid(), validation.getReason());
                            if (record.getOid() != null) {
                                invalidRecordOids.add(record.getOid());
                            }
                            totalSkippedInvalidRecords.incrementAndGet();
                        }
                    }

                    Mono<Void> markInvalidMono = Mono.empty();
                    if (!invalidRecordOids.isEmpty()) {
                        markInvalidMono = elmisLogRepository.markRecordsAsSynced(invalidRecordOids)
                                .doOnSuccess(count -> log.info("Marked {} unusable ELMIS records as synced to skip", count))
                                .then();
                    }

                    if (validRecords.isEmpty()) {
                        return markInvalidMono.thenReturn(invalidRecordOids.size());
                    }

                    // Group valid records by prescription UUID
                    Map<UUID, List<ElmisLogRecord>> prescriptionGroups = validRecords.stream()
                            .collect(Collectors.groupingBy(ElmisLogRecord::getPrescriptionUuid));

                    return markInvalidMono
                            .then(processPrescriptionGroups(prescriptionGroups))
                            .map(successfulOids -> successfulOids.size() + invalidRecordOids.size());
                })
                .onErrorResume(e -> {
                    log.error("Error processing prescription records: {}", e.getMessage(), e);
                    totalErrors.incrementAndGet();
                    return Mono.just(0);
                });
    }

    /**
     * Only rejects records that cannot be routed at all. A missing NUPN or registration date makes
     * the <em>patient profile</em> incomplete, not the prescription - those records stay in play so
     * the prescription is still delivered. See {@link #hasCompleteProfile}.
     */
    private ValidationResult validateElmisLogRecord(ElmisLogRecord record) {
        if (record == null) {
            return ValidationResult.invalid("Record is null");
        }
        if (record.getOid() == null) {
            return ValidationResult.invalid("Oid is null");
        }
        if (record.getPrescriptionUuid() == null) {
            return ValidationResult.invalid("PrescriptionUuid is null");
        }
        if (record.getPatientUuid() == null) {
            return ValidationResult.invalid("PatientUuid is null");
        }

        return ValidationResult.valid();
    }

    /** A profile is only sendable when ELMIS can key it - NUPN plus a registration date. */
    private boolean hasCompleteProfile(ElmisLogRecord record) {
        return record.getPatientId() != null
                && !record.getPatientId().trim().isEmpty()
                && record.getRegistrationDateTime() != null;
    }

    private Mono<List<UUID>> processPrescriptionGroups(Map<UUID, List<ElmisLogRecord>> prescriptionGroups) {
        Set<UUID> sentPatientProfiles = ConcurrentHashMap.newKeySet();
        List<UUID> successfulOids = Collections.synchronizedList(new ArrayList<>());

        return Flux.fromIterable(prescriptionGroups.entrySet())
                .concatMap(entry -> processPrescriptionGroup(entry.getValue(), sentPatientProfiles, successfulOids))
                .then(Mono.defer(() -> {
                    if (!successfulOids.isEmpty()) {
                        return elmisLogRepository.markRecordsAsSynced(successfulOids)
                                .doOnSuccess(count -> log.info("Marked {} prescription records as synced", count))
                                .thenReturn(successfulOids);
                    }
                    return Mono.just(successfulOids);
                }));
    }

    /**
     * Sends one prescription. The patient profile is attempted first but is strictly best-effort:
     * a missing or failed profile is logged and the prescription goes out regardless.
     */
    private Mono<Void> processPrescriptionGroup(
            List<ElmisLogRecord> records,
            Set<UUID> sentPatientProfiles,
            List<UUID> successfulOids) {

        if (records == null || records.isEmpty()) {
            return Mono.empty();
        }

        ElmisLogRecord first = records.getFirst();
        UUID patientUuid = first.getPatientUuid();
        UUID prescriptionUuid = first.getPrescriptionUuid();

        if (patientUuid == null || prescriptionUuid == null) {
            log.warn("Skipping prescription group with null patientUuid or prescriptionUuid");
            return Mono.empty();
        }

        List<UUID> recordOids = records.stream()
                .map(ElmisLogRecord::getOid)
                .filter(Objects::nonNull)
                .toList();

        if (recordOids.isEmpty()) {
            log.warn("No valid record OIDs found for prescription {}", prescriptionUuid);
            return Mono.empty();
        }

        String prescriptionPayload = payloadBuilderService.buildPrescriptionPayload(records, prepPepRegimenIds);
        if (prescriptionPayload == null || prescriptionPayload.isEmpty()) {
            log.error("Failed to build prescription payload for {}", prescriptionUuid);
            totalErrors.incrementAndGet();
            quarantine(quarantinedPrescriptions, prescriptionUuid, "prescription", "payload could not be built");
            return Mono.empty();
        }

        return sendProfileBestEffort(first, patientUuid, prescriptionUuid, sentPatientProfiles)
                .then(kafkaProducerService.sendPrescription(prescriptionPayload, "prescription-" + prescriptionUuid))
                .defaultIfEmpty(false)
                .onErrorResume(e -> {
                    log.error("Error sending prescription {}: {}", prescriptionUuid, e.getMessage());
                    return Mono.just(false);
                })
                .doOnNext(success -> {
                    if (Boolean.TRUE.equals(success)) {
                        successfulOids.addAll(recordOids);
                        totalPrescriptionsSent.incrementAndGet();
                        quarantinedPrescriptions.remove(prescriptionUuid);
                        log.debug("Prescription sent: {}", prescriptionUuid);
                    } else {
                        totalErrors.incrementAndGet();
                        quarantine(quarantinedPrescriptions, prescriptionUuid, "prescription", "kafka send failed");
                    }
                })
                .then();
    }

    /**
     * Publishes the patient profile if it is complete and not already sent in this batch. Never
     * errors and never short-circuits the caller - the returned Mono always completes.
     */
    private Mono<Void> sendProfileBestEffort(
            ElmisLogRecord record, UUID patientUuid, UUID prescriptionUuid, Set<UUID> sentPatientProfiles) {

        if (sentPatientProfiles.contains(patientUuid)) {
            return Mono.empty();
        }

        if (!hasCompleteProfile(record)) {
            log.warn("Patient profile for {} is incomplete (NUPN/registration date missing); " +
                    "sending prescription {} without it", patientUuid, prescriptionUuid);
            totalSkippedIncompleteProfiles.incrementAndGet();
            totalPrescriptionsSentWithoutProfile.incrementAndGet();
            return Mono.empty();
        }

        String profilePayload = payloadBuilderService.buildPatientProfilePayload(record);
        if (profilePayload == null || profilePayload.isEmpty()) {
            log.warn("Failed to build patient profile payload for patient {}; " +
                    "sending prescription {} without it", patientUuid, prescriptionUuid);
            totalPrescriptionsSentWithoutProfile.incrementAndGet();
            return Mono.empty();
        }

        return kafkaProducerService.sendPatientProfile(profilePayload, "profile-" + patientUuid)
                .defaultIfEmpty(false)
                .onErrorResume(e -> {
                    log.error("Error sending patient profile for {}: {}", patientUuid, e.getMessage());
                    return Mono.just(false);
                })
                .doOnNext(success -> {
                    if (Boolean.TRUE.equals(success)) {
                        sentPatientProfiles.add(patientUuid);
                        totalProfilesSent.incrementAndGet();
                        log.debug("Patient profile sent for {}", patientUuid);
                    } else {
                        log.warn("Patient profile send failed for {}; sending prescription {} anyway",
                                patientUuid, prescriptionUuid);
                        totalErrors.incrementAndGet();
                        totalPrescriptionsSentWithoutProfile.incrementAndGet();
                    }
                })
                .then();
    }

    // ==================== Client Profile Polling ====================

    private void startClientPolling() {
        clientPollingDisposable = Flux.defer(this::processPendingClients)
                .repeatWhen(completed -> completed.flatMap(v -> {
                    if (!running.get()) {
                        return Mono.empty();
                    }
                    return Mono.delay(Duration.ofMillis(nextDelay(clientEmptyPolls)));
                }))
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe(
                        count -> {
                            if (count > 0) {
                                log.debug("Processed {} client records", count);
                            }
                        },
                        error -> {
                            log.error("Error in client polling loop: {}", error.getMessage(), error);
                            totalErrors.incrementAndGet();
                            if (running.get()) {
                                Mono.delay(Duration.ofSeconds(5)).subscribe(v -> startClientPolling());
                            }
                        },
                        () -> log.info("Client polling stopped")
                );
    }

    private Mono<Integer> processPendingClients() {
        List<UUID> excluded = activeQuarantine(quarantinedClients, "client");

        return clientRepository.findUnprocessedClients(batchSize, excluded)
                .collectList()
                .flatMap(clients -> {
                    if (clients.isEmpty()) {
                        clientEmptyPolls.incrementAndGet();
                        return Mono.just(0);
                    }

                    clientEmptyPolls.set(0);
                    log.debug("Processing {} client profile records", clients.size());

                    List<ClientRecord> validClients = new ArrayList<>();
                    List<UUID> invalidClientOids = new ArrayList<>();

                    for (ClientRecord client : clients) {
                        ValidationResult validation = validateClientRecord(client);
                        if (validation.isValid()) {
                            validClients.add(client);
                        } else {
                            log.warn("Skipping invalid client Oid: {} - Reason: {}",
                                    client.getOid(), validation.getReason());
                            if (client.getOid() != null) {
                                invalidClientOids.add(client.getOid());
                            }
                            if (validation.isIncompleteProfile()) {
                                totalSkippedIncompleteProfiles.incrementAndGet();
                            } else {
                                totalSkippedInvalidRecords.incrementAndGet();
                            }
                        }
                    }

                    // Mark invalid clients as synced
                    Mono<Void> markInvalidMono = Mono.empty();
                    if (!invalidClientOids.isEmpty()) {
                        markInvalidMono = clientRepository.markClientsAsSynced(invalidClientOids)
                                .doOnSuccess(count -> log.info("Marked {} invalid clients as synced to skip", count))
                                .then();
                    }

                    if (validClients.isEmpty()) {
                        return markInvalidMono.thenReturn(invalidClientOids.size());
                    }

                    return markInvalidMono
                            .then(processClients(validClients))
                            .map(successfulOids -> successfulOids.size() + invalidClientOids.size());
                })
                .onErrorResume(e -> {
                    log.error("Error processing client records: {}", e.getMessage(), e);
                    totalErrors.incrementAndGet();
                    return Mono.just(0);
                });
    }

    private ValidationResult validateClientRecord(ClientRecord client) {
        if (client == null) {
            return ValidationResult.invalid("Client is null");
        }
        if (client.getOid() == null) {
            return ValidationResult.invalid("Oid is null");
        }
        if (client.getHmisCode() == null || client.getHmisCode().trim().isEmpty()) {
            return ValidationResult.invalid("HMISCode is null or empty");
        }

        if (client.getNupn() == null || client.getNupn().trim().isEmpty()) {
            return ValidationResult.incompleteProfile("NUPN (PatientId) is null or empty");
        }
        if (client.getRegistrationDate() == null) {
            return ValidationResult.incompleteProfile("RegistrationDate is null");
        }

        return ValidationResult.valid();
    }

    private Mono<List<UUID>> processClients(List<ClientRecord> clients) {
        List<UUID> successfulClientOids = Collections.synchronizedList(new ArrayList<>());

        return Flux.fromIterable(clients)
                .flatMap(client -> processClientProfile(client, successfulClientOids), 5)
                .then(Mono.defer(() -> {
                    if (!successfulClientOids.isEmpty()) {
                        return clientRepository.markClientsAsSynced(successfulClientOids)
                                .doOnSuccess(count -> {
                                    log.info("Marked {} client profiles as synced", count);
                                    totalClientProfilesSent.addAndGet(successfulClientOids.size());
                                })
                                .thenReturn(successfulClientOids);
                    }
                    return Mono.just(successfulClientOids);
                }));
    }

    private Mono<Void> processClientProfile(ClientRecord client, List<UUID> successfulClientOids) {
        String profilePayload = payloadBuilderService.buildPatientProfilePayload(client);

        if (profilePayload == null || profilePayload.isEmpty()) {
            log.error("Failed to build patient profile payload for client {}", client.getOid());
            totalErrors.incrementAndGet();
            quarantine(quarantinedClients, client.getOid(), "client", "payload could not be built");
            return Mono.empty();
        }

        return kafkaProducerService.sendPatientProfile(profilePayload, "client-profile-" + client.getOid())
                .defaultIfEmpty(false)
                .onErrorResume(e -> {
                    log.error("Error sending client profile for {}: {}", client.getOid(), e.getMessage());
                    return Mono.just(false);
                })
                .doOnNext(success -> {
                    if (Boolean.TRUE.equals(success)) {
                        successfulClientOids.add(client.getOid());
                        quarantinedClients.remove(client.getOid());
                        log.debug("Client profile sent for {}", client.getOid());
                    } else {
                        totalErrors.incrementAndGet();
                        quarantine(quarantinedClients, client.getOid(), "client", "kafka send failed");
                    }
                })
                .then();
    }

    // ==================== Quarantine ====================

    private long nextDelay(AtomicInteger emptyPolls) {
        return emptyPolls.get() > 5 ? idleIntervalMs : pollingIntervalMs;
    }

    /**
     * Parks a failing item behind an exponential backoff so it stops occupying the batch. The row
     * is left unsynced, so it is retried once the backoff expires and again after any restart.
     */
    private void quarantine(Map<UUID, Quarantine> registry, UUID id, String kind, String reason) {
        if (id == null) {
            return;
        }

        Quarantine entry = registry.compute(id, (key, existing) -> {
            int attempts = existing == null ? 1 : existing.attempts() + 1;
            long backoff = Math.min(
                    quarantineBaseBackoffMs << Math.min(attempts - 1, 16),
                    quarantineMaxBackoffMs);
            return new Quarantine(attempts, System.currentTimeMillis() + backoff);
        });

        log.warn("Quarantined {} {} after {} failed attempt(s) ({}); retrying in {}s",
                kind, id, entry.attempts(), reason,
                Math.max(0, entry.retryAtMs() - System.currentTimeMillis()) / 1000);

        pruneQuarantine(registry);
    }

    /** Returns the ids still inside their backoff window, i.e. those to hide from the next poll. */
    private List<UUID> activeQuarantine(Map<UUID, Quarantine> registry, String kind) {
        if (registry.isEmpty()) {
            return List.of();
        }

        long now = System.currentTimeMillis();
        List<Map.Entry<UUID, Quarantine>> notDue = registry.entrySet().stream()
                .filter(e -> e.getValue().retryAtMs() > now)
                .sorted(Comparator.comparingLong((Map.Entry<UUID, Quarantine> e) -> e.getValue().retryAtMs())
                        .reversed())
                .toList();

        if (notDue.size() > MAX_EXCLUSIONS) {
            log.warn("{} {} items quarantined, exceeding the {} exclusion limit - the remainder will be " +
                            "re-read and retried on every poll until the backlog clears",
                    notDue.size(), kind, MAX_EXCLUSIONS);
            notDue = notDue.subList(0, MAX_EXCLUSIONS);
        }

        return notDue.stream().map(Map.Entry::getKey).toList();
    }

    /** Drops the entries closest to being due once the registry grows past its cap. */
    private void pruneQuarantine(Map<UUID, Quarantine> registry) {
        if (registry.size() <= MAX_QUARANTINE_ENTRIES) {
            return;
        }

        registry.entrySet().stream()
                .sorted(Comparator.comparingLong(e -> e.getValue().retryAtMs()))
                .limit(registry.size() - MAX_QUARANTINE_ENTRIES)
                .map(Map.Entry::getKey)
                .toList()
                .forEach(registry::remove);
    }

    private record Quarantine(int attempts, long retryAtMs) {
    }

    @Getter
    private static class ValidationResult {
        private final boolean valid;
        private final String reason;
        private final boolean incompleteProfile;

        private ValidationResult(boolean valid, String reason, boolean incompleteProfile) {
            this.valid = valid;
            this.reason = reason;
            this.incompleteProfile = incompleteProfile;
        }

        public static ValidationResult valid() {
            return new ValidationResult(true, null, false);
        }

        public static ValidationResult invalid(String reason) {
            return new ValidationResult(false, reason, false);
        }

        public static ValidationResult incompleteProfile(String reason) {
            return new ValidationResult(false, reason, true);
        }

    }
}
