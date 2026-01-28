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

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicInteger consecutiveEmptyPolls = new AtomicInteger(0);

    // Metrics
    private final AtomicLong totalPrescriptionsSent = new AtomicLong(0);
    private final AtomicLong totalProfilesSent = new AtomicLong(0);
    private final AtomicLong totalClientProfilesSent = new AtomicLong(0);
    private final AtomicLong totalErrors = new AtomicLong(0);
    private final AtomicLong totalSkippedInvalidRecords = new AtomicLong(0);
    private final AtomicLong totalSkippedIncompleteProfiles = new AtomicLong(0);

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
        running.set(true);
        startPrescriptionPolling();
        startClientPolling();
    }

    @PreDestroy
    public void stop() {
        log.info("Stopping ELMIS Kafka Sender Service");
        log.info("Final stats - Prescriptions: {}, Profiles (from prescriptions): {}, Client Profiles: {}, " +
                        "Errors: {}, Skipped Invalid: {}, Skipped Incomplete Profiles: {}",
                totalPrescriptionsSent.get(), totalProfilesSent.get(), totalClientProfilesSent.get(),
                totalErrors.get(), totalSkippedInvalidRecords.get(), totalSkippedIncompleteProfiles.get());

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
                    long delay = consecutiveEmptyPolls.get() > 5 ? idleIntervalMs : pollingIntervalMs;
                    return Mono.delay(Duration.ofMillis(delay));
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
        return elmisLogRepository.findUnprocessedRecords(batchSize)
                .collectList()
                .flatMap(records -> {
                    if (records.isEmpty()) {
                        consecutiveEmptyPolls.incrementAndGet();
                        return Mono.just(0);
                    }

                    consecutiveEmptyPolls.set(0);
                    log.debug("Processing {} ELMIS prescription records", records.size());

                    List<ElmisLogRecord> validRecords = new ArrayList<>();
                    List<UUID> invalidRecordOids = new ArrayList<>();

                    for (ElmisLogRecord record : records) {
                        ValidationResult validation = validateElmisLogRecord(record);
                        if (validation.isValid()) {
                            validRecords.add(record);
                        } else {
                            log.warn("Skipping invalid ELMIS record Oid: {} - Reason: {}",
                                    record.getOid(), validation.getReason());
                            if (record.getOid() != null) {
                                invalidRecordOids.add(record.getOid());
                            }
                            if (validation.isIncompleteProfile()) {
                                totalSkippedIncompleteProfiles.incrementAndGet();
                            } else {
                                totalSkippedInvalidRecords.incrementAndGet();
                            }
                        }
                    }

                    Mono<Void> markInvalidMono = Mono.empty();
                    if (!invalidRecordOids.isEmpty()) {
                        markInvalidMono = elmisLogRepository.markRecordsAsSynced(invalidRecordOids)
                                .doOnSuccess(count -> log.info("Marked {} invalid ELMIS records as synced to skip", count))
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

        // Validate patient profile fields
        if (record.getSex() == null || record.getSex().trim().isEmpty()) {
            return ValidationResult.incompleteProfile("Sex is null or empty");
        }
        if (record.getPatientId() == null || record.getPatientId().trim().isEmpty()) {
            return ValidationResult.incompleteProfile("PatientId (NUPN) is null or empty");
        }
        if (record.getRegistrationDateTime() == null) {
            return ValidationResult.incompleteProfile("RegistrationDateTime is null");
        }

        return ValidationResult.valid();
    }

    private Mono<List<UUID>> processPrescriptionGroups(Map<UUID, List<ElmisLogRecord>> prescriptionGroups) {
        Set<UUID> sentPatientProfiles = ConcurrentHashMap.newKeySet();
        List<UUID> successfulOids = Collections.synchronizedList(new ArrayList<>());

        return Flux.fromIterable(prescriptionGroups.entrySet())
                .concatMap(entry -> processPrescriptionGroup(entry.getValue(), sentPatientProfiles, successfulOids))
                .then(Mono.defer(() -> {
                    if (!successfulOids.isEmpty()) {
                        return elmisLogRepository.markRecordsAsSynced(successfulOids)
                                .doOnSuccess(count -> {
                                    log.info("Marked {} prescription records as synced", count);
                                    totalPrescriptionsSent.addAndGet(prescriptionGroups.size());
                                    totalProfilesSent.addAndGet(sentPatientProfiles.size());
                                })
                                .thenReturn(successfulOids);
                    }
                    return Mono.just(successfulOids);
                }));
    }

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

        Mono<Boolean> profileMono;
        if (sentPatientProfiles.contains(patientUuid)) {
            profileMono = Mono.just(true);
        } else {
            String profilePayload = payloadBuilderService.buildPatientProfilePayload(first);
            if (profilePayload == null || profilePayload.isEmpty()) {
                log.warn("Failed to build patient profile payload for patient {}", patientUuid);
                profileMono = Mono.just(false);
            } else {
                profileMono = kafkaProducerService.sendPatientProfile(profilePayload, "profile-" + patientUuid)
                        .doOnSuccess(success -> {
                            if (Boolean.TRUE.equals(success)) {
                                sentPatientProfiles.add(patientUuid);
                                log.debug("Patient profile sent for {}", patientUuid);
                            }
                        })
                        .onErrorResume(e -> {
                            log.error("Error sending patient profile for {}: {}", patientUuid, e.getMessage());
                            return Mono.just(false);
                        });
            }
        }

        return profileMono.flatMap(profileSuccess -> {
            if (!profileSuccess) {
                log.warn("Skipping prescription {} due to profile failure", prescriptionUuid);
                totalErrors.incrementAndGet();
                return Mono.empty();
            }

            String prescriptionPayload = payloadBuilderService.buildPrescriptionPayload(records);
            if (prescriptionPayload == null || prescriptionPayload.isEmpty()) {
                log.warn("Failed to build prescription payload for {}", prescriptionUuid);
                totalErrors.incrementAndGet();
                return Mono.empty();
            }

            return kafkaProducerService.sendPrescription(prescriptionPayload, "prescription-" + prescriptionUuid)
                    .doOnSuccess(success -> {
                        if (Boolean.TRUE.equals(success)) {
                            successfulOids.addAll(recordOids);
                            log.debug("Prescription sent: {}", prescriptionUuid);
                        } else {
                            log.warn("Failed to send prescription {}", prescriptionUuid);
                            totalErrors.incrementAndGet();
                        }
                    })
                    .onErrorResume(e -> {
                        log.error("Error sending prescription {}: {}", prescriptionUuid, e.getMessage());
                        totalErrors.incrementAndGet();
                        return Mono.just(false);
                    });
        }).then();
    }

    // ==================== Client Profile Polling ====================

    private void startClientPolling() {
        clientPollingDisposable = Flux.defer(this::processPendingClients)
                .repeatWhen(completed -> completed.flatMap(v -> {
                    if (!running.get()) {
                        return Mono.empty();
                    }
                    long delay = consecutiveEmptyPolls.get() > 5 ? idleIntervalMs : pollingIntervalMs;
                    return Mono.delay(Duration.ofMillis(delay));
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
        return clientRepository.findUnprocessedClients(batchSize)
                .collectList()
                .flatMap(clients -> {
                    if (clients.isEmpty()) {
                        return Mono.just(0);
                    }

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

        if (client.getSex() == null) {
            return ValidationResult.incompleteProfile("Sex is null");
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
            log.warn("Failed to build patient profile payload for client {}", client.getOid());
            totalErrors.incrementAndGet();
            return Mono.empty();
        }

        return kafkaProducerService.sendPatientProfile(profilePayload, "client-profile-" + client.getOid())
                .doOnSuccess(success -> {
                    if (Boolean.TRUE.equals(success)) {
                        successfulClientOids.add(client.getOid());
                        log.debug("Client profile sent for {}", client.getOid());
                    } else {
                        log.warn("Failed to send client profile for {}", client.getOid());
                        totalErrors.incrementAndGet();
                    }
                })
                .onErrorResume(e -> {
                    log.error("Error sending client profile for {}: {}", client.getOid(), e.getMessage());
                    totalErrors.incrementAndGet();
                    return Mono.just(false);
                })
                .then();
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