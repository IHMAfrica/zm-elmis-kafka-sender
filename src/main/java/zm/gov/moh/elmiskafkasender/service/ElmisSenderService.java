package zm.gov.moh.elmiskafkasender.service;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
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
        log.info("Final stats - Prescriptions: {}, Profiles (from prescriptions): {}, Client Profiles: {}, Errors: {}",
                totalPrescriptionsSent.get(), totalProfilesSent.get(), totalClientProfilesSent.get(), totalErrors.get());

        running.set(false);

        if (prescriptionPollingDisposable != null && !prescriptionPollingDisposable.isDisposed()) {
            prescriptionPollingDisposable.dispose();
        }
        if (clientPollingDisposable != null && !clientPollingDisposable.isDisposed()) {
            clientPollingDisposable.dispose();
        }

        kafkaProducerService.close();
    }

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

                    Map<UUID, List<ElmisLogRecord>> prescriptionGroups = records.stream()
                            .collect(Collectors.groupingBy(ElmisLogRecord::getPrescriptionUuid));

                    return processPrescriptionGroups(prescriptionGroups)
                            .map(List::size);
                })
                .onErrorResume(e -> {
                    log.error("Error processing prescription records", e);
                    totalErrors.incrementAndGet();
                    return Mono.just(0);
                });
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

        if (records.isEmpty()) {
            return Mono.empty();
        }

        ElmisLogRecord first = records.getFirst();
        UUID patientUuid = first.getPatientUuid();
        UUID prescriptionUuid = first.getPrescriptionUuid();
        List<UUID> recordOids = records.stream().map(ElmisLogRecord::getOid).toList();

        // Send patient profile first if not already sent
        Mono<Boolean> profileMono;
        if (sentPatientProfiles.contains(patientUuid)) {
            profileMono = Mono.just(true);
        } else {
            String profilePayload = payloadBuilderService.buildPatientProfilePayload(first);
            profileMono = kafkaProducerService.sendPatientProfile(profilePayload, "profile-" + patientUuid)
                    .doOnSuccess(success -> {
                        if (Boolean.TRUE.equals(success)) {
                            sentPatientProfiles.add(patientUuid);
                            log.debug("Patient profile sent for {}", patientUuid);
                        }
                    });
        }

        return profileMono.flatMap(profileSuccess -> {
            if (!profileSuccess) {
                log.warn("Skipping prescription {} due to profile failure", prescriptionUuid);
                return Mono.empty();
            }

            String prescriptionPayload = payloadBuilderService.buildPrescriptionPayload(records);
            return kafkaProducerService.sendPrescription(prescriptionPayload, "prescription-" + prescriptionUuid)
                    .doOnSuccess(success -> {
                        if (Boolean.TRUE.equals(success)) {
                            successfulOids.addAll(recordOids);
                            log.debug("Prescription sent: {}", prescriptionUuid);
                        } else {
                            totalErrors.incrementAndGet();
                        }
                    });
        }).then();
    }

    private void startClientPolling() {
        clientPollingDisposable = Flux.defer(this::processPendingClients)
                .repeatWhen(completed -> completed.flatMap(v -> {
                    if (!running.get()) {
                        return Mono.empty();
                    }
                    // Use same adaptive delay as prescriptions
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
                            log.error("Error in client polling loop", error);
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

                    return processClients(clients)
                            .map(List::size);
                })
                .onErrorResume(e -> {
                    log.error("Error processing client records", e);
                    totalErrors.incrementAndGet();
                    return Mono.just(0);
                });
    }

    private Mono<List<UUID>> processClients(List<ClientRecord> clients) {
        List<UUID> successfulClientOids = Collections.synchronizedList(new ArrayList<>());

        return Flux.fromIterable(clients)
                .flatMap(client -> processClientProfile(client, successfulClientOids), 5) // Concurrency of 5
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
        if (client.getHmisCode() == null || client.getHmisCode().isEmpty()) {
            log.warn("Client {} has no HMIS code, skipping", client.getOid());
            return Mono.empty();
        }

        String profilePayload = payloadBuilderService.buildPatientProfilePayload(client);

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
                .then();
    }
}