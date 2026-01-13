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
import zm.gov.moh.elmiskafkasender.entity.ElmisLogRecord;
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
    private final AtomicLong totalSent = new AtomicLong(0);
    private final AtomicLong totalErrors = new AtomicLong(0);

    private Disposable pollingDisposable;

    public ElmisSenderService(
            ElmisLogRepository elmisLogRepository,
            KafkaProducerService kafkaProducerService,
            PayloadBuilderService payloadBuilderService) {
        this.elmisLogRepository = elmisLogRepository;
        this.kafkaProducerService = kafkaProducerService;
        this.payloadBuilderService = payloadBuilderService;
    }

    @PostConstruct
    public void start() {
        log.info("Starting ELMIS Kafka Sender Service");
        running.set(true);
        startPolling();
    }

    @PreDestroy
    public void stop() {
        log.info("Stopping ELMIS Kafka Sender Service. Total sent: {}, Total errors: {}",
                totalSent.get(), totalErrors.get());
        running.set(false);
        if (pollingDisposable != null && !pollingDisposable.isDisposed()) {
            pollingDisposable.dispose();
        }
        kafkaProducerService.close();
    }

    private void startPolling() {
        pollingDisposable = Flux.defer(this::processPendingRecords)
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
                                log.debug("Processed {} records", count);
                            }
                        },
                        error -> {
                            log.error("Error in polling loop", error);
                            totalErrors.incrementAndGet();
                            if (running.get()) {
                                Mono.delay(Duration.ofSeconds(5))
                                        .subscribe(v -> startPolling());
                            }
                        },
                        () -> log.info("Polling stopped")
                );
    }

    private Mono<Integer> processPendingRecords() {
        return elmisLogRepository.findUnprocessedRecords(batchSize)
                .collectList()
                .flatMap(records -> {
                    if (records.isEmpty()) {
                        consecutiveEmptyPolls.incrementAndGet();
                        return Mono.just(0);
                    }

                    consecutiveEmptyPolls.set(0);
                    log.debug("Processing {} ELMIS records", records.size());

                    Map<UUID, List<ElmisLogRecord>> prescriptionGroups = records.stream()
                            .collect(Collectors.groupingBy(ElmisLogRecord::getPrescriptionUuid));

                    return processGroups(prescriptionGroups)
                            .map(List::size);
                })
                .onErrorResume(e -> {
                    log.error("Error processing records", e);
                    totalErrors.incrementAndGet();
                    return Mono.just(0);
                });
    }

    private Mono<List<UUID>> processGroups(Map<UUID, List<ElmisLogRecord>> prescriptionGroups) {
        Set<UUID> sentPatientProfiles = ConcurrentHashMap.newKeySet();
        List<UUID> successfulOids = Collections.synchronizedList(new ArrayList<>());

        return Flux.fromIterable(prescriptionGroups.entrySet())
                .concatMap(entry -> processPrescriptionGroup(entry.getValue(), sentPatientProfiles, successfulOids))
                .then(Mono.defer(() -> {
                    if (!successfulOids.isEmpty()) {
                        return elmisLogRepository.markRecordsAsSynced(successfulOids)
                                .doOnSuccess(count -> {
                                    log.info("Marked {} records as synced", count);
                                    totalSent.addAndGet(successfulOids.size());
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
        List<UUID> recordOids = records.stream()
                .map(ElmisLogRecord::getOid)
                .toList();

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
}