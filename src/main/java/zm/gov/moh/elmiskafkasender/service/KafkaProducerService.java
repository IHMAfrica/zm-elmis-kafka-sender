package zm.gov.moh.elmiskafkasender.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;
import reactor.kafka.sender.SenderResult;

import java.time.Duration;

@Service
@Slf4j
public class KafkaProducerService {
    private final KafkaSender<String, String> kafkaSender;

    @Value("${elmis.topics.prescriptions}")
    private String prescriptionsTopic;

    @Value("${elmis.topics.patient-profiles}")
    private String patientProfilesTopic;

    public KafkaProducerService(KafkaSender<String, String> kafkaSender) {
        this.kafkaSender = kafkaSender;
    }

    public Mono<Boolean> sendPatientProfile(String payload, String correlationId) {
        return sendMessage(patientProfilesTopic, payload, correlationId)
                .map(result -> true)
                .onErrorResume(e -> {
                    log.error("Failed to send patient profile [{}]: {}", correlationId, e.getMessage());
                    return Mono.just(false);
                });
    }

    public Mono<Boolean> sendPrescription(String payload, String correlationId) {
        return sendMessage(prescriptionsTopic, payload, correlationId)
                .map(result -> true)
                .onErrorResume(e -> {
                    log.error("Failed to send prescription [{}]: {}", correlationId, e.getMessage());
                    return Mono.just(false);
                });
    }

    private Mono<SenderResult<String>> sendMessage(String topic, String payload, String correlationId) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, null, payload);

        SenderRecord<String, String, String> senderRecord = SenderRecord.create(record, correlationId);

        return kafkaSender.send(Mono.just(senderRecord))
                .next()
                .doOnSuccess(result -> {
                    assert result != null;
                    if (result.exception() != null) {
                        log.error("Kafka send failed for [{}]: {}", correlationId, result.exception().getMessage());
                    } else {
                        log.debug("Message sent to {} partition {} offset {} [{}]",
                                result.recordMetadata().topic(),
                                result.recordMetadata().partition(),
                                result.recordMetadata().offset(),
                                correlationId);
                    }
                })
                .doOnError(e -> log.error("Error sending message [{}]: {}", correlationId, e.getMessage()));
    }

    public void close() {
        kafkaSender.close();
    }
}
