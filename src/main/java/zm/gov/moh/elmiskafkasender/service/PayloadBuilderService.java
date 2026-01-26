package zm.gov.moh.elmiskafkasender.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import zm.gov.moh.elmiskafkasender.dto.*;
import zm.gov.moh.elmiskafkasender.entity.ClientRecord;
import zm.gov.moh.elmiskafkasender.entity.ElmisLogRecord;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Service
@Slf4j
@RequiredArgsConstructor
public class PayloadBuilderService {
    private static final String DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
    private static final String DATE_FORMAT = "yyyy-MM-dd";
    private static final DateTimeFormatter DATETIME_FORMATTER = DateTimeFormatter.ofPattern(DATETIME_FORMAT);
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern(DATE_FORMAT);

    private final ObjectMapper objectMapper;

    public String buildPatientProfilePayload(ClientRecord client) {
        PatientProfileDto profile = PatientProfileDto.builder()
                .msh(buildMsh(client.getHmisCode(), "profile"))
                .registrationDateTime(formatDateTime(client.getRegistrationDate()))
                .dateOfBirth(formatDate(client.getDob()))
                .patientUuid(client.getOid())
                .nrcNumber(nullToEmpty(client.getNrc()))
                .firstName(nullToEmpty(client.getFirstName()))
                .lastName(nullToEmpty(client.getSurname()))
                .patientId(nullToEmpty(client.getNupn()))
                .sex(mapSex(client.getSex()))
                .build();

        return toJson(profile);
    }

    public String buildPatientProfilePayload(ElmisLogRecord record) {
        PatientProfileDto profile = PatientProfileDto.builder()
                .msh(buildMsh(record.getHmisCode(), "profile"))
                .registrationDateTime(formatDateTime(record.getRegistrationDateTime()))
                .dateOfBirth(formatDate(record.getDateOfBirth()))
                .patientUuid(record.getPatientUuid())
                .nrcNumber(nullToEmpty(record.getNrcNumber()))
                .firstName(nullToEmpty(record.getFirstName()))
                .lastName(nullToEmpty(record.getLastName()))
                .patientId(nullToEmpty(record.getPatientId()))
                .sex(nullToEmpty(record.getSex()).equals("1") ? "M" : "F")
                .build();

        return toJson(profile);
    }

    public String buildPrescriptionPayload(List<ElmisLogRecord> prescriptionRecords) {
        if (prescriptionRecords == null || prescriptionRecords.isEmpty()) {
            return null;
        }

        ElmisLogRecord first = prescriptionRecords.getFirst();

        PrescriptionDto prescription = PrescriptionDto.builder()
                .msh(buildMsh(first.getHmisCode(), "prescription"))
                .patientUuid(first.getPatientUuid())
                .artNumber(nullToEmpty(first.getArtNumber()))
                .cd4(nullToEmpty(first.getCd4Count()))
                .viralLoad(nullToEmpty(first.getViralLoad()))
                .dateOfBled(formatDateTime(first.getDateOfBled()))
                .regimenId(first.getRegimenId())
                .clinicianId(first.getClinicianId())
                .prescriptionUuid(first.getPrescriptionUuid())
                .regimen(buildRegimen(prescriptionRecords))
                .vitals(buildVitals(first))
                .prescription(buildPrescriptionDrugs(prescriptionRecords))
                .build();

        return toJson(prescription);
    }

    private MshDto buildMsh(String hmisCode, String messageType) {
        return MshDto.builder()
                .timestamp(LocalDateTime.now().plusHours(2).format(DATETIME_FORMATTER))
                .sendingApplication("CarePro")
                .receivingApplication("elmis")
                .messageId(UUID.randomUUID().toString())
                .hmisCode(hmisCode)
                .mflCode(hmisCode)
                .messageType(messageType)
                .build();
    }

    private RegimenDto buildRegimen(List<ElmisLogRecord> records) {
        ElmisLogRecord specialDrug = records.stream()
                .filter(r -> r.getSpecialDrug() != null && r.getSpecialDrug() == 1)
                .findFirst()
                .orElse(null);

        if (specialDrug == null) {
            return RegimenDto.builder()
                    .medicationId("")
                    .regimenCode("")
                    .quantityPerDose(BigDecimal.ZERO)
                    .dosageUnit("")
                    .frequency("")
                    .duration(0)
                    .build();
        }

        return RegimenDto.builder()
                .medicationId(uuidToString(specialDrug.getMedicationId()))
                .regimenCode(nullToEmpty(specialDrug.getDrugIdentifier()))
                .quantityPerDose(specialDrug.getQuantityPerDose())
                .dosageUnit(nullToEmpty(specialDrug.getDosageUnit()))
                .frequency(nullToEmpty(specialDrug.getFrequency()))
                .duration(specialDrug.getDuration())
                .build();
    }

    private VitalsDto buildVitals(ElmisLogRecord record) {
        boolean hasVitals = (record.getHeight() != null && record.getHeight().compareTo(BigDecimal.ZERO) > 0)
                || (record.getWeight() != null && record.getWeight().compareTo(BigDecimal.ZERO) > 0)
                || (record.getBloodPressure() != null && !record.getBloodPressure().isEmpty());

        if (!hasVitals) {
            return VitalsDto.builder()
                    .height("")
                    .heightDateTimeCollected("")
                    .weight("")
                    .weightDateTimeCollected("")
                    .bloodPressure("")
                    .bloodPressureDateTimeCollected("")
                    .build();
        }

        return VitalsDto.builder()
                .height(formatDecimal(record.getHeight()))
                .heightDateTimeCollected(formatDateTime(record.getHeightDateTimeCollected()))
                .weight(formatDecimal(record.getWeight()))
                .weightDateTimeCollected(formatDateTime(record.getWeightDateTimeCollected()))
                .bloodPressure(nullToEmpty(record.getBloodPressure()))
                .bloodPressureDateTimeCollected(formatDateTime(record.getBloodPressureDateTimeCollected()))
                .build();
    }

    private PrescriptionDrugsDto buildPrescriptionDrugs(List<ElmisLogRecord> records) {
        List<PrescriptionDrugDto> drugs = new ArrayList<>();
        String prescriptionDate = "";

        for (ElmisLogRecord record : records) {
            if (record.getSpecialDrug() == null || record.getSpecialDrug() != 1) {
                drugs.add(PrescriptionDrugDto.builder()
                        .medicationId(uuidToString(record.getMedicationId()))
                        .drugCode(nullToEmpty(record.getDrugIdentifier()))
                        .quantityPerDose(record.getQuantityPerDose())
                        .dosageUnit(nullToEmpty(record.getDosageUnit()))
                        .frequency(nullToEmpty(record.getFrequency()))
                        .duration(record.getDuration())
                        .build());

                prescriptionDate = formatDate(record.getPrescriptionDate());
            }
        }

        return PrescriptionDrugsDto.builder()
                .prescriptionDrugs(drugs)
                .date(prescriptionDate)
                .build();
    }

    private String mapSex(Short sex) {
        if (sex == null) return "";
        // Assuming 0 = Male, 1 = Female based on common conventions
        return sex == 0 ? "M" : "F";
    }

    private String formatDateTime(LocalDateTime dateTime) {
        if (dateTime == null || dateTime.equals(LocalDateTime.MIN)) {
            return "";
        }
        return dateTime.format(DATETIME_FORMATTER);
    }

    private String formatDate(LocalDateTime dateTime) {
        if (dateTime == null || dateTime.equals(LocalDateTime.MIN)) {
            return "";
        }
        return dateTime.format(DATE_FORMATTER);
    }

    private String formatDecimal(BigDecimal value) {
        if (value == null || value.compareTo(BigDecimal.ZERO) == 0) {
            return "";
        }
        return value.setScale(2, java.math.RoundingMode.HALF_UP).toString();
    }

    private String nullToEmpty(String value) {
        return value == null ? "" : value;
    }

    private String uuidToString(UUID uuid) {
        return uuid == null ? "" : uuid.toString();
    }

    private String toJson(Object object) {
        try {
            return objectMapper.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize object to JSON", e);
            return null;
        }
    }
}