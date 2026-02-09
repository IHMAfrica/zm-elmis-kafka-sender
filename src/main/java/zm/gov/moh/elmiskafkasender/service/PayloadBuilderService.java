package zm.gov.moh.elmiskafkasender.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import zm.gov.moh.elmiskafkasender.dto.MshDto;
import zm.gov.moh.elmiskafkasender.dto.PatientProfileDto;
import zm.gov.moh.elmiskafkasender.entity.ClientRecord;
import zm.gov.moh.elmiskafkasender.entity.ElmisLogRecord;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

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

    public String buildPrescriptionPayload(List<ElmisLogRecord> prescriptionRecords, Set<Integer> prepPepRegimenIds) {
        if (prescriptionRecords == null || prescriptionRecords.isEmpty()) {
            return null;
        }

        ElmisLogRecord first = prescriptionRecords.getFirst();

        boolean isPrepOrPep = prepPepRegimenIds != null
                && first.getRegimenId() != null
                && prepPepRegimenIds.contains(first.getRegimenId());
        String artNumber = isPrepOrPep ? generateRandomArtNumber() : nullToEmpty(first.getArtNumber());

        var prescription = new java.util.LinkedHashMap<String, Object>();
        prescription.put("msh", buildMshMap(first.getHmisCode(), "prescription"));
        prescription.put("patientUuid", first.getPatientUuid());
        prescription.put("artNumber", artNumber);
        prescription.put("cd4", nullToEmpty(first.getCd4Count()));
        prescription.put("viralLoad", nullToEmpty(first.getViralLoad()));
        prescription.put("dateOfBled", formatDateTime(first.getDateOfBled()));
        prescription.put("regimenId", first.getRegimenId());
        prescription.put("clinicianId", first.getClinicianId());
        prescription.put("prescriptionUuid", first.getPrescriptionUuid());
        prescription.put("regimen", buildRegimenMap(prescriptionRecords));
        prescription.put("vitals", buildVitalsMap(first));
        prescription.put("prescription", buildPrescriptionDrugsMap(prescriptionRecords));

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

    private java.util.Map<String, Object> buildMshMap(String hmisCode, String messageType) {
        var msh = new java.util.LinkedHashMap<String, Object>();
        msh.put("timestamp", LocalDateTime.now().plusHours(2).format(DATETIME_FORMATTER));
        msh.put("sendingApplication", "CarePro");
        msh.put("receivingApplication", "elmis");
        msh.put("messageId", UUID.randomUUID().toString());
        msh.put("hmisCode", hmisCode);
        msh.put("mflCode", hmisCode);
        msh.put("messageType", messageType);
        return msh;
    }

    private java.util.Map<String, Object> buildRegimenMap(List<ElmisLogRecord> records) {
        ElmisLogRecord specialDrug = records.stream()
                .filter(r -> r.getSpecialDrug() != null && r.getSpecialDrug() == 1)
                .findFirst()
                .orElse(null);

        var regimen = new java.util.LinkedHashMap<String, Object>();
        if (specialDrug == null) {
            regimen.put("medicationId", "");
            regimen.put("regimenCode", "");
            regimen.put("quantityPerDose", BigDecimal.ZERO);
            regimen.put("dosageUnit", "");
            regimen.put("frequency", "");
            regimen.put("duration", 0);
        } else {
            regimen.put("medicationId", uuidToString(specialDrug.getMedicationId()));
            regimen.put("regimenCode", nullToEmpty(specialDrug.getDrugIdentifier()));
            regimen.put("quantityPerDose", specialDrug.getQuantityPerDose());
            regimen.put("dosageUnit", nullToEmpty(specialDrug.getDosageUnit()));
            regimen.put("frequency", nullToEmpty(specialDrug.getFrequency()));
            regimen.put("duration", specialDrug.getDuration());
        }
        return regimen;
    }

    private java.util.Map<String, Object> buildVitalsMap(ElmisLogRecord record) {
        var vitals = new java.util.LinkedHashMap<String, Object>();

        boolean hasVitals = (record.getHeight() != null && record.getHeight().compareTo(BigDecimal.ZERO) > 0)
                || (record.getWeight() != null && record.getWeight().compareTo(BigDecimal.ZERO) > 0)
                || (record.getBloodPressure() != null && !record.getBloodPressure().isEmpty());

        if (!hasVitals) {
            vitals.put("height", "");
            vitals.put("heightDateTimeCollected", "");
            vitals.put("weight", "");
            vitals.put("weightDateTimeCollected", "");
            vitals.put("bloodPressure", "");
            vitals.put("bloodPressureDateTimeCollected", "");
        } else {
            vitals.put("height", formatDecimal(record.getHeight()));
            vitals.put("heightDateTimeCollected", formatDateTime(record.getHeightDateTimeCollected()));
            vitals.put("weight", formatDecimal(record.getWeight()));
            vitals.put("weightDateTimeCollected", formatDateTime(record.getWeightDateTimeCollected()));
            vitals.put("bloodPressure", nullToEmpty(record.getBloodPressure()));
            vitals.put("bloodPressureDateTimeCollected", formatDateTime(record.getBloodPressureDateTimeCollected()));
        }
        return vitals;
    }

    private java.util.Map<String, Object> buildPrescriptionDrugsMap(List<ElmisLogRecord> records) {
        List<java.util.Map<String, Object>> drugs = new ArrayList<>();
        String prescriptionDate = "";

        for (ElmisLogRecord record : records) {
            if (record.getSpecialDrug() == null || record.getSpecialDrug() != 1) {
                var drug = new java.util.LinkedHashMap<String, Object>();
                drug.put("medicationId", uuidToString(record.getMedicationId()));
                drug.put("drugCode", nullToEmpty(record.getDrugIdentifier()));
                drug.put("quantityPerDose", record.getQuantityPerDose());
                drug.put("dosageUnit", nullToEmpty(record.getDosageUnit()));
                drug.put("frequency", nullToEmpty(record.getFrequency()));
                drug.put("duration", record.getDuration());
                drugs.add(drug);

                prescriptionDate = formatDate(record.getPrescriptionDate());
            }
        }

        var prescription = new java.util.LinkedHashMap<String, Object>();
        prescription.put("prescriptionDrugs", drugs);
        prescription.put("date", prescriptionDate);
        return prescription;
    }

    private String mapSex(Short sex) {
        if (sex == null) return "";
        return sex == 1 ? "M" : "F";
    }

    private String formatDateTime(LocalDateTime dateTime) {
        if (dateTime == null) return "";
        return dateTime.format(DATETIME_FORMATTER);
    }

    private String formatDate(LocalDateTime dateTime) {
        if (dateTime == null) return "";
        return dateTime.format(DATE_FORMATTER);
    }

    private String formatDecimal(BigDecimal value) {
        if (value == null || value.compareTo(BigDecimal.ZERO) == 0) return "";
        return value.setScale(2, java.math.RoundingMode.HALF_UP).toString();
    }

    private String generateRandomArtNumber() {
        String chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        StringBuilder sb = new StringBuilder("sp-");
        ThreadLocalRandom random = ThreadLocalRandom.current();
        for (int i = 0; i < 5; i++) {
            sb.append(chars.charAt(random.nextInt(chars.length())));
        }
        return sb.toString();
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