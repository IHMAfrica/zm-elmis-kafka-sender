package zm.gov.moh.elmiskafkasender.repository;

import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import zm.gov.moh.elmiskafkasender.entity.ElmisLogRecord;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

@Repository
public class ElmisLogRepository {

    private final DatabaseClient databaseClient;

    public ElmisLogRepository(DatabaseClient databaseClient) {
        this.databaseClient = databaseClient;
    }

    public Flux<ElmisLogRecord> findUnprocessedRecords(int batchSize) {
        String query = """
            SELECT TOP (:batchSize)
                Oid, HmisCode, PatientUuid, ArtNumber, Cd4Count, ViralLoad,
                DateOfBled, RegimenId, DrugIdentifier, MedicationId, QuantityPerDose,
                DosageUnit, Frequency, Duration, Height, HeightDateTimeCollected,
                Weight, WeightDateTimeCollected, BloodPressure, BloodPressureDateTimeCollected,
                PrescriptionDate, ClinicianId, PrescriptionUuid, SpecialDrug,
                RegistrationDateTime, DateOfBirth, NrcNumber, FirstName, LastName,
                PatientId, Sex, IsSynced, IsDeleted
            FROM dbo.ElmisLogs WITH (READPAST)
            WHERE (IsSynced IS NULL OR IsSynced = 0)
              AND (IsDeleted IS NULL OR IsDeleted = 0)
            ORDER BY Oid ASC
            """;

        return databaseClient.sql(query)
                .bind("batchSize", batchSize)
                .map((row, metadata) -> ElmisLogRecord.builder()
                        .oid(row.get("Oid", UUID.class))
                        .hmisCode(row.get("HmisCode", String.class))
                        .patientUuid(row.get("PatientUuid", UUID.class))
                        .artNumber(row.get("ArtNumber", String.class))
                        .cd4Count(row.get("Cd4Count", String.class))
                        .viralLoad(row.get("ViralLoad", String.class))
                        .dateOfBled(row.get("DateOfBled", LocalDateTime.class))
                        .regimenId(row.get("RegimenId", Integer.class))
                        .drugIdentifier(row.get("DrugIdentifier", String.class))
                        .medicationId(row.get("MedicationId", UUID.class))
                        .quantityPerDose(row.get("QuantityPerDose", java.math.BigDecimal.class))
                        .dosageUnit(row.get("DosageUnit", String.class))
                        .frequency(row.get("Frequency", String.class))
                        .duration(row.get("Duration", Integer.class))
                        .height(row.get("Height", java.math.BigDecimal.class))
                        .heightDateTimeCollected(row.get("HeightDateTimeCollected", LocalDateTime.class))
                        .weight(row.get("Weight", java.math.BigDecimal.class))
                        .weightDateTimeCollected(row.get("WeightDateTimeCollected", LocalDateTime.class))
                        .bloodPressure(row.get("BloodPressure", String.class))
                        .bloodPressureDateTimeCollected(row.get("BloodPressureDateTimeCollected", LocalDateTime.class))
                        .prescriptionDate(row.get("PrescriptionDate", LocalDateTime.class))
                        .clinicianId(row.get("ClinicianId", UUID.class))
                        .prescriptionUuid(row.get("PrescriptionUuid", UUID.class))
                        .specialDrug(row.get("SpecialDrug", Integer.class))
                        .registrationDateTime(row.get("RegistrationDateTime", LocalDateTime.class))
                        .dateOfBirth(row.get("DateOfBirth", LocalDateTime.class))
                        .nrcNumber(row.get("NrcNumber", String.class))
                        .firstName(row.get("FirstName", String.class))
                        .lastName(row.get("LastName", String.class))
                        .patientId(row.get("PatientId", String.class))
                        .sex(row.get("Sex", String.class))
                        .isSynced(row.get("IsSynced", Boolean.class))
                        .isDeleted(row.get("IsDeleted", Boolean.class))
                        .build())
                .all();
    }

    public Mono<Long> markRecordsAsSynced(List<UUID> oids) {
        if (oids == null || oids.isEmpty()) {
            return Mono.just(0L);
        }

        StringBuilder queryBuilder = new StringBuilder();
        queryBuilder.append("UPDATE dbo.ElmisLogs SET IsSynced = 1 WHERE Oid IN (");

        for (int i = 0; i < oids.size(); i++) {
            if (i > 0) {
                queryBuilder.append(", ");
            }
            queryBuilder.append("'").append(oids.get(i)).append("'");
        }
        queryBuilder.append(")");

        return databaseClient.sql(queryBuilder.toString())
                .fetch()
                .rowsUpdated();
    }
}
