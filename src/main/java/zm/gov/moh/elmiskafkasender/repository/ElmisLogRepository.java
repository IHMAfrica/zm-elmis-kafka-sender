package zm.gov.moh.elmiskafkasender.repository;

import io.r2dbc.spi.Row;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import zm.gov.moh.elmiskafkasender.entity.ElmisLogRecord;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

@Repository
@Slf4j
@RequiredArgsConstructor
public class ElmisLogRepository {

    private final DatabaseClient databaseClient;

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
                .map((row, metadata) -> mapToElmisLogRecord(row))
                .all()
                .doOnError(e -> log.error("Failed to fetch unprocessed ELMIS records", e));
    }

    public Flux<Integer> findPREPAndPEPRegimenIds() {
        String query = """
                SELECT RegimenId FROM dbo.SpecialDrugs sd JOIN dbo.DrugRegimens dr
                ON dr.Oid = sd.RegimenId WHERE dr.Oid in (14)
                """;

        return databaseClient.sql(query)
                .map((row, metadata) -> getInteger(row, "RegimenId"))
                .all()
                .doOnError(e -> log.error("Failed to fetch PREP and PEP regimen ids", e));
    }

    public Mono<Long> markRecordsAsSynced(List<UUID> oids) {
        if (oids == null || oids.isEmpty()) {
            return Mono.just(0L);
        }

        StringBuilder queryBuilder = new StringBuilder();
        queryBuilder.append("UPDATE dbo.ElmisLogs SET IsSynced = 1, DateModified = GETDATE() WHERE Oid IN (");

        for (int i = 0; i < oids.size(); i++) {
            if (i > 0) {
                queryBuilder.append(", ");
            }
            queryBuilder.append("'").append(oids.get(i)).append("'");
        }
        queryBuilder.append(")");

        return databaseClient.sql(queryBuilder.toString())
                .fetch()
                .rowsUpdated()
                .doOnSuccess(count -> log.debug("Marked {} ELMIS records as synced", count))
                .doOnError(e -> log.error("Failed to mark ELMIS records as synced", e));
    }

    private ElmisLogRecord mapToElmisLogRecord(Row row) {
        return ElmisLogRecord.builder()
                .oid(getUuid(row, "Oid"))
                .hmisCode(getString(row, "HmisCode"))
                .patientUuid(getUuid(row, "PatientUuid"))
                .artNumber(getString(row, "ArtNumber"))
                .cd4Count(getString(row, "Cd4Count"))
                .viralLoad(getString(row, "ViralLoad"))
                .dateOfBled(getDateTime(row, "DateOfBled"))
                .regimenId(getInteger(row, "RegimenId"))
                .drugIdentifier(getString(row, "DrugIdentifier"))
                .medicationId(getUuid(row, "MedicationId"))
                .quantityPerDose(getBigDecimal(row, "QuantityPerDose"))
                .dosageUnit(getString(row, "DosageUnit"))
                .frequency(getString(row, "Frequency"))
                .duration(getInteger(row, "Duration"))
                .height(getBigDecimal(row, "Height"))
                .heightDateTimeCollected(getDateTime(row, "HeightDateTimeCollected"))
                .weight(getBigDecimal(row, "Weight"))
                .weightDateTimeCollected(getDateTime(row, "WeightDateTimeCollected"))
                .bloodPressure(getString(row, "BloodPressure"))
                .bloodPressureDateTimeCollected(getDateTime(row, "BloodPressureDateTimeCollected"))
                .prescriptionDate(getDateTime(row, "PrescriptionDate"))
                .clinicianId(getUuid(row, "ClinicianId"))
                .prescriptionUuid(getUuid(row, "PrescriptionUuid"))
                .specialDrug(getInteger(row, "SpecialDrug"))
                .registrationDateTime(getDateTime(row, "RegistrationDateTime"))
                .dateOfBirth(getDateTime(row, "DateOfBirth"))
                .nrcNumber(getString(row, "NrcNumber"))
                .firstName(getString(row, "FirstName"))
                .lastName(getString(row, "LastName"))
                .patientId(getString(row, "PatientId"))
                .sex(getString(row, "Sex"))
                .isSynced(getBoolean(row, "IsSynced"))
                .isDeleted(getBoolean(row, "IsDeleted"))
                .build();
    }

    private UUID getUuid(Row row, String col) {
        try { return row.get(col, UUID.class); } catch (Exception e) { return null; }
    }

    private String getString(Row row, String col) {
        try { Object v = row.get(col); return v != null ? v.toString() : null; } catch (Exception e) { return null; }
    }

    private Integer getInteger(Row row, String col) {
        try { Object v = row.get(col); return v instanceof Number ? ((Number) v).intValue() : null; } catch (Exception e) { return null; }
    }

    private BigDecimal getBigDecimal(Row row, String col) {
        try { return row.get(col, BigDecimal.class); } catch (Exception e) { return null; }
    }

    private LocalDateTime getDateTime(Row row, String col) {
        try { return row.get(col, LocalDateTime.class); } catch (Exception e) { return null; }
    }

    private Boolean getBoolean(Row row, String col) {
        try { return row.get(col, Boolean.class); } catch (Exception e) { return null; }
    }
}