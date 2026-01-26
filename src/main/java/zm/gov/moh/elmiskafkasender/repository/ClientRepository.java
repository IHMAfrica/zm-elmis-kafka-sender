package zm.gov.moh.elmiskafkasender.repository;

import io.r2dbc.spi.Row;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import zm.gov.moh.elmiskafkasender.entity.ClientRecord;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

@Repository
@Slf4j
@RequiredArgsConstructor
public class ClientRepository {

    private final DatabaseClient databaseClient;

    public Flux<ClientRecord> findUnprocessedClients(int batchSize) {
        String query = """
                SELECT TOP (:batchSize)
                    c.Oid,
                    c.FirstName,
                    c.Surname,
                    c.Sex,
                    c.DOB,
                    c.IsDOBEstimated,
                    c.NRC,
                    c.NoNRC,
                    c.NAPSANumber,
                    c.UnderFiveCardNumber,
                    c.NUPN,
                    c.RegistrationDate,
                    c.CreatedIn,
                    c.ELMISSyncStatus,
                    f.HMISCode
                FROM dbo.Clients c WITH (READPAST)
                LEFT JOIN dbo.Facilities f ON c.CreatedIn = f.Oid
                WHERE (c.ELMISSyncStatus IS NULL OR c.ELMISSyncStatus = 0)
                  AND (c.IsDeleted IS NULL OR c.IsDeleted = 0)
                ORDER BY c.DateCreated ASC
                """;

        return databaseClient.sql(query)
                .bind("batchSize", batchSize)
                .map((row, metadata) -> mapToClientRecord(row))
                .all()
                .doOnError(e -> log.error("Failed to fetch unprocessed clients", e));
    }

    public Mono<Long> markClientsAsSynced(List<UUID> clientOids) {
        if (clientOids == null || clientOids.isEmpty()) {
            return Mono.just(0L);
        }

        StringBuilder queryBuilder = new StringBuilder();
        queryBuilder.append("UPDATE dbo.Clients SET ELMISSyncStatus = 1, DateModified = GETDATE() WHERE Oid IN (");

        for (int i = 0; i < clientOids.size(); i++) {
            if (i > 0) {
                queryBuilder.append(", ");
            }
            queryBuilder.append("'").append(clientOids.get(i)).append("'");
        }
        queryBuilder.append(")");

        return databaseClient.sql(queryBuilder.toString())
                .fetch()
                .rowsUpdated()
                .doOnSuccess(count -> log.debug("Marked {} clients as synced", count))
                .doOnError(e -> log.error("Failed to mark clients as synced", e));
    }

    private ClientRecord mapToClientRecord(Row row) {
        return ClientRecord.builder()
                .oid(getUuid(row, "Oid"))
                .firstName(getString(row, "FirstName"))
                .surname(getString(row, "Surname"))
                .sex(getShort(row, "Sex"))
                .dob(getDateTime(row, "DOB"))
                .isDobEstimated(getBoolean(row, "IsDOBEstimated"))
                .nrc(getString(row, "NRC"))
                .noNrc(getBoolean(row, "NoNRC"))
                .napsaNumber(getString(row, "NAPSANumber"))
                .underFiveCardNumber(getString(row, "UnderFiveCardNumber"))
                .nupn(getString(row, "NUPN"))
                .registrationDate(getDateTime(row, "RegistrationDate"))
                .createdIn(getInteger(row, "CreatedIn"))
                .elmissSyncStatus(getBoolean(row, "ELMISSyncStatus"))
                .hmisCode(getString(row, "HMISCode"))
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

    private Short getShort(Row row, String col) {
        try { Object v = row.get(col); return v instanceof Number ? ((Number) v).shortValue() : null; } catch (Exception e) { return null; }
    }

    private LocalDateTime getDateTime(Row row, String col) {
        try { return row.get(col, LocalDateTime.class); } catch (Exception e) { return null; }
    }

    private Boolean getBoolean(Row row, String col) {
        try { return row.get(col, Boolean.class); } catch (Exception e) { return null; }
    }
}