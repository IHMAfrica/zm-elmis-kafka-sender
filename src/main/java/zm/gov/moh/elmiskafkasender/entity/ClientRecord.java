package zm.gov.moh.elmiskafkasender.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ClientRecord {
    private UUID oid;
    private String firstName;
    private String surname;
    private Short sex;
    private LocalDateTime dob;
    private Boolean isDobEstimated;
    private String nrc;
    private Boolean noNrc;
    private String napsaNumber;
    private String underFiveCardNumber;
    private String nupn;
    private LocalDateTime registrationDate;
    private Integer createdIn;
    private Boolean elmissSyncStatus;
    private String hmisCode;
}