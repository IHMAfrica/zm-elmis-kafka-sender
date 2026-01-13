// src/main/java/com/carepro/elmis/entity/ElmisLogRecord.java
package zm.gov.moh.elmiskafkasender.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.Table;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table("ElmisLogs")
public class ElmisLogRecord {

    @Id
    @Column("Oid")
    private UUID oid;

    @Column("HmisCode")
    private String hmisCode;

    @Column("PatientUuid")
    private UUID patientUuid;

    @Column("ArtNumber")
    private String artNumber;

    @Column("Cd4Count")
    private String cd4Count;

    @Column("ViralLoad")
    private String viralLoad;

    @Column("DateOfBled")
    private LocalDateTime dateOfBled;

    @Column("RegimenId")
    private Integer regimenId;

    @Column("DrugIdentifier")
    private String drugIdentifier;

    @Column("MedicationId")
    private UUID medicationId;

    @Column("QuantityPerDose")
    private BigDecimal quantityPerDose;

    @Column("DosageUnit")
    private String dosageUnit;

    @Column("Frequency")
    private String frequency;

    @Column("Duration")
    private Integer duration;

    @Column("Height")
    private BigDecimal height;

    @Column("HeightDateTimeCollected")
    private LocalDateTime heightDateTimeCollected;

    @Column("Weight")
    private BigDecimal weight;

    @Column("WeightDateTimeCollected")
    private LocalDateTime weightDateTimeCollected;

    @Column("BloodPressure")
    private String bloodPressure;

    @Column("BloodPressureDateTimeCollected")
    private LocalDateTime bloodPressureDateTimeCollected;

    @Column("PrescriptionDate")
    private LocalDateTime prescriptionDate;

    @Column("ClinicianId")
    private UUID clinicianId;

    @Column("PrescriptionUuid")
    private UUID prescriptionUuid;

    @Column("SpecialDrug")
    private Integer specialDrug;

    @Column("IsSynced")
    private Boolean isSynced;

    @Column("IsDeleted")
    private Boolean isDeleted;

    @Column("RegistrationDateTime")
    private LocalDateTime registrationDateTime;

    @Column("DateOfBirth")
    private LocalDateTime dateOfBirth;

    @Column("NrcNumber")
    private String nrcNumber;

    @Column("FirstName")
    private String firstName;

    @Column("LastName")
    private String lastName;

    @Column("PatientId")
    private String patientId;

    @Column("Sex")
    private String sex;
}