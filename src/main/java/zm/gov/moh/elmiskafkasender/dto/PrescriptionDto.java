package zm.gov.moh.elmiskafkasender.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PrescriptionDto {

    private MshDto msh;

    private UUID patientUuid;

    private String artNumber;

    private String cd4;

    private String viralLoad;

    private String dateOfBled;

    private Integer regimenId;

    private UUID clinicianId;

    private UUID prescriptionUuid;

    private RegimenDto regimen;

    private VitalsDto vitals;

    private PrescriptionDrugsDto prescription;
}