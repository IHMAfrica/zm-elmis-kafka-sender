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
public class PatientProfileDto {

    private MshDto msh;

    private String registrationDateTime;

    private String dateOfBirth;

    private UUID patientUuid;

    private String nrcNumber;

    private String firstName;

    private String lastName;

    private String patientId;

    private String sex;
}