package zm.gov.moh.elmiskafkasender.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class VitalsDto {

    private String height;

    private String heightDateTimeCollected;

    private String weight;

    private String weightDateTimeCollected;

    private String bloodPressure;

    private String bloodPressureDateTimeCollected;
}