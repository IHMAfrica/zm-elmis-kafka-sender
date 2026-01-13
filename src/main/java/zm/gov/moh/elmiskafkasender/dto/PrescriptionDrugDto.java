package zm.gov.moh.elmiskafkasender.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PrescriptionDrugDto {

    private String medicationId;

    private String drugCode;

    private BigDecimal quantityPerDose;

    private String dosageUnit;

    private String frequency;

    private Integer duration;
}