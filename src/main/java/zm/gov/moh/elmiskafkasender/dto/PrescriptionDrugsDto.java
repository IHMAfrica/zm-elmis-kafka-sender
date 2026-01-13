package zm.gov.moh.elmiskafkasender.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PrescriptionDrugsDto {

    @Builder.Default
    private List<PrescriptionDrugDto> prescriptionDrugs = new ArrayList<>();

    private String date;
}