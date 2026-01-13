package zm.gov.moh.elmiskafkasender.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class MshDto {

    private String timestamp;

    private String sendingApplication;

    private String receivingApplication;

    private String messageId;

    private String hmisCode;

    @JsonProperty("mflCode")
    private String mflCode;

    private String messageType;
}