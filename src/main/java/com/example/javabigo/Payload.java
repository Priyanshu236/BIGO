package com.example.javabigo;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class Payload {
    private String id;

    @JsonProperty("seismic_activity")
    private Double seismicActivity;

    @JsonProperty("temperature_c")
    private Double temperatureC;

    @JsonProperty("radiation_level")
    private Double radiationLevel;
}

