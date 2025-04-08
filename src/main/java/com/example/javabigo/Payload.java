package com.example.javabigo;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class Payload {
    private String id;

    @JsonProperty("seismic_activity")
    private String seismicActivity;

    @JsonProperty("temperature_c")
    private String temperatureC;

    @JsonProperty("radiation_level")
    private String radiationLevel;

    private String name;
}

