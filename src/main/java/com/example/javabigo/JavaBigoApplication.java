package com.example.javabigo;

import com.example.javabigo.erasure.ReedSolomon;
import com.example.javabigo.service.PayloadCodec;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Arrays;
import java.util.UUID;

@SpringBootApplication
public class JavaBigoApplication {

	public static void main(String[] args) {
        Double tmp = 4.000002;
        SpringApplication.run(JavaBigoApplication.class, args);
        Payload payload = new Payload();
        payload.setId(UUID.randomUUID().toString());
        payload.setSeismicActivity(tmp);
        payload.setTemperatureC(tmp);
        payload.setRadiationLevel(tmp);
        payload.setModificationCount(3);

        PayloadCodec codec = new PayloadCodec();

        try {
            byte[][] shards = codec.encode(payload);
            Payload output = codec.decode(shards);
            System.out.println("Erasure decoding -> " + output);
        } catch (Exception e) {
            System.out.println("Something went wrong");
        }

	}

}
