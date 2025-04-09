package com.example.javabigo;

import com.example.javabigo.erasure.ReedSolomon;
import com.example.javabigo.service.PayloadCodec;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Arrays;

@SpringBootApplication
public class JavaBigoApplication {

	public static void main(String[] args) {
        SpringApplication.run(JavaBigoApplication.class, args);
        Payload payload = new Payload();
        payload.setId("22");
        payload.setSeismicActivity("22");
        payload.setTemperatureC("22");
        payload.setRadiationLevel("22");
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
