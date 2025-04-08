package com.example.javabigo.controller;

import com.example.javabigo.Payload;
import com.example.javabigo.service.BigoService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;

@RestController
public class HelloController {

    private final BigoService service;

    @Autowired
    public HelloController(BigoService service) {
        this.service = service;
    }

    @GetMapping("/{locationId}")
    public ResponseEntity<?> getData(@PathVariable String locationId) {
        if(locationId.equals("health")) {
            //return 200
            return new ResponseEntity<>(HttpStatus.OK);
        }
        try {
            AbstractMap.SimpleEntry<Integer, Payload> result = service.getData(locationId);

            if (result == null) {
                System.out.println("Data not found");
                return ResponseEntity.status(HttpStatus.NOT_FOUND).build();
            }

            Map<String, Object> response = generateResponse(locationId, result);

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    @PutMapping("/{locationId}")
    public ResponseEntity<Void> saveData(@PathVariable String locationId, @RequestBody Payload payload) {
        service.saveData(locationId, payload, true);
        return ResponseEntity.status(HttpStatus.CREATED).build();
    }

    private static Map<String, Object> generateResponse(String locationId, AbstractMap.SimpleEntry<Integer, Payload> result) {
        int modificationCount = result.getKey();
        Payload payload = result.getValue();

        Map<String, Object> response = new HashMap<>();
        response.put("id", payload.getId());
        response.put("modification_count", modificationCount);
        response.put("seismic_activity", payload.getSeismicActivity());
        response.put("temperature_c", payload.getTemperatureC());
        response.put("radiation_level", payload.getRadiationLevel());
        response.put("location_id", locationId);
        return response;
    }

    @PutMapping("/replicate/{locationId}")
    public ResponseEntity<Void> replicateData(@PathVariable String locationId, @RequestBody Payload payload) {
        // Save the data without triggering further replication
        service.saveData(locationId, payload, false);
        return ResponseEntity.ok().build();
    }
}