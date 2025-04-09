package com.example.javabigo.controller;

import com.example.javabigo.Payload;
import com.example.javabigo.service.ReplicationService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;

@RestController
public class HelloController {

    private final ReplicationService replicationService;

    @Autowired
    public HelloController(ReplicationService service) {
        this.replicationService = service;
    }

    @GetMapping("/{locationId}")
    public ResponseEntity<?> getData(@PathVariable String locationId) {
        if(locationId.equals("health")) {
            //return 200
            return new ResponseEntity<>(HttpStatus.OK);
        }
        try {
            Payload result = replicationService.getData(locationId);

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
        replicationService.saveData(locationId, payload, true);
        return ResponseEntity.status(HttpStatus.CREATED).build();
    }

    private static Map<String, Object> generateResponse(String locationId, Payload result) {

        Map<String, Object> response = new HashMap<>();
        response.put("id", result.getId());
        response.put("modification_count", result.getModificationCount());
        response.put("seismic_activity", result.getSeismicActivity());
        response.put("temperature_c", result.getTemperatureC());
        response.put("radiation_level", result.getRadiationLevel());
        response.put("location_id", locationId);
        return response;
    }

    @PutMapping("/replicate/{locationId}")
    public ResponseEntity<Void> replicateData(@PathVariable String locationId, @RequestBody Payload payload) {
        // Save the data without triggering further replication
        replicationService.saveData(locationId, payload, false);
        return ResponseEntity.ok().build();
    }
}