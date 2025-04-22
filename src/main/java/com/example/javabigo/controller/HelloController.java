package com.example.javabigo.controller;

import com.example.javabigo.Payload;
import com.example.javabigo.service.BigoService;
import com.example.javabigo.service.ReplicationService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
public class HelloController {

    private final String firstNodeIp;
    private final String currentNodeIp;
    private final ReplicationService replicationService;
    private final BigoService bigoService;
    private final String redirectUrl;

    @Autowired
    public HelloController(ReplicationService replicationService, BigoService bigoService, @Value("${current.node.ip}") String currentNodeIp,
                           @Value("${all.node.ips}") String allNodeIps, @Value("${server.port}") String httpServerPort) {
        List<String> allNodes = Arrays.stream(allNodeIps.split(","))
                .map(String::trim)
                .toList();
        this.currentNodeIp = currentNodeIp;
        this.firstNodeIp = allNodes.getFirst();
        this.replicationService = replicationService;
        this.bigoService = bigoService;
        this.redirectUrl = "http://" + allNodes.getFirst() + ":" + httpServerPort + "/";
    }

    @GetMapping("/{locationId}")
    public ResponseEntity<?> getData(@PathVariable String locationId) {
        if (locationId.equals("health")) {
            //return 200
            return new ResponseEntity<>(HttpStatus.OK);
        }
        try {
            Payload result = replicationService.getData(locationId, false);

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

        if (!currentNodeIp.equals(firstNodeIp)) {
            String firstNodeUrl = redirectUrl + locationId;

            HttpHeaders headers = new HttpHeaders();
            headers.setLocation(URI.create(firstNodeUrl));
            return new ResponseEntity<>(headers, HttpStatus.TEMPORARY_REDIRECT);
        }

        replicationService.saveData(locationId, payload);
        return ResponseEntity.status(HttpStatus.CREATED).build();
    }

    @GetMapping("/entries/count")
    public ResponseEntity<?> getCount() {
        long result = replicationService.getMapEntriesCount();
        return ResponseEntity.ok(result);
    }

    private Map<String, Object> generateResponse(String locationId, Payload result) {

        Map<String, Object> response = new HashMap<>();
        response.put("id", result.getId());
        response.put("modification_count", bigoService.getModificationCountOf(locationId));
        response.put("seismic_activity", result.getSeismicActivity());
        response.put("temperature_c", result.getTemperatureC());
        response.put("radiation_level", result.getRadiationLevel());
        response.put("location_id", locationId);
        return response;
    }
}