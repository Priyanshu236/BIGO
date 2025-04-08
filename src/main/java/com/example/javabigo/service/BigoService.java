package com.example.javabigo.service;

import com.example.javabigo.Payload;

import java.util.AbstractMap;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class BigoService {

    private final ConcurrentHashMap<String, AbstractMap.SimpleEntry<Integer, Payload>> dataStore = new ConcurrentHashMap<>();
    private final ReplicationService replicationService;

    @Autowired
    public BigoService(ReplicationService replicationService) {
        this.replicationService = replicationService;
    }

    public void saveData(String locationId, Payload payload, boolean shouldReplicateToOthers) {
        AbstractMap.SimpleEntry<Integer, Payload> existing = dataStore.get(locationId);
        int currentCount = existing != null ? existing.getKey() : 0;
        int newCount = currentCount + 1;

        // Create a new Payload object and copy properties
        Payload updatedPayload = new Payload();
        updatedPayload.setId(payload.getId());
        updatedPayload.setSeismicActivity(payload.getSeismicActivity());
        updatedPayload.setTemperatureC(payload.getTemperatureC());
        updatedPayload.setRadiationLevel(payload.getRadiationLevel());
        updatedPayload.setName(payload.getName());

        dataStore.put(locationId, new AbstractMap.SimpleEntry<>(newCount, updatedPayload));
        if(shouldReplicateToOthers) {
            replicationService.replicateData(locationId, updatedPayload);
        }
        System.out.println("Saved data for location [" + locationId + "] with modification count [" + newCount + "]");
    }

    public AbstractMap.SimpleEntry<Integer, Payload> getData(String locationId) {
        return dataStore.get(locationId);
    }
}
