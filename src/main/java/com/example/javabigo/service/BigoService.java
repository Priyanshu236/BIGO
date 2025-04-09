package com.example.javabigo.service;

import com.example.javabigo.Payload;

import java.util.concurrent.ConcurrentHashMap;

import org.springframework.stereotype.Service;

@Service
public class BigoService {

    private final ConcurrentHashMap<String, Payload> dataStore = new ConcurrentHashMap<>();

    public Payload saveData(String locationId, Payload payload) {
        Payload existing = dataStore.get(locationId);
        int currentCount = existing != null ? existing.getModificationCount() : 0;

        // Create a new Payload object and copy properties
        Payload updatedPayload = new Payload();
        updatedPayload.setId(payload.getId());
        updatedPayload.setSeismicActivity(payload.getSeismicActivity());
        updatedPayload.setTemperatureC(payload.getTemperatureC());
        updatedPayload.setRadiationLevel(payload.getRadiationLevel());
        updatedPayload.setModificationCount(currentCount + 1);

        dataStore.put(locationId, updatedPayload);

        return updatedPayload;
    }

    public void saveDataToMap(String locationId, Payload payload) {
        dataStore.put(locationId, payload);
    }

    public Payload getData(String locationId) {
        return dataStore.get(locationId);
    }
}
