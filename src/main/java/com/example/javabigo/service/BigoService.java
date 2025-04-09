package com.example.javabigo.service;

import com.example.javabigo.Payload;

import java.util.concurrent.ConcurrentHashMap;

import org.springframework.stereotype.Service;

@Service
public class BigoService {

    private final ConcurrentHashMap<String, byte[]> dataStore = new ConcurrentHashMap<>();

    public void saveData(String locationId, byte[] shard) {
        dataStore.put(locationId, shard);
    }


    public byte[] getData(String locationId) {
        return dataStore.get(locationId);
    }
}
