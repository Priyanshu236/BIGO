package com.example.javabigo.service;

import java.util.concurrent.ConcurrentHashMap;

import org.springframework.stereotype.Service;

@Service
public class BigoService {

    private final ConcurrentHashMap<String, DataEntry> dataStore = new ConcurrentHashMap<>();

    public void saveData(String locationId, byte[] shard) {
        dataStore.compute(locationId, (key, existingEntry) -> {
            if (existingEntry == null) {
                return new DataEntry(shard, 1);
            } else {
                existingEntry.shard = shard;
                existingEntry.incrementModificationCount();
                return existingEntry;
            }
        });
    }

    public byte[] getShardOf(String locationId) {
        DataEntry entry = dataStore.get(locationId);
        return entry != null ? entry.shard : null;
    }

    public int getModificationCountOf(String locationId) {
        DataEntry entry = dataStore.get(locationId);
        return entry != null ? entry.modificationCount : 0;
    }

    public long mapKeysCount() {
        return dataStore.mappingCount();
    }

    private class DataEntry {
        private byte[] shard;
        int modificationCount;

        public DataEntry(byte[] shard, int modificationCount) {
            this.shard = shard;
            this.modificationCount = modificationCount;
        }

        public void incrementModificationCount() {
            this.modificationCount++;
        }
    }
}
