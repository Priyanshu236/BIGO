package com.example.javabigo.service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.stereotype.Service;

@Service
public class BigoService {

    private final Map<String, DataEntry> dataStore = new ConcurrentHashMap<>();

    public void saveData(String locationId, byte[] shard) {
        DataEntry existingEntry = dataStore.get(locationId);
        if (existingEntry == null) {
            dataStore.put(locationId, new DataEntry(shard, 1));
        } else {
            existingEntry.shard = shard;
            existingEntry.incrementModificationCount();
        }
    }

    public byte[] getShardOf(String locationId) {
        DataEntry entry = dataStore.get(locationId);
        return entry != null ? entry.shard : null;
    }

    public int getModificationCountOf(String locationId) {
        DataEntry entry = dataStore.get(locationId);
        return entry != null ? entry.modificationCount : 0;
    }

    public int mapKeysCount() {
        return dataStore.size();
    }

    private static class DataEntry {
        private byte[] shard;
        private int modificationCount;

        public DataEntry(byte[] shard, int modificationCount) {
            this.shard = shard;
            this.modificationCount = modificationCount;
        }

        public void incrementModificationCount() {
            this.modificationCount++;
        }
    }
}
