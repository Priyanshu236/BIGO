package com.example.javabigo.service;

import com.example.javabigo.Payload;
import com.example.javabigo.erasure.ReedSolomon;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class PayloadCodec {
    public static final int DATA_SHARDS = 4;
    public static final int PARITY_SHARDS = 3;
    public static final int TOTAL_SHARDS = DATA_SHARDS + PARITY_SHARDS;
    public static final int BYTES_IN_INT = 4;

    private final ObjectMapper objectMapper;

    public PayloadCodec() {
        this.objectMapper = new ObjectMapper();
    }

    /**
     * Encodes a Payload object into Reed-Solomon encoded shards
     * @param payload The payload to encode
     * @return An array of byte arrays, each representing a shard
     */
    public byte[][] encode(Payload payload) throws IOException {
        // Convert payload to JSON bytes
        byte[] payloadBytes = objectMapper.writeValueAsBytes(payload);
        final int payloadSize = payloadBytes.length;

        // Figure out how big each shard will be.
        // The total size stored will be the payload size (4 bytes) plus the payload.
        final int storedSize = payloadSize + BYTES_IN_INT;
        final int shardSize = (storedSize + DATA_SHARDS - 1) / DATA_SHARDS;

        // Create a buffer holding the payload size, followed by
        // the contents of the payload.
        final int bufferSize = shardSize * DATA_SHARDS;
        final byte[] allBytes = new byte[bufferSize];

        // Store payload size at the beginning
        ByteBuffer.wrap(allBytes).putInt(payloadSize);

        // Copy payload bytes after the size
        System.arraycopy(payloadBytes, 0, allBytes, BYTES_IN_INT, payloadSize);

        // Make the buffers to hold the shards.
        byte[][] shards = new byte[TOTAL_SHARDS][shardSize];

        // Fill in the data shards
        for (int i = 0; i < DATA_SHARDS; i++) {
            System.arraycopy(allBytes, i * shardSize, shards[i], 0, shardSize);
        }

        // Use Reed-Solomon to calculate the parity.
        ReedSolomon reedSolomon = ReedSolomon.create(DATA_SHARDS, PARITY_SHARDS);
        reedSolomon.encodeParity(shards, 0, shardSize);

        // Return the array of shards
        return shards;
    }

    /**
     * Decodes Reed-Solomon encoded shards back into a Payload object
     * @param shards The array of shards, some of which may be null
     * @return The reconstructed Payload object
     */
    public Payload decode(byte[][] shards) throws IOException {
        if (shards.length != TOTAL_SHARDS) {
            throw new IllegalArgumentException("Expected " + TOTAL_SHARDS + " shards, got " + shards.length);
        }

        // Determine shard size from the first non-null shard
        int shardSize = -1;
        for (byte[] shard : shards) {
            if (shard != null) {
                shardSize = shard.length;
                break;
            }
        }

        if (shardSize == -1) {
            throw new IllegalArgumentException("All shards are null");
        }

        // Count available shards
        int shardCount = 0;
        boolean[] shardPresent = new boolean[TOTAL_SHARDS];
        for (int i = 0; i < TOTAL_SHARDS; i++) {
            if (shards[i] != null) {
                shardPresent[i] = true;
                shardCount++;
            }
        }

        // We need at least DATA_SHARDS to be able to reconstruct the data
        if (shardCount < DATA_SHARDS) {
            throw new IllegalArgumentException("Not enough shards present. Need at least " + DATA_SHARDS + ", have " + shardCount);
        }

        // If we're missing any data shards, we need to use Reed-Solomon to reconstruct them
        if (shardCount < TOTAL_SHARDS) {
            // Create a reed solomon codec
            ReedSolomon reedSolomon = ReedSolomon.create(DATA_SHARDS, PARITY_SHARDS);

            // Create a copy of the shards matrix, with null shards replaced by new byte arrays
            byte[][] shardsCopy = new byte[TOTAL_SHARDS][];
            for (int i = 0; i < TOTAL_SHARDS; i++) {
                if (shards[i] != null) {
                    shardsCopy[i] = shards[i];
                } else {
                    shardsCopy[i] = new byte[shardSize];
                }
            }

            // Reconstruct missing shards
            reedSolomon.decodeMissing(shardsCopy, shardPresent, 0, shardSize);

            // Use the reconstructed shards for further processing
            shards = shardsCopy;
        }

        // Combine the data shards to get the original data
        byte[] allBytes = new byte[shardSize * DATA_SHARDS];
        for (int i = 0; i < DATA_SHARDS; i++) {
            System.arraycopy(shards[i], 0, allBytes, i * shardSize, shardSize);
        }

        // Extract the original payload size
        int payloadSize = ByteBuffer.wrap(allBytes).getInt();

        // Extract the payload bytes
        byte[] payloadBytes = Arrays.copyOfRange(allBytes, BYTES_IN_INT, BYTES_IN_INT + payloadSize);

        // Convert back to Payload object
        return objectMapper.readValue(payloadBytes, Payload.class);
    }
}
