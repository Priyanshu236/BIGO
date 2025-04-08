package com.example.javabigo.service;

import com.example.javabigo.Payload;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Service
public class ReplicationService {
    private final RestTemplate restTemplate = new RestTemplate();
    private final List<String> peerNodeIps;
    private final String currentNodeIp;

    @Autowired
    public ReplicationService(@Value("${current.node.ip}") String currentNodeIp,
                              @Value("${all.node.ips}") String allNodeIps) {
        this.currentNodeIp = currentNodeIp;
        this.peerNodeIps = Arrays.stream(allNodeIps.split(","))
                .map(String::trim)
                .filter(ip -> !ip.equals(this.currentNodeIp))
                .collect(Collectors.toList());

        System.out.println("My IP: " + currentNodeIp);
        System.out.println("Other IPs: " + peerNodeIps);
    }

    public void replicateData(String locationId, Payload payload) {
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        for (String peerIp : peerNodeIps) {
            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                try {
                    String url = "http://" + peerIp + "/replicate/" + locationId;
                    HttpHeaders headers = new HttpHeaders();
                    headers.setContentType(MediaType.APPLICATION_JSON);

                    HttpEntity<Payload> request = new HttpEntity<>(payload, headers);
                    restTemplate.exchange(url, HttpMethod.PUT, request, Void.class);

                } catch (Exception e) {
                    System.err.println("Failed to replicate data to node [" + peerIp + "]: " + e.getMessage());
                }
            });

            futures.add(future);
        }

        // Wait for all futures to complete (equivalent to Promise.all)
        CompletableFuture<Void> allFutures = CompletableFuture.allOf(
                futures.toArray(new CompletableFuture[0])
        );

        // Block until all replications are complete
        try {
            allFutures.get();
        } catch (InterruptedException | ExecutionException e) {
            Thread.currentThread().interrupt();
            System.err.println("Error while waiting for replications to complete: " + e.getMessage());
        }
    }
}