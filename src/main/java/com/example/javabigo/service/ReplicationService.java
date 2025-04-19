package com.example.javabigo.service;

import com.example.javabigo.Payload;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Service
public class ReplicationService {
    private final List<String> peerNodeIps;
    private final String currentNodeIp;
    private final BigoService bigoService;
    private static final int SOCKET_PORT = 8089;
    private Map<String, Socket> peerConnections = new ConcurrentHashMap<>();
    private Map<String, byte[]> requestedShardsResponse = new ConcurrentHashMap<>();
    private Map<String, Integer> nodesIndex = new HashMap<>();
    private PayloadCodec payloadCodec = new PayloadCodec();

    @Autowired
    public ReplicationService(BigoService bigoService, @Value("${current.node.ip}") String currentNodeIp,
                              @Value("${all.node.ips}") String allNodeIps) {
        this.currentNodeIp = currentNodeIp;
        this.bigoService = bigoService;
        List<String> allNodes = Arrays.stream(allNodeIps.split(","))
                .map(String::trim)
                .toList();

        for (int i = 0; i < allNodes.size(); i++) {
            nodesIndex.put(allNodes.get(i), i);
        }

        this.peerNodeIps = allNodes.stream()
                .filter(ip -> !ip.equals(this.currentNodeIp))
                .collect(Collectors.toList());

        runSocketServer(currentNodeIp);
        connectToPeers();
    }

    public void saveData(String locationId, Payload payload) {
        if (bigoService.getData(locationId) == null) {
            try {
                shardAndReplicateData(locationId, payload);
            } catch (Exception e) {
                System.err.println("Error encoding data: " + e.getMessage());
            }
        } else {
            Payload prevData = getData(locationId);
            payload.setModificationCount(prevData.getModificationCount() + 1);
            try {
                shardAndReplicateData(locationId, payload);
            } catch (Exception e) {
                System.err.println("Error encoding data: " + e.getMessage());
            }
        }
    }

    private void shardAndReplicateData(String locationId, Payload payload) throws IOException {
        byte[][] shards = payloadCodec.encode(payload);
        bigoService.saveData(locationId, shards[nodesIndex.get(currentNodeIp)]);
        replicateData(locationId, shards);
    }


    public Payload getData(String locationId) {
        byte[][] shards = new byte[PayloadCodec.TOTAL_SHARDS][];
        byte[] currentShard = bigoService.getData(locationId);
        if(currentShard == null) {
            return null;
        }
        shards[nodesIndex.get(currentNodeIp)] = currentShard;

        Map<String, CompletableFuture<Void>> futures = new HashMap<>();

        // Make all requests at once
        for (String peerIp : peerNodeIps) {
            String requestId = UUID.randomUUID().toString();
            Socket peerSocket = peerConnections.get(peerIp);

            if(peerSocket == null) {
                shards[nodesIndex.get(peerIp)] = null;
                continue;
            }

            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                try {
                    fetchShardFromNode(peerIp, locationId, requestId);
                    while (requestedShardsResponse.get(requestId) == null) {
                        if(peerSocket.isClosed() || !peerSocket.isConnected()) {
                            shards[nodesIndex.get(peerIp)] = null;
                            peerConnections.remove(peerIp);
                            break;
                        }
                    }
                    shards[nodesIndex.get(peerIp)] = requestedShardsResponse.get(requestId);
                    requestedShardsResponse.remove(requestId);
                } catch (Exception e) {
                    shards[nodesIndex.get(peerIp)] = null;
                    peerConnections.remove(peerIp);
                }
            });

            futures.put(requestId, future);
        }
        CompletableFuture[] futureArray = futures.values().toArray(new CompletableFuture[futures.size()]);
        CompletableFuture.allOf(futureArray).join();

        try {
            return payloadCodec.decode(shards);
        } catch (IOException e) {
            throw new RuntimeException("Decoding failed", e);
        }
    }



    private void saveIndividualShard(String locationId, byte[] payload) {
        bigoService.saveData(locationId, payload);
    }

    private void replicateData(String locationId, byte[][] shards) {
        for (String peer : peerNodeIps) {
            int shardIndexToBeSent = nodesIndex.get(peer);
            sendShardTo(peerConnections.get(peer), locationId, shards[shardIndexToBeSent]);
        }
    }


    private void connectToPeers() {
        for (String peerIp : peerNodeIps) {
            try {
                Socket socket = new Socket(peerIp, SOCKET_PORT);
                peerConnections.put(peerIp, socket);
                socket.getOutputStream().write((currentNodeIp +'\n').getBytes());//echo back my ip
                listenToSocket(socket);
            } catch (IOException e) {
                System.err.println("Could not connect to " + peerIp + ": " + e.getMessage());
            }
        }
    }

    private void listenToSocket(Socket socket) {
        new Thread(() -> {
            try {
                InputStream inputStream = socket.getInputStream();
                BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
                String line;

                while ((line = reader.readLine()) != null) { // Reads until newline
                    processReceivedMessage(line, socket);
                }
            } catch (IOException e) {
                System.err.println("Connection error: " + e.getMessage());
            }
        }).start();
    }

    private void sendShardTo(Socket socket, String locationId, byte[] shard) {
        if (socket != null && !socket.isClosed()) {
            try {
                OutputStream outputStream = socket.getOutputStream();
                String message = "STORE:" + locationId + ":" + Base64.getEncoder().encodeToString(shard) + "\n";
                outputStream.write(message.getBytes());
            } catch (IOException e) {
                System.err.println("Error when sending message to " + socket.getInetAddress().getHostAddress() + ": " + e.getMessage());
            }
        }
    }

    private void runSocketServer(String currentNodeIp) {
        new Thread(() -> {
            try (ServerSocket serverSocket = new ServerSocket(SOCKET_PORT, 50, InetAddress.getByName(currentNodeIp))) {
                while (true) {
                    Socket clientSocket = serverSocket.accept();
                    listenToSocket(clientSocket);
                }
            } catch (IOException e) {
                System.err.println("Socket server error: " + e.getMessage());
                e.printStackTrace();
            }
        }).start();
    }

    private void processReceivedMessage(String message, Socket socket) {
        if (message.startsWith("STORE:")) {
            // Split the message into components
            String[] parts = message.split(":", 3);
            if (parts.length >= 3) {
                String locationId = parts[1];
                String payloadStr = parts[2];
                try {
                    byte[] shard = Base64.getDecoder().decode(payloadStr);
                    saveIndividualShard(locationId, shard);
                } catch (Exception e) {
                    System.out.println("payload str _ " + payloadStr);
                    System.err.println(message);
                    System.err.println("Failed to parse payload: " + e.getMessage());
                }
            } else {
                System.err.println("Invalid message format: " + message);
            }
        } else if (message.startsWith("FETCH:")) {
            try {
                String[] parts = message.split(":", 3);
                String locationId = parts[1];
                String requestId = parts[2];
                byte[] shard = bigoService.getData(locationId);

                String response = "RESP:" + Base64.getEncoder().encodeToString(shard) + ":" + requestId+'\n';
                socket.getOutputStream().write(response.getBytes());
            } catch (IOException e) {
                System.err.println("Failed to handle fetch request");
            }
        } else if (message.startsWith("RESP:")) {
            String[] parts = message.split(":", 3);
            String shardStr = parts[1];
            String requestId = parts[2];
            requestedShardsResponse.put(requestId, Base64.getDecoder().decode(shardStr));
        } else {
            peerConnections.put(message, socket);
            System.out.println("Connection established: " + currentNodeIp +" - " + message);
        }

    }

    private void fetchShardFromNode(String nodeIp, String locationId, String requestId) {
        Socket socket = peerConnections.get(nodeIp);

        try {
            String request = "FETCH:" + locationId + ":" + requestId+'\n';
            socket.getOutputStream().write(request.getBytes());
        } catch (IOException e) {
            System.err.println("Fetch failed from " + nodeIp);
        }
    }

    public long getMapEntriesCount() {
        return bigoService.mapKeysCount();
    }
}