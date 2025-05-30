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
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

@Service
public class ReplicationService {

    private final ConcurrentHashMap<String, ReentrantLock> lockMap = new ConcurrentHashMap<>();
    private final List<String> peerNodeIps;
    private final Map<String, Integer> nodesIndex = new HashMap<>();
    private final String currentNodeIp;
    private final BigoService bigoService;
    private static final int SOCKET_PORT = 8089;
    private final Map<String, Socket> peerConnections = new ConcurrentHashMap<>();
    private final Map<String, byte[]> requestedShardsResponse = new ConcurrentHashMap<>();
    private final PayloadCodec payloadCodec = new PayloadCodec();

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
        ReentrantLock lock = lockMap.computeIfAbsent(locationId, id -> new ReentrantLock());
        lock.lock();
        try {
            shardAndReplicateData(locationId, payload);
        } catch (Exception e) {
            System.err.println("Error encoding data: " + e.getMessage());
        } finally {
            lock.unlock();
            lockMap.computeIfPresent(locationId, (id, l) -> l.hasQueuedThreads() ? l : null);
        }
    }

    private void shardAndReplicateData(String locationId, Payload payload) throws IOException {
        byte[][] shards = payloadCodec.encode(payload);
        bigoService.saveData(locationId, shards[nodesIndex.get(currentNodeIp)]);
        replicateData(locationId, shards);
    }


    public Payload getData(String locationId, boolean isWriting) {
        byte[][] shards = new byte[7][];
        byte[] currentShard = bigoService.getShardOf(locationId);
        if (currentShard == null) {
            System.out.println("Shard not saved in node");
            return null;
        }
        shards[nodesIndex.get(currentNodeIp)] = currentShard;

        Map<String, CompletableFuture<Void>> futures = new HashMap<>();

        for (String peerIp : peerNodeIps) {
            String requestId = UUID.randomUUID().toString();
            Socket peerSocket = peerConnections.get(peerIp);

            if (peerSocket == null) {
                shards[nodesIndex.get(peerIp)] = null;
                continue;
            }

            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                try {
                    fetchShardFromNode(peerSocket, locationId, requestId);
                    long startTime = System.currentTimeMillis();
                    long timeout = 500;

                    while (requestedShardsResponse.get(requestId) == null) {
                        if (!isWriting && peerConnections.size() != 7 && System.currentTimeMillis() - startTime > timeout) {
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
        for (String peerIp : peerNodeIps) {
            int shardIndexToBeSent = nodesIndex.get(peerIp);
            sendShardTo(peerIp, locationId, shards[shardIndexToBeSent]);
        }
    }


    private void connectToPeers() {
        for (String peerIp : peerNodeIps) {
            try {
                Socket socket = new Socket(peerIp, SOCKET_PORT);
                peerConnections.put(peerIp, socket);
                socket.getOutputStream().write((currentNodeIp + '\n').getBytes());//echo back my ip
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

    private void sendShardTo(String peer, String locationId, byte[] shard) {
        Socket socket = peerConnections.get(peer);
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
                while (peerConnections.size() < 6) {
                    Socket clientSocket = serverSocket.accept();
                    listenToSocket(clientSocket);
                }
            } catch (IOException e) {
                System.err.println("Socket server error: " + e.getMessage());
            }
        }).start();
    }

    private void processReceivedMessage(String message, Socket socket) {
        if (Objects.equals(message, "")) {
            System.out.println("Received empty message");
            return;
        }

        if (message.startsWith("STORE:")) {
            // Split the message into components
            String[] parts = message.split(":", 3);
            if (parts.length >= 3) {
                String locationId = parts[1];
                String payloadStr = parts[2];

                ReentrantLock lock = lockMap.computeIfAbsent(locationId, id -> new ReentrantLock());
                lock.lock();
                try {
                    byte[] shard = Base64.getDecoder().decode(payloadStr);
                    saveIndividualShard(locationId, shard);
                } catch (Exception e) {
                    System.out.println("payload str _ " + payloadStr);
                    System.err.println(message);
                    System.err.println("Failed to parse payload: " + e.getMessage());
                } finally {
                    lockMap.computeIfPresent(locationId, (id, l) -> l.hasQueuedThreads() ? l : null);
                }
            } else {
                System.err.println("Invalid message format: " + message);
            }
        } else if (message.startsWith("FETCH:")) {
            try {
                String[] parts = message.split(":", 3);
                String locationId = parts[1];
                String requestId = parts[2];
                byte[] shard = bigoService.getShardOf(locationId);

                String response = "RESP:" + Base64.getEncoder().encodeToString(shard) + ":" + requestId + '\n';
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
            System.out.println("Connection established: " + currentNodeIp + " - " + message);
        }
    }

    private void fetchShardFromNode(Socket socket, String locationId, String requestId) {

        try {
            String request = "FETCH:" + locationId + ":" + requestId + '\n';
            socket.getOutputStream().write(request.getBytes());
        } catch (IOException e) {
            System.err.println("Fetch failed");
        }
    }

    public long getMapEntriesCount() {
        return bigoService.mapKeysCount();
    }
}