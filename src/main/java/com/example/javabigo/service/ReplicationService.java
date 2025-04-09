package com.example.javabigo.service;

import com.example.javabigo.Payload;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
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
    ObjectMapper mapper = new ObjectMapper();

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
                byte[][] shards = payloadCodec.encode(payload);
                bigoService.saveData(locationId, shards[nodesIndex.get(currentNodeIp)]);
                replicateData(locationId, shards);
            } catch (Exception e) {
                System.out.println("Error encoding data: " + e.getMessage());
            }
        }
    }


    public Payload getData(String locationId) {
        byte[][] shards = new byte[PayloadCodec.TOTAL_SHARDS][];

        shards[nodesIndex.get(currentNodeIp)] = bigoService.getData(locationId);
        // Collect shards from all nodes
        for (String peerIp : peerNodeIps) {
            System.out.println("fetching shard for locationid " + locationId + " and peer " + peerIp);
            String requestId = UUID.randomUUID().toString();
            fetchShardFromNode(peerIp, locationId, requestId);

            while (requestedShardsResponse.get(requestId) == null) {
            }
            System.out.println("while loop finished");
            shards[nodesIndex.get(peerIp)] = requestedShardsResponse.get(requestId);
            requestedShardsResponse.remove(requestId);
        }

        try {
            return payloadCodec.decode(shards);
        } catch (IOException e) {
            System.out.println("decode messed up");
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
                socket.getOutputStream().write(currentNodeIp.getBytes());//echo back my ip

                listenToSocket(socket, peerIp); // Start listener
            } catch (IOException e) {
                System.err.println("Could not connect to " + peerIp + ": " + e.getMessage());
            }
        }
    }

    private void listenToSocket(Socket socket, String peerIp) {
        new Thread(() -> {
            try {
                InputStream inputStream = socket.getInputStream();
                byte[] buffer = new byte[4096];
                int bytesRead;

                while ((bytesRead = inputStream.read(buffer)) != -1) {
                    String message = new String(buffer, 0, bytesRead);
                    System.out.println("message received for IP: " + currentNodeIp + " and mesages is " + message);
                    processReceivedMessage(message, socket);
                }
            } catch (IOException e) {
                System.err.println("Connection error with " + peerIp + ": " + e.getMessage());
                peerConnections.remove(peerIp);
            }
        }).start();
    }

    private void sendShardTo(Socket socket, String locationId, byte[] shard) {
        if (socket != null && !socket.isClosed()) {
            try {
                OutputStream outputStream = socket.getOutputStream();
                String message = "STORE:" + locationId + ":" + Base64.getEncoder().encodeToString(shard);

                outputStream.write(message.getBytes());
                outputStream.flush();

            } catch (IOException e) {
                System.err.println("Error when sending message to " + socket.getInetAddress().getHostAddress() + ": " + e.getMessage());
                try {
                    socket.close();
                } catch (IOException ex) {
                    // Ignore
                }
                // Remove disconnected peer
                peerConnections.values().remove(socket);
            }
        }
    }

    private void runSocketServer(String currentNodeIp) {
        new Thread(() -> {
            try (ServerSocket serverSocket = new ServerSocket(SOCKET_PORT, 50, InetAddress.getByName(currentNodeIp))) {
                while (true) {
                    Socket clientSocket = serverSocket.accept();
                    String declaredClientIp = getClientReportedIp(clientSocket.getInputStream());


                    peerConnections.put(declaredClientIp, clientSocket);
                    listenToSocket(clientSocket, declaredClientIp);
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

                String response = "RESP:" + Base64.getEncoder().encodeToString(shard) + ":" + requestId;
                socket.getOutputStream().write(response.getBytes());
            } catch (IOException e) {
                System.err.println("Failed to handle fetch request");
            }
        } else if (message.startsWith("RESP:")) {
            String[] parts = message.split(":", 3);
            String shardStr = parts[1];
            String requestId = parts[2];
            requestedShardsResponse.put(requestId, Base64.getDecoder().decode(shardStr));
        }

    }

    private String getClientReportedIp(InputStream inputStream) {
        try {
            byte[] buffer = new byte[4096];
            int bytesRead;

            while ((bytesRead = inputStream.read(buffer)) != -1) {
                return new String(buffer, 0, bytesRead);
            }
        } catch (IOException e) {
            System.err.println("Error reading client-reported IP: " + e.getMessage());
            return "unknown";
        }
        return "";
    }

    private void fetchShardFromNode(String nodeIp, String locationId, String requestId) {
        Socket socket = peerConnections.get(nodeIp);

        try {
            String request = "FETCH:" + locationId + ":" + requestId;
            socket.getOutputStream().write(request.getBytes());
            socket.getOutputStream().flush();
        } catch (IOException e) {
            System.err.println("Fetch failed from " + nodeIp);
        }
    }
}