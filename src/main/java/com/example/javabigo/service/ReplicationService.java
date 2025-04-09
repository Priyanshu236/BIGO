package com.example.javabigo.service;

import com.example.javabigo.Payload;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Service
public class ReplicationService {
    private final List<String> peerNodeIps;
    private final String currentNodeIp;
    private final BigoService bigoService;
    private static final int SOCKET_PORT = 8089;
    private Map<String, Socket> peerConnections = new ConcurrentHashMap<>();
    ObjectMapper mapper = new ObjectMapper();

    @Autowired
    public ReplicationService(BigoService bigoService, @Value("${current.node.ip}") String currentNodeIp,
                              @Value("${all.node.ips}") String allNodeIps) {
        this.currentNodeIp = currentNodeIp;
        this.bigoService = bigoService;
        this.peerNodeIps = Arrays.stream(allNodeIps.split(","))
                .map(String::trim)
                .filter(ip -> !ip.equals(this.currentNodeIp))
                .collect(Collectors.toList());

        runSocketServer(currentNodeIp);
        connectToPeers();
    }

    public void saveData(String locationId, Payload payload, boolean shouldReplicateToOthers) {
        if (shouldReplicateToOthers) {
            Payload savedPayload = bigoService.saveData(locationId, payload);
            replicateData(locationId, savedPayload);
        } else {
            bigoService.saveDataToMap(locationId, payload);
        }
    }

    public void replicateData(String locationId, Payload payload) {
        for (Map.Entry<String, Socket> peer : peerConnections.entrySet()) {
            sendMessageTo(peer.getValue(), locationId, payload);
        }
    }

    public Payload getData(String locationId) {
        return bigoService.getData(locationId);
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

                    processReceivedMessage(message);
                }
            } catch (IOException e) {
                System.err.println("Connection error with " + peerIp + ": " + e.getMessage());
                peerConnections.remove(peerIp);
            }
        }).start();
    }

    public void sendMessageTo(Socket socket, String locationId, Payload payload) {
        if (socket != null && !socket.isClosed()) {
            try {
                OutputStream outputStream = socket.getOutputStream();
                String message = "DATA:" + locationId + ":" + mapper.writeValueAsString(payload);
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

    private void processReceivedMessage(String message) {
        if (message.startsWith("DATA:")) {
            // Split the message into components
            String[] parts = message.split(":", 3);
            if (parts.length >= 3) {
                String locationId = parts[1];
                String payloadStr = parts[2];
                try {
                    // Parse the payload string

                    Payload payload = mapper.readValue(payloadStr, Payload.class);

                    // Save the data (with false to avoid replication loop)
                    saveData(locationId, payload, false);

                } catch (Exception e) {
                    System.err.println("Failed to parse payload: " + e.getMessage());
                }
            } else {
                System.err.println("Invalid message format: " + message);
            }
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
}