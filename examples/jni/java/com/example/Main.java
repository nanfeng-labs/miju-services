package com.example;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

public final class Main {
    public static void main(String[] args) throws Exception {
        Path directory = Files.createTempDirectory("miju-jni-udp-");
        Path addressFile = directory.resolve("address.txt");
        try {
            System.out.println(MijuBridge.nativeVersion());
            for (int generation = 0; generation < 2; generation++) {
                int start = MijuBridge.nativeStart(new String[]{"127.0.0.1:0", addressFile.toString()});
                if (start != 0) throw new AssertionError("start=" + start + ": " + MijuBridge.nativeLastError());
                String address = Files.readString(addressFile);
                int port = Integer.parseInt(address.substring(address.lastIndexOf(':') + 1));
                try (DatagramSocket socket = new DatagramSocket()) {
                    socket.setSoTimeout(5000);
                    byte[] message = "设备状态 / device-status 🍊".getBytes(StandardCharsets.UTF_8);
                    socket.send(new DatagramPacket(message, message.length, InetAddress.getLoopbackAddress(), port));
                    byte[] reply = new byte[1024];
                    DatagramPacket packet = new DatagramPacket(reply, reply.length);
                    socket.receive(packet);
                    if (!new String(reply, 0, packet.getLength(), StandardCharsets.UTF_8)
                            .equals(new String(message, StandardCharsets.UTF_8))) throw new AssertionError("echo mismatch");
                }
                int stop = MijuBridge.nativeStop(5000);
                if (stop != 0 || MijuBridge.nativeStatus() != 0 || Files.exists(addressFile)) {
                    throw new AssertionError("cleanup incomplete: stop=" + stop);
                }
                try (DatagramSocket rebound = new DatagramSocket(port, InetAddress.getLoopbackAddress())) {
                    System.out.println("generation " + generation + " stopped; UDP port " + rebound.getLocalPort() + " released");
                }
            }
        } finally {
            int stopped = MijuBridge.nativeStop(5000);
            if (stopped != 0 && stopped != 2) throw new AssertionError("stop before JVM exit=" + stopped);
            Files.deleteIfExists(addressFile);
            Files.deleteIfExists(directory);
        }
    }
}
