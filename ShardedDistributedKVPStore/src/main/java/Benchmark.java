import java.io.*;
import java.net.*;
import java.util.concurrent.*;

public class Benchmark {
    private static final int CONCURRENT_THREADS = 5;
    private static final String HOST = "localhost";
    private static final int PORT = 8080;

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Usage: java Benchmark <total_number_of_requests>");
            return;
        }

        int totalRequests = Integer.parseInt(args[0]);
        int reqsPerThread = totalRequests / CONCURRENT_THREADS;
        int actualTotalRequests = reqsPerThread * CONCURRENT_THREADS;

        ExecutorService executor = Executors.newFixedThreadPool(CONCURRENT_THREADS);
        CountDownLatch latch = new CountDownLatch(CONCURRENT_THREADS);

        System.out.printf("Connecting %d threads (%d requests each, %d total)...%n",
                CONCURRENT_THREADS, reqsPerThread, actualTotalRequests);

        long startTime = System.nanoTime();

        for (int t = 0; t < CONCURRENT_THREADS; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try (Socket s = new Socket(HOST, PORT);
                     PrintWriter out = new PrintWriter(s.getOutputStream(), true);
                     BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream()))) {

                    for (int i = 0; i < reqsPerThread; i++) {
                        int globalIndex = threadId * reqsPerThread + i;
                        int keyNum = globalIndex % 11;
                        int valNum = globalIndex % 13;

                        String payload = (globalIndex % 20 < 19)
                                ? "GET key_" + keyNum
                                : "POST key_" + keyNum + " val_" + valNum;

                        out.println(payload);

                        String response = in.readLine();
                        if (response == null) {
                            System.err.println("Thread " + threadId + " connection closed at request " + i);
                            break;
                        }
                    }
                } catch (IOException e) {
                    System.err.println("Thread " + threadId + " error: " + e.getMessage());
                } finally {
                    latch.countDown();
                }
            });
        }

        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            executor.shutdown();
        }

        long endTime = System.nanoTime();

        double totalSeconds = (endTime - startTime) / 1_000_000_000.0;
        double reqPerSec = actualTotalRequests / totalSeconds;
        double avgLatencyMs = (totalSeconds * CONCURRENT_THREADS / actualTotalRequests) * 1000.0;

        System.out.println("\n--- Benchmark Results ---");
        System.out.printf("Total Requests: %d%n", actualTotalRequests);
        System.out.printf("Connections:    %d%n", CONCURRENT_THREADS);
        System.out.printf("Total Time:     %.3f seconds%n", totalSeconds);
        System.out.printf("Throughput:     %.2f req/sec%n", reqPerSec);
        System.out.printf("Avg Latency:    %.3f ms/req%n", avgLatencyMs);
    }
}