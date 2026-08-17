import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

public class Benchmark {
    private static final int CONCURRENT_THREADS = 8;
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

        // Store per-thread latencies and responses without lock contention during timing
        List<List<Long>> threadLatencies = new CopyOnWriteArrayList<>();
        List<List<String>> threadResponses = new CopyOnWriteArrayList<>();

        System.out.printf("Connecting %d threads (%d requests each, %d total)...%n",
                CONCURRENT_THREADS, reqsPerThread, actualTotalRequests);

        long startTime = System.nanoTime();

        for (int t = 0; t < CONCURRENT_THREADS; t++) {
            final int threadId = t;
            executor.submit(() -> {
                List<Long> localLatencies = new ArrayList<>(reqsPerThread);
                List<String> localResponses = new ArrayList<>(reqsPerThread);

                try (Socket s = new Socket(HOST, PORT);
                     PrintWriter out = new PrintWriter(s.getOutputStream(), true);
                     BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream()))) {

                    for (int i = 0; i < reqsPerThread; i++) {
                        int globalIndex = threadId * reqsPerThread + i;
                        int keyNum = globalIndex % 11;
                        int valNum = globalIndex % 13;

                        String payload = (globalIndex % 20 < 19)
                                ? "GET " + keyNum
                                : "PUT " + keyNum + " " + valNum;

                        long reqStart = System.nanoTime();
                        out.println(payload);
                        String response = in.readLine();
                        long reqEnd = System.nanoTime();

                        if (response == null) {
                            System.err.println("Thread " + threadId + " connection closed at request " + i);
                            break;
                        }

                        localLatencies.add(reqEnd - reqStart);
                        localResponses.add(response);
                    }
                } catch (IOException e) {
                    System.err.println("Thread " + threadId + " error: " + e.getMessage());
                } finally {
                    threadLatencies.add(localLatencies);
                    threadResponses.add(localResponses);
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

        // Aggregate latencies and tally unique response occurrences
        List<Long> allLatencies = new ArrayList<>(actualTotalRequests);
        Map<String, Integer> responseCounts = new HashMap<>();

        for (int i = 0; i < threadLatencies.size(); i++) {
            allLatencies.addAll(threadLatencies.get(i));
            for (String resp : threadResponses.get(i)) {
                responseCounts.put(resp, responseCounts.getOrDefault(resp, 0) + 1);
            }
        }
        Collections.sort(allLatencies);

        if (allLatencies.isEmpty()) {
            System.err.println("No requests completed successfully.");
            return;
        }

        int count = allLatencies.size();
        double totalSeconds = (endTime - startTime) / 1_000_000_000.0;
        double reqPerSec = count / totalSeconds;
        double avgLatencyMs = (allLatencies.stream().mapToLong(Long::longValue).average().orElse(0) / 1_000_000.0);

        long p50Ns = allLatencies.get((int) (count * 0.50));
        long p95Ns = allLatencies.get((int) (count * 0.95));
        long p99Ns = allLatencies.get((int) (count * 0.99));
        long p999Ns = allLatencies.get((int) (count * 0.999));

        System.out.println("\n--- Benchmark Results ---");
        System.out.printf("Total Requests: %d%n", count);
        System.out.printf("Connections:    %d%n", CONCURRENT_THREADS);
        System.out.printf("Total Time:     %.3f seconds%n", totalSeconds);
        System.out.printf("Throughput:     %.2f req/sec%n", reqPerSec);

        System.out.println("\n--- Latency Breakdown ---");
        System.out.printf("Avg Latency:    %.3f ms%n", avgLatencyMs);
        System.out.printf("p50 (Median):   %.3f ms%n", p50Ns / 1_000_000.0);
        System.out.printf("p95 Latency:    %.3f ms%n", p95Ns / 1_000_000.0);
        System.out.printf("p99 Latency:    %.3f ms%n", p99Ns / 1_000_000.0);
        System.out.printf("p999 Latency:    %.3f ms%n", p999Ns / 1_000_000.0);
        System.out.println(allLatencies.get(allLatencies.size()-1)/1_000_000.0);

       
    }
}