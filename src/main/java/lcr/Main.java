package lcr;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class Main {

    private static int[] makeUids(int n, String order, long seed) {
        List<Integer> ids = new ArrayList<>();
        for (int i = 1; i <= n; i++) ids.add(i);
        switch (order) {
            case "asc": break;
            case "desc": Collections.reverse(ids); break;
            case "random": Collections.shuffle(ids, new Random(seed)); break;
            default: throw new IllegalArgumentException("order must be asc|desc|random");
        }
        return ids.stream().mapToInt(Integer::intValue).toArray();
    }

    public static void main(String[] args) throws InterruptedException, IOException {
        long seed = args.length > 0 ? Long.parseLong(args[0]) : System.nanoTime();
        int maxDelayMs = args.length > 1 ? Integer.parseInt(args[1]) : 50;
        int nNodes = args.length > 2 ? Integer.parseInt(args[2]) : 9;
        String order = args.length > 3 ? args[3] : "random";
        Path outDir = Paths.get(args.length > 4 ? args[4] : "runs");
        boolean verbose = Boolean.getBoolean("verbose");

        int[] uids = makeUids(nNodes, order, seed);
        Observer observer = new Observer();
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(uids.length);

        Node[] nodes = new Node[uids.length];
        for (int i = 0; i < uids.length; i++) {
            Reporter reporter = new ReporterSend(observer, scheduler, seed + i, maxDelayMs);
            nodes[i] = new Node(uids[i], reporter);
        }
        for (int i = 0; i < uids.length; i++)
            nodes[i].setSuccessor(nodes[(i + 1) % uids.length]);

        Thread[] threads = new Thread[nodes.length];
        for (int i = 0; i < nodes.length; i++) {
            threads[i] = new Thread(nodes[i]);
            threads[i].start();
        }
        for (Thread t : threads) t.join();

        scheduler.shutdown();
        if (!scheduler.awaitTermination(30, TimeUnit.SECONDS)) {
            System.err.println("WARNING: observer trace incomplete (scheduler timed out)");
            System.exit(3);
        }

        List<Event> truth = observer.getTrace().stream()
                .sorted(Comparator.comparingLong(Event::getTrueSeq)).collect(Collectors.toList());
        if (verbose) {
            observer.getTrace().forEach(System.out::println);
            truth.forEach(System.out::println);
        }

        Files.createDirectories(outDir);
        String tag = "n" + nNodes + "-d" + maxDelayMs + "-" + order + "-s" + seed;
        observer.writeCsv(outDir.resolve("observed-" + tag + ".csv"), observer.getTrace());
        observer.writeCsv(outDir.resolve("truth-" + tag + ".csv"), truth);
    }
}