package lcr;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Main {
    public static void main(String[] args) throws InterruptedException, IOException {
        long seed = args.length > 0 ? Long.parseLong(args[0]) : System.nanoTime();
        int maxDelayMs = args.length > 1 ? Integer.parseInt(args[1]) : 50;
        System.out.println("seed = " + seed + ", maxDelayMs = " + maxDelayMs);

        int[] uids = {3, 7, 4, 9, 2, 1, 5, 8, 6};
        Observer observer = new Observer();
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(uids.length);

        Node[] nodes = new Node[uids.length];
        for (int i = 0; i < uids.length; i++) {
            Reporter reporter = new ReporterSend(observer, scheduler, seed + i, maxDelayMs);
            nodes[i] = new Node(uids[i], reporter);
        }

        for (int i = 0; i < uids.length; i++) {
            nodes[i].setSuccessor(nodes[(i + 1) % uids.length]);
        }

        Thread[] threads = new Thread[nodes.length];
        for (int i = 0; i < nodes.length; i++) {
            threads[i] = new Thread(nodes[i]);
            threads[i].start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        scheduler.shutdown();
        if (!scheduler.awaitTermination(10, TimeUnit.SECONDS))
            System.err.println("WARNING: observer trace incomplete (scheduler timed out)");

        System.out.println("--- trace ---");
        observer.getTrace().forEach(System.out::println);
        System.out.println("--- ground truth (por trueSeq) ---");
        observer.getTrace().stream().sorted(Comparator.comparingLong(Event::getTrueSeq)).forEach(System.out::println);

        // A seed vai no nome para os ficheiros não serem sobrescritos a cada execução
        // (a seed sozinha não torna a execução reprodutível, por causa do escalonamento das threads).
        Files.createDirectories(Paths.get("runs"));
        observer.writeCsv(Paths.get("runs/observed-" + seed + ".csv"), observer.getTrace());
        observer.writeCsv(Paths.get("runs/truth-" + seed + ".csv"),
                observer.getTrace().stream().sorted(Comparator.comparingLong(Event::getTrueSeq)).collect(java.util.stream.Collectors.toList()));
    }
}