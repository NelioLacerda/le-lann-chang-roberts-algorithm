package lcr;

import java.util.Comparator;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Main {
    public static void main(String[] args) throws InterruptedException {
        long seed = args.length > 0 ? Long.parseLong(args[0]) : System.nanoTime();
        int maxDelayMs = args.length > 1 ? Integer.parseInt(args[1]) : 50;
        System.out.println("seed = " + seed + ", maxDelayMs = " + maxDelayMs);

        int[] uids = {3, 7};
        Observer observer = new Observer();
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(uids.length);

        Node[] nodes = new Node[uids.length];
        for (int i = 0; i < uids.length; i++) {
            Reporter reporter = new Reporter(observer, scheduler, seed + i, maxDelayMs);
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
        System.out.println("RECV before SEND: " + observer.recvBeforeSend());
        System.out.println("DISCARD before RECV: " + observer.discardBeforeRecv());
        System.out.println("ELECTED SEND before BECOME_LEADER: " + observer.electedSendBeforeLeader());

    }
}
