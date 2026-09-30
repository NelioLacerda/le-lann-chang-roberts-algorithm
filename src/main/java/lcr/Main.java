package lcr;

public class Main {
    public static void main(String[] args) throws InterruptedException {
        int[] uids = {1, 4, 2, 5, 3};
        Node[] nodes = new Node[uids.length];
        for (int i = 0; i < uids.length; i++) {
            nodes[i] = new Node(uids[i]);
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
    }
}
