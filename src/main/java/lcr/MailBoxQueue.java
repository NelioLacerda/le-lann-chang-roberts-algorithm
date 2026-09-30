package lcr;

import java.util.ArrayDeque;

public final class MailBoxQueue {
    private final ArrayDeque<Message> queue = new ArrayDeque<>();

    public synchronized void put(Message msg) {
        queue.addLast(msg);
        notifyAll();
    }

    public synchronized Message take() throws InterruptedException {
        while (queue.isEmpty())
            wait();
        return queue.removeFirst();
    }
}