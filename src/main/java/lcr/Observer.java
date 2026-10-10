package lcr;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

public final class Observer {
    private static final Logger log = LogManager.getLogger(Observer.class);

    private final List<Event> trace;
    private final AtomicBoolean firstClaimed = new AtomicBoolean(false);
    private final CountDownLatch firstObserved = new CountDownLatch(1);

    public Observer() {
        this.trace = new ArrayList<>();
        log.info("Observer created");
    }

    public boolean claimFirst() {
        return firstClaimed.compareAndSet(false, true);
    }

    public void putFirst(Event event) {
        put(event);
        firstObserved.countDown();
    }

    public void awaitFirst() throws InterruptedException {
        firstObserved.await();
    }

    public synchronized void put(Event event) {
        trace.add(event);
        log.debug("Observer stored event: node = {}, kind = {}, peerNodeId = {}, nodeIdSent = {}, type = {}, seq = {}",
                event.getNodeId(),
                event.getKind(),
                event.getPeerId(),
                event.getNodeIdToSend(),
                event.getMsgType(),
                event.getTrueSeq()
        );
    }

    public List<Event> getTrace() {
        return trace;
    }
}
