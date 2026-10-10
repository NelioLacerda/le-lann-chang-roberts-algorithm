package lcr;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
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
        log.debug("Observer stored event: node = {}, kind = {}, peerNodeId = {}, nodeIdSent = {}, type = {}, msgId = {}, seq = {}, localSeq = {}",
                event.getNodeId(),
                event.getKind(),
                event.getPeerId(),
                event.getNodeIdToSend(),
                event.getMsgType(),
                event.getMsgId(),
                event.getTrueSeq(),
                event.getLocalSeq()
        );
    }

    // Colunas: pos,kind,node,peer,msgId,nodeIdToSend,type,trueSeq,localSeq
    public void writeCsv(Path path, List<Event> events) throws IOException {
        try (BufferedWriter w = Files.newBufferedWriter(path)) {
            w.write("pos,kind,node,peer,msgId,nodeIdToSend,type,trueSeq,localSeq\n");
            int pos = 0;
            for (Event e : events) {
                w.write(pos++ + "," + e.getKind() + "," + e.getNodeId() + "," + e.getPeerId() + ","
                        + e.getMsgId() + "," + e.getNodeIdToSend() + "," + e.getMsgType() + ","
                        + e.getTrueSeq() + "," + e.getLocalSeq() + "\n");
            }
        }
    }

    public List<Event> getTrace() {
        return trace;
    }
}