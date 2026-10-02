package lcr;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class Observer {
    private static final Logger log = LogManager.getLogger(Observer.class);

    private final List<Event> trace;

    public Observer() {
        this.trace = new ArrayList<>();
        log.info("Observer created");
    }

    public synchronized void put(Event event) {
        trace.add(event);
        log.debug("Observer stored event: node = {}, kind = {}, peer = {}, msgId = {}, id = {}, type = {}, seq = {}",
                event.getNodeId(),
                event.getKind(),
                event.getPeer(),
                event.getMsgId(),
                event.getId(),
                event.getMsgType(),
                event.getTrueSeq()
        );
    }

    public List<Event> getTrace() {
        return trace;
    }

    public int recvBeforeSend() {
        Map<Long, Integer> sendPos = new HashMap<>();
        for (int i = 0; i < trace.size(); i++) {
            Event e = trace.get(i);
            if (e.getKind() == Kind.SEND) sendPos.put(e.getMsgId(), i);
        }
        int bad = 0;
        for (int i = 0; i < trace.size(); i++) {
            Event e = trace.get(i);
            Integer s = sendPos.get(e.getMsgId());
            if (e.getKind() == Kind.RECV && (s == null || i < s)) {
                bad++;
                log.warn("Illegal: RECV of msgId = {} observed at #{} before its SEND at #{}",
                        e.getMsgId(), i + 1, sendPos.get(e.getMsgId()) + 1);
            }
        }
        return bad;
    }

    public int discardBeforeRecv() {
        Map<Long, Integer> recvPos = new HashMap<>();
        for (int i = 0; i < trace.size(); i++) {
            Event e = trace.get(i);
            if (e.getKind() == Kind.RECV) recvPos.put(e.getMsgId(), i);
        }
        int bad = 0;
        for (int i = 0; i < trace.size(); i++) {
            Event e = trace.get(i);
            if (e.getKind() == Kind.DISCARD) {
                Integer r = recvPos.get(e.getMsgId());
                if (r == null || i < r) {
                    bad++;
                    log.warn("Illegal: DISCARD of msgId = {} at node {} observed at #{} before its RECV at {}",
                            e.getMsgId(), e.getNodeId(), i + 1, r == null ? "(never observed)" : "#" + (r + 1));
                }
            }
        }
        return bad;
    }

    public int electedSendBeforeLeader() {
        Map<Integer, Integer> leaderPos = new HashMap<>();
        for (int i = 0; i < trace.size(); i++) {
            Event e = trace.get(i);
            if (e.getKind() == Kind.BECOME_LEADER) leaderPos.put(e.getNodeId(), i);
        }
        int bad = 0;
        for (int i = 0; i < trace.size(); i++) {
            Event e = trace.get(i);
            if (e.getKind() == Kind.SEND && e.getMsgType() == Type.ELECTED) {
                Integer p = leaderPos.get(e.getId());
                if (p == null || i < p) {
                    bad++;
                    log.warn("Illegal: SEND of ELECTED by node {} observed at #{} before BECOME_LEADER of {} at {}",
                            e.getNodeId(), i + 1, e.getId(), p == null ? "(never observed)" : "#" + (p + 1));
                }
            }
        }
        return bad;
    }
}
