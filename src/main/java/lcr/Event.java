package lcr;

public class Event {
    private final int nodeId;
    private final Kind kind;
    private final int peer;
    private final long msgId;
    private final int id;
    private final Type msgType;
    private final long trueSeq;

    public Event(int nodeId, Kind kind, int peer, long msgId, int id, Type msgType, long trueSeq) {
        this.nodeId = nodeId;
        this.kind = kind;
        this.peer = peer;
        this.msgId = msgId;
        this.id = id;
        this.msgType = msgType;
        this.trueSeq = trueSeq;
    }

    public int getNodeId() {
        return nodeId;
    }

    public Kind getKind() {
        return kind;
    }

    public int getPeer() {
        return peer;
    }

    public long getMsgId() {
        return msgId;
    }

    public int getId() {
        return id;
    }

    public Type getMsgType() {
        return msgType;
    }

    public long getTrueSeq() {
        return trueSeq;
    }

    @Override
    public String toString() {
        return String.format("Event{node=%d, kind=%s, peer=%d, msgId=%d, id=%d, type=%s, trueSeq=%d}",
                nodeId, kind, peer, msgId, id, msgType, trueSeq);
    }
}
