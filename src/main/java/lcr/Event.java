package lcr;

public class Event {
    private final int nodeId;
    private final Kind kind;
    private final int peerId;
    private final int nodeIdToSend;
    private final Type msgType;
    private final long trueSeq;

    //Event from $peerId$ to $nodeId$ pr vice-versa
    public Event(int nodeId, Kind kind, int peerId, int nodeIdToSend, Type msgType, long trueSeq) {
        this.nodeId = nodeId;
        this.kind = kind;
        this.peerId = peerId;
        this.nodeIdToSend = nodeIdToSend;
        this.msgType = msgType;
        this.trueSeq = trueSeq;
    }

    public int getNodeId() {
        return nodeId;
    }

    public Kind getKind() {
        return kind;
    }

    public int getPeerId() {
        return peerId;
    }

    public int getNodeIdToSend() {
        return nodeIdToSend;
    }

    public Type getMsgType() {
        return msgType;
    }

    public long getTrueSeq() {
        return trueSeq;
    }

    @Override
    public String toString() {
        return String.format("Event{node=%d, kind=%s, peerId=%d, nodeIdToSend=%d, type=%s, trueSeq=%d}",
                nodeId, kind, peerId, nodeIdToSend, msgType, trueSeq);
    }
}
