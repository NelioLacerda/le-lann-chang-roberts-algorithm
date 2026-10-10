package lcr;

public class Event {
    private final int nodeId;
    private final Kind kind;
    private final int peerId;
    private final int nodeIdToSend;
    private final Type msgType;
    private final long trueSeq;
    private final long msgId;
    private final long localSeq;

    // Event from $peerId$ to $nodeId$ or vice-versa.
    // trueSeq: contador global (só para ground truth, nenhum nó o conheceria).
    // localSeq: contador local ao nó que gerou o evento (ordem de programa do nó).
    public Event(int nodeId, Kind kind, int peerId, int nodeIdToSend, Type msgType,
                 long trueSeq, long msgId, long localSeq) {
        this.nodeId = nodeId;
        this.kind = kind;
        this.peerId = peerId;
        this.nodeIdToSend = nodeIdToSend;
        this.msgType = msgType;
        this.trueSeq = trueSeq;
        this.msgId = msgId;
        this.localSeq = localSeq;
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

    public long getMsgId() {
        return msgId;
    }

    public long getLocalSeq() {
        return localSeq;
    }

    @Override
    public String toString() {
        return String.format("Event{node=%d, kind=%s, peerId=%d, nodeIdToSend=%d, type=%s, msgId=%d, trueSeq=%d, localSeq=%d}",
                nodeId, kind, peerId, nodeIdToSend, msgType, msgId, trueSeq, localSeq);
    }
}