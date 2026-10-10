package lcr;

public class Message {
    private final int fromId;
    private final int nodeIdToSend;
    private final Type msgType;
    private final long msgId;

    public Message(int fromId, int nodeIdToSend, Type msgType, long msgId) {
        this.fromId = fromId;
        this.nodeIdToSend = nodeIdToSend;
        this.msgType = msgType;
        this.msgId = msgId;
    }

    public int getFromId() {
        return fromId;
    }

    public int getNodeIdToSend() {
        return nodeIdToSend;
    }

    public Type getMsgType() {
        return msgType;
    }

    public long getMsgId() {
        return msgId;
    }
}
