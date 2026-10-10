package lcr;

public class Message {
    private final int fromId;
    private final int nodeIdToSend;
    private final Type msgType;

    public Message(int fromId, int nodeIdToSend, Type msgType) {
        this.fromId = fromId;
        this.nodeIdToSend = nodeIdToSend;
        this.msgType = msgType;
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
}
