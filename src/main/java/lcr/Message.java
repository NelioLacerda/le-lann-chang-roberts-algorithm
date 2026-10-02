package lcr;

public class Message {
    private final int from;
    private final int senderId;
    private final long msgId;
    private final Type msgType;

    public Message(int from, int senderId, long msgId, Type msgType) {
        this.from = from;
        this.senderId = senderId;
        this.msgId = msgId;
        this.msgType = msgType;
    }

    public int getFrom() {
        return from;
    }

    public int getSenderId() {
        return senderId;
    }

    public long getMsgId() {
        return msgId;
    }

    public Type getMsgType() {
        return msgType;
    }
}
