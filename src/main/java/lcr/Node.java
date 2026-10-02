package lcr;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;

import java.util.concurrent.atomic.AtomicLong;

public class Node implements Runnable{
    private static final Logger log = LogManager.getLogger(Node.class);
    private static final AtomicLong SEQ = new AtomicLong();

    //The msg id is used to identify pairs of messages; if a send() and a recv() correspond
    //to the same message, they must have the same msg id. This var is used to infer the casual order.
    private static final AtomicLong MSG_ID = new AtomicLong();

    private final int myId;
    private final MailBoxQueue messagesQueue;
    private final Reporter reporter;

    private int maxId;
    private boolean isLeader;
    private Node successor;

    public Node(int nodeId, Reporter reporter) {
        this.messagesQueue = new MailBoxQueue();
        this.reporter = reporter;
        this.myId = nodeId;
        this.maxId = nodeId;
        this.isLeader = false;
    }

    public void setSuccessor(Node successor) {
        this.successor = successor;
    }

    void sendMessage(int id, Type msgType) {
        long msgId = MSG_ID.incrementAndGet();
        long seq = SEQ.incrementAndGet();
        log.debug("Send: to = {}, id = {}, msgId = {}, type = {}, seq = {}", successor.myId, id, msgId, msgType, seq);

        reporter.report(new Event(myId, Kind.SEND, successor.getMyId(), msgId, id, msgType, seq));
        successor.deliverMessage(new Message(myId, id, msgId, msgType));
    }

    void deliverMessage(Message message) {
        messagesQueue.put(message);
    }

    Message receiveMessage() throws InterruptedException {
        Message message = messagesQueue.take();
        long seq = SEQ.incrementAndGet();
        log.debug("Receive: from = {}, id = {}, msgId = {}, type = {}, seq = {}", message.getFrom(),
                message.getSenderId(), message.getMsgId(), message.getMsgType(), seq);
        reporter.report(new Event(myId, Kind.RECV, message.getFrom(), message.getMsgId(),
                message.getSenderId(), message.getMsgType(), seq));
        return message;
    }

    @Override
    public void run() {
        ThreadContext.put("nodeId", String.valueOf(myId));
        try {
            log.info("Node {} initiated, sending message", myId);
            sendMessage(myId, Type.ELECTION);
            while (true) {
                Message message = receiveMessage();
                int msgId = message.getSenderId();
                if (message.getMsgType() == Type.ELECTION) {
                    if (msgId == myId) {
                        isLeader = true;
                        long seq = SEQ.incrementAndGet();
                        log.debug("State: isLeader = true, seq = {}", seq);
                        reporter.report(new Event(myId, Kind.BECOME_LEADER, myId, -1, myId, Type.ELECTION, seq));
                        sendMessage(myId, Type.ELECTED);
                    } else if (msgId > maxId) {
                        log.debug("State: maxId {} -> {}", maxId, msgId);
                        maxId = msgId;
                        sendMessage(msgId, Type.ELECTION);
                    } else {
                        long seq = SEQ.incrementAndGet();
                        log.debug("Discard: from = {}, id = {}, msgId = {}, seq = {}",
                                message.getFrom(), msgId, message.getMsgId(), seq);
                        reporter.report(new Event(myId, Kind.DISCARD, message.getFrom(), message.getMsgId(),
                                msgId, message.getMsgType(), seq));
                    }
                } else {
                    if (msgId != myId) sendMessage(msgId, Type.ELECTED);
                    long seq = SEQ.incrementAndGet();
                    log.info("Leader elected: {}, isLeader = {}, seq = {}", msgId, isLeader, seq);
                    reporter.report(new Event(myId, Kind.LEADER, msgId, -1, msgId, Type.ELECTED, seq));
                    break;
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            ThreadContext.clearAll();
        }
    }

    public int getMyId() {
        return myId;
    }
}
