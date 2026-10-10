package lcr;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;

import java.util.concurrent.atomic.AtomicLong;

public class Node implements Runnable {
    private static final Logger log = LogManager.getLogger(Node.class);
    private static final AtomicLong SEQ = new AtomicLong();
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

    void sendMessage(int nodeIdToSend, Type msgType) {
        long seq = SEQ.getAndIncrement();
        long msgId = MSG_ID.getAndIncrement();
        log.debug("Send(msgId = {}): to = {}, nodeIdSend = {}, type = {}, seq = {}", msgId, successor.getMyId(), nodeIdToSend, msgType, seq);

        reporter.report(new Event(myId, Kind.SEND, successor.getMyId(), nodeIdToSend, msgType, seq, msgId));
        successor.deliverMessage(new Message(myId, nodeIdToSend, msgType, msgId));
    }

    void deliverMessage(Message message) {
        messagesQueue.put(message);
    }

    Message receiveMessage() throws InterruptedException {
        Message message = messagesQueue.take();
        long seq = SEQ.getAndIncrement();
        log.debug("Receive: from = {}, nodeIdToSend = {}, type = {}, seq = {}", message.getFromId(),
                message.getNodeIdToSend(), message.getMsgType(), seq);
        reporter.report(new Event(myId, Kind.RECV, message.getFromId(), message.getNodeIdToSend(),
                message.getMsgType(), seq));
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
                int nodeId = message.getNodeIdToSend();
                if (message.getMsgType() == Type.ELECTION) {
                    if (nodeId == myId) {
                        isLeader = true;
                        long seq = SEQ.getAndIncrement();
                        log.debug("State: isLeader = true, seq = {}", seq);
                        reporter.report(new Event(myId, Kind.REQUEST_TO_BECOME_LEADER, myId
                                , myId, Type.ELECTION, seq));
                        sendMessage(myId, Type.ELECTED);
                    } else if (nodeId > maxId) {
                        long seq = SEQ.getAndIncrement();
                        log.debug("State: maxId {} -> {}, seq = {}", maxId, nodeId, seq);
                        maxId = nodeId;
                        sendMessage(nodeId, Type.ELECTION);
                    } else {
                        long seq = SEQ.getAndIncrement();
                        log.debug("Discard: from = {}, nodeIdSent = {}, seq = {}",
                                message.getFromId(), nodeId, seq);
                        reporter.report(new Event(myId, Kind.DISCARD, message.getFromId(),
                                nodeId, message.getMsgType(), seq));
                    }
                } else {
                    if (nodeId != myId) sendMessage(nodeId, Type.ELECTED);
                    long seq = SEQ.getAndIncrement();
                    log.info("Leader elected node: {}, isLeader = {}, seq = {}", nodeId, isLeader, seq);
                    reporter.report(new Event(myId, Kind.LEADER, nodeId, nodeId, Type.ELECTED, seq));
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
