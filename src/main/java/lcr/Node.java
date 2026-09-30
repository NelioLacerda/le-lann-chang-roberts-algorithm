package lcr;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;

public class Node implements Runnable{
    private static final Logger log = LogManager.getLogger(Node.class);

    private final int myId;

    private final MailBoxQueue messagesQueue;
    private long clock;

    private int maxId;
    private boolean isLeader;
    private Node successor;

    public Node(int nodeId) {
        this.messagesQueue = new MailBoxQueue();
        this.clock = 0;
        this.myId = nodeId;
        this.maxId = nodeId;
        this.isLeader = false;
    }

    public void setSuccessor(Node successor) {
        this.successor = successor;
    }

    void sendMessage(int id, Type msgType) {
        clock++;
        log.debug("Send: to = {}, id = {}, clock = {}, type = {}", successor.myId, id, clock, msgType);
        successor.deliverMessage(new Message(myId, id, clock, msgType));
    }

    void deliverMessage(Message message) {
        messagesQueue.put(message);
    }

    Message receiveMessage() throws InterruptedException {
        Message message = messagesQueue.take();
        clock = Math.max(clock, message.clock()) + 1;
        log.debug("Receive: from = {}, id = {}, clock = {}, type = {}", message.from(), message.id(), clock, message.msgType());
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
                int msgId = message.id();
                if (message.msgType() == Type.ELECTION) {
                    if (msgId == myId) {
                        isLeader = true;
                        log.debug("State: isLeader = true, clock = {}", clock);
                        sendMessage(myId, Type.ELECTED);
                    } else if (msgId > maxId) {
                        log.debug("State: maxId {} -> {}, clock = {}", maxId, msgId, clock);
                        maxId = msgId;
                        sendMessage(msgId, Type.ELECTION);
                    } else
                        log.debug("Discard: from = {}, id = {}, clock = {}", message.from(), msgId, clock);
                } else {
                    if (msgId != myId) sendMessage(msgId, Type.ELECTED);
                    log.info("Leader elected: {}, clock = {}, isLeader = {}", msgId, clock, isLeader);
                    break;
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            ThreadContext.clearAll();
        }
    }
}
