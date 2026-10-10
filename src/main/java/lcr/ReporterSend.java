package lcr;

import java.util.Random;
import java.util.concurrent.ScheduledExecutorService;

import static java.util.concurrent.TimeUnit.MILLISECONDS;


public class ReporterSend implements Reporter {
    private final Observer observer;
    private final ScheduledExecutorService scheduler;
    private final Random random;
    private final int maxDelayMsg;

    public ReporterSend(Observer observer, ScheduledExecutorService scheduler, long rndSeed, int maxDelayMsg) {
        this.observer = observer;
        this.scheduler = scheduler;
        this.random = new Random(rndSeed);
        this.maxDelayMsg = maxDelayMsg;
    }

    @Override
    public void report(Event event) {
        // O primeiro SEND observado é reportado de imediato e só uma vez.
        // (Antes, o claimFirst() também era chamado em drawDelay(), o que fazia
        // com que o primeiro SEND fosse inserido duas vezes no trace.)
        if (event.getKind() == Kind.SEND && observer.claimFirst()) {
            observer.putFirst(event);
            return;
        }
        long delayMs = drawDelay(event);
        scheduler.schedule(() -> {
            try {
                observer.awaitFirst();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            observer.put(event);
        }, delayMs, MILLISECONDS);
    }

    private long drawDelay(Event event) {
        long delayMs;
        switch (event.getKind()) {
            case SEND:
                delayMs = maxDelayMsg / 2 + random.nextInt(maxDelayMsg / 2 + 1);
                break;
            case REQUEST_TO_BECOME_LEADER:
                delayMs = maxDelayMsg + random.nextInt(maxDelayMsg + 1);
                break;
            case DISCARD:
                delayMs = random.nextInt(maxDelayMsg / 20 + 1);
                break;
            default:
                delayMs = maxDelayMsg / 10 + random.nextInt(maxDelayMsg / 10 + 1);
        }
        return delayMs;
    }
}