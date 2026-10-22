package lcr;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class LegalizerThirdAlg {

    private static final class Builder {
        final List<Event> pi = new ArrayList<>();
        final Set<Key> innerPi = new HashSet<>();
        final Set<Event> p = new LinkedHashSet<>();
        final Map<Integer, Long> innerN = new HashMap<>();
        final Map<Integer, List<Event>> eventsByNode = new HashMap<>();
        final Map<Integer, Event> sendOf = new HashMap<>();
        final Map<Key, Integer> obsIdx = new HashMap<>();

        final Map<Key, Long> dm = new HashMap<>();
        long dPi = 0;

        Builder(List<Event> events) {
            for (int i = 0; i < events.size(); i++) {
                Event e = events.get(i);
                obsIdx.put(e.key(), i);
                eventsByNode.computeIfAbsent(e.node, k -> new ArrayList<>()).add(e);
                if (e.kind == Kind.SEND) sendOf.put(e.msgId, e);
            }
            for (List<Event> of : eventsByNode.values())
                of.sort(Comparator.comparingLong(x -> x.localSeq));
        }

        /** localSeq of the first event of the node, from its next one on, that is not a SEND. */
        long turnLimit(int node) {
            List<Event> of = eventsByNode.get(node);
            long j = innerN.getOrDefault(node, 0L);
            while (j < of.size() && of.get((int) j).kind == Kind.SEND) j++;
            return j;
        }

        boolean atItsTurn(Event e) {
            return e.localSeq <= turnLimit(e.node);
        }

        boolean isNextOfItsNode(Event e) {
            return e.localSeq == innerN.getOrDefault(e.node, 0L);
        }

        boolean canRelease(Event e) {
            if (innerPi.contains(e.key())) return false;
            if (e.kind != Kind.RECV) return isNextOfItsNode(e);
            if (!atItsTurn(e)) return false;
            Event s = sendOf.get(e.msgId);
            return innerPi.contains(s.key()) || atItsTurn(s);
        }

        void append(Event e) {
            pi.add(e);
            innerPi.add(e.key());
            innerN.put(e.node, e.localSeq + 1);
        }

        /**
         * Event e goes to pi.
         * If it was already observed it waits in rho and is released (its move
         * right ends here);
         * else, it is registered in rho so that it is skipped when it shows up
         * in sigma, and dm remembers where it was inserted.
         */
        void moveLeft(Event e) {
            if (p.remove(e)) dPi += pi.size() - dm.get(e.key());
            else {
                p.add(e);
                dm.put(e.key(), (long) pi.size() + 1);
            }
            append(e);
        }

        List<Event> earlierSends(Event e) {
            List<Event> of = eventsByNode.get(e.node);
            List<Event> out = new ArrayList<>();
            for (long k = innerN.getOrDefault(e.node, 0L); k < e.localSeq; k++) out.add(of.get((int) k));
            return out;
        }

        void releaseOne(Event e) {
            if (e.kind == Kind.RECV) {
                List<Event> own = earlierSends(e);
                List<Event> msg = new ArrayList<>();
                Event s = sendOf.get(e.msgId);
                if (!innerPi.contains(s.key())) {
                    msg = earlierSends(s);
                    msg.add(s);
                }
                int i = 0, j = 0;
                while (i < own.size() || j < msg.size()) {
                    boolean takeOwn = j == msg.size()
                            || (i < own.size() && obsIdx.get(own.get(i).key()) < obsIdx.get(msg.get(j).key()));
                    moveLeft(takeOwn ? own.get(i++) : msg.get(j++));
                }
            }
            dPi += pi.size() - dm.get(e.key());
            p.remove(e);
            append(e);
        }

        void release() {
            while (true) {
                Event found = null;
                for (Event e : p) {
                    if (canRelease(e)) {
                        found = e;
                        break;
                    }
                }
                if (found == null) return;
                releaseOne(found);
            }
        }

        void settleMovedLeft(Event op) {
            dPi += pi.size() - dm.get(op.key());
            p.remove(op);
        }

        void waitInRho(Event op) {
            dm.put(op.key(), (long) pi.size());
            p.add(op);
        }
    }

    private static Result legalize(List<Event> events) {
        Builder builder = new Builder(events);

        for (Event op : events) {
            if (builder.p.contains(op)) {
                builder.settleMovedLeft(op);
                continue;
            }
            builder.release();
            if (builder.p.contains(op)) {
                builder.settleMovedLeft(op);
            } else if (op.kind == Kind.RECV) {
                builder.waitInRho(op);
            } else if (builder.isNextOfItsNode(op)) {
                builder.append(op);
            } else {
                builder.waitInRho(op);
            }
        }
        builder.release();

        return new Result(builder.pi, builder.dPi);
    }

    public static void main(String[] args) throws IOException {
        String in = null;
        String out = null;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-i":
                    in = args[++i];
                    break;
                case "-o":
                    out = args[++i];
                    break;
            }
        }

        Path inPath = Paths.get(in);
        Csv csv;
        List<Event> legal;
        long dPi;
        try {
            csv = load(inPath);
            Result result = legalize(csv.events);
            legal = result.events;
            dPi = result.dPi;
        } catch (IllegalArgumentException | IOException ex) {
            System.out.println("ERROR: " + ex.getMessage());
            System.exit(2);
            return;
        }

        Path outPath;
        if (out != null)
            outPath = Paths.get(out);
        else {
            String name = inPath.getFileName().toString();
            outPath = inPath.resolveSibling(name.contains("observed") ?
                    name.replaceFirst("observed", "legal") : "legal-" + name);
        }
        write(outPath, csv, legal);
        System.out.println("dPi = " + dPi);
    }

    private static Csv load(Path path) throws IOException {
        List<String> lines = Files.readAllLines(path);
        if (lines.isEmpty()) throw new IllegalArgumentException("empty file");
        Csv csv = new Csv();
        csv.header = lines.get(0);
        csv.columns = csv.header.split(",");

        for (int i = 0; i < csv.columns.length; i++)
            csv.col.put(csv.columns[i], i);

        for (String name : new String[] {"kind", "node", "msgId", "localSeq"}) {
            if (!csv.col.containsKey(name)) {
                throw new IllegalArgumentException("missing column '" + name + "' in the CSV header");
            }
        }

        for (int ln = 1; ln < lines.size(); ln++) {
            String line = lines.get(ln);
            if (line.isEmpty()) continue;
            String[] r = line.split(",", -1);
            Kind kind;
            try {
                kind = Kind.valueOf(r[csv.col.get("kind")]);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("line " + ln + ": invalid kind '" + r[csv.col.get("kind")] + "'");
            }
            csv.events.add(new Event(kind,
                    Integer.parseInt(r[csv.col.get("node")]),
                    Integer.parseInt(r[csv.col.get("msgId")]),
                    Long.parseLong(r[csv.col.get("localSeq")]),
                    r));
        }
        return csv;
    }

    private static void write(Path path, Csv csv, List<Event> events) throws IOException {
        Integer posCol = csv.col.get("pos");
        try (BufferedWriter w = Files.newBufferedWriter(path)) {
            w.write(csv.header);
            w.write("\n");
            for (int i = 0; i < events.size(); i++) {
                String[] c = events.get(i).row.clone();
                if (posCol != null) c[posCol] = String.valueOf(i);
                w.write(String.join(",", c));
                w.write("\n");
            }
        }
    }

    private static final class Csv {
        String header;
        String[] columns;
        Map<String, Integer> col = new HashMap<>();
        List<Event> events = new ArrayList<>();
    }

    private static final class Event {
        final Kind kind;
        final int node;
        final int msgId;
        final long localSeq;
        final String[] row;

        Event(Kind kind, int node, int msgId, long localSeq, String[] row) {
            this.kind = kind;
            this.node = node;
            this.msgId = msgId;
            this.localSeq = localSeq;
            this.row = row;
        }

        Key key() {
            return new Key(node, localSeq);
        }
    }

    private static final class Result {
        final List<Event> events;
        final long dPi;

        Result(List<Event> events, long dPi) {
            this.events = events;
            this.dPi = dPi;
        }
    }

    private static final class Key {
        final int node;
        final long localSeq;

        Key(int node, long localSeq) {
            this.node = node;
            this.localSeq = localSeq;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) return false;
            Key key = (Key) o;
            return node == key.node && localSeq == key.localSeq;
        }

        @Override
        public int hashCode() {
            return Objects.hash(node, localSeq);
        }
    }
}
