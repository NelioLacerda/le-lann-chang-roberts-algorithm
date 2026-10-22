package lcr;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class LegalizerFirstAlg {

    private static final class Builder {
        final Map<Integer, Key> sendOf = new HashMap<>();
        final Map<Integer, Long> innerN = new HashMap<>(); // cada nó tem o seu innerN
        final List<Event> pi = new ArrayList<>();
        final Set<Key> innerPi = new HashSet<>();
        final Set<Event> p = new LinkedHashSet<>();

        Builder(List<Event> events) {
            for (Event e : events) {
                if (e.kind == Kind.SEND) {
                    sendOf.put(e.msgId, e.key());
                }
            }
        }

        boolean isNextOfItsNode(Event e) {
            return e.localSeq == innerN.getOrDefault(e.node, 0L);
        }

        boolean canAppend(Event e) {
            if (!isNextOfItsNode(e)) return false;
            return e.kind != Kind.RECV || innerPi.contains(sendOf.get(e.msgId));
        }

        void append(Event e) {
            pi.add(e);
            innerPi.add(e.key());
            innerN.put(e.node, e.localSeq + 1);
        }

        /**
         * while there is an event in rho that can be appended: append it and remove it from rho.
         */
        void release() {
            while (true) {
                Event found = null;
                for (Event e : p) {
                    if (canAppend(e)) {
                        found = e;
                        break;
                    }
                }
                if (found == null) return;
                p.remove(found);
                append(found);
            }
        }
    }

    private static List<Event> legalize(List<Event> events) {
        Builder builder = new Builder(events);

        for (Event e : events) {
            builder.release();

            if (e.kind == Kind.RECV) {
                builder.p.add(e);
            } else if (builder.isNextOfItsNode(e)) {
                builder.append(e);
            } else builder.p.add(e);
        }
        builder.release();

        return builder.pi;
    }

    private static Csv load(Path path) throws IOException {
        List<String> lines = Files.readAllLines(path);
        if (lines.isEmpty()) throw new IllegalArgumentException("empty file");
        Csv csv = new Csv();
        csv.header = lines.get(0);
        csv.columns = csv.header.split(",");

        for (int i = 0; i < csv.columns.length; i++)
            csv.col.put(csv.columns[i], i);

        for (String name : new String[]{"kind", "node", "msgId", "localSeq"}) {
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
        try {
            csv = load(inPath);
            legal = legalize(csv.events);
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
