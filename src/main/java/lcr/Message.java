package lcr;

public record Message(int from, int id, long clock, Type msgType) {
}
