import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Mock stream producer for the Account Activity Consumer exercise (see problem_statement.md).
 * Hand this to the candidate as-is.
 *
 * It plays back data/stream.jsonl as messages on two topics, "accounts" and "transactions",
 * in the order a real message system delivered them. What it copies from a real stream: the
 * same event can arrive more than once, and messages don't arrive in the order things
 * happened. What it does NOT do: remove duplicates, sort messages, or tell you which ones
 * matter. That's your program's job.
 *
 * API:
 *   MockStreamProducer producer = new MockStreamProducer(dataDir);
 *   List<Message> batch = producer.poll(10);   // empty once every message has been delivered
 *   message.offset() / .topic() / .key() / .value() (parsed Map) / .raw() (the JSON text)
 *
 * Java has no built-in JSON library, so this file includes a small one at the bottom
 * (Json.parse, Json.write, Json.writePretty). You're free to use it for your reports.
 *
 * How to run (JDK 17+, no build step), from the repo root:
 *   java starter/MockStreamProducer.java                  # reads ./data, writes ./out
 *   java starter/MockStreamProducer.java path/to/data out # or point it elsewhere
 */
public class MockStreamProducer {
    public static final String ACCOUNTS_TOPIC = "accounts";
    public static final String TRANSACTIONS_TOPIC = "transactions";

    /**
     * offset: position in the delivery log, starting at 0. topic: "accounts" or "transactions".
     * key: the account_id the event is about. value: the event, parsed by Json.parse
     * (objects -> Map, arrays -> List, numbers -> BigDecimal). raw: the event as the original JSON text.
     */
    public record Message(long offset, String topic, String key, Map<String, Object> value, String raw) {}

    private final List<Message> log = new ArrayList<>();
    private int position = 0;

    public MockStreamProducer(Path dataDir) throws IOException {
        long offset = 0;
        for (String line : Files.readAllLines(dataDir.resolve("stream.jsonl"), StandardCharsets.UTF_8)) {
            if (line.isBlank()) continue;
            log.add(toMessage(offset++, line));
        }
    }

    /** Return the next batch of up to maxRecords messages; empty once everything is delivered. */
    public List<Message> poll(int maxRecords) {
        if (maxRecords < 1) throw new IllegalArgumentException("maxRecords must be >= 1");
        int end = Math.min(log.size(), position + maxRecords);
        List<Message> batch = new ArrayList<>(log.subList(position, end));
        position = end;
        return batch;
    }

    public List<Message> poll() {
        return poll(10);
    }

    @SuppressWarnings("unchecked")
    private static Message toMessage(long offset, String line) {
        Map<String, Object> value = (Map<String, Object>) Json.parse(line);
        Map<String, Object> data = (Map<String, Object>) value.get("data");
        String topic = "ACCOUNT_OPENED".equals(value.get("event_type")) ? ACCOUNTS_TOPIC : TRANSACTIONS_TOPIC;
        return new Message(offset, topic, (String) data.get("account_id"), value, line);
    }

    public static void main(String[] args) throws IOException {
        Path dataDir = Path.of(args.length > 0 ? args[0] : "data");
        Path outDir = Path.of(args.length > 1 ? args[1] : "out");

        MockStreamProducer producer = new MockStreamProducer(dataDir);
        int received = 0;
        Map<String, Integer> byTopic = new TreeMap<>();

        // TODO 1. Read:        call poll() until it returns an empty list
        // TODO 2. Duplicates:  the same event can arrive more than once; decide what makes two messages "the same"
        // TODO 3. Order:       messages don't arrive in the order things happened
        // TODO 4. Balances:    write out/account_balances.json  (see contracts/reports.schema.json)
        // TODO 5. Statements:  write out/daily_statements.json  (see contracts/reports.schema.json)
        while (true) {
            List<Message> batch = producer.poll(10);
            if (batch.isEmpty()) break;
            for (Message message : batch) {
                received++;
                byTopic.merge(message.topic(), 1, Integer::sum);
                if (message.offset() == 0) {
                    System.out.println("first message: topic=" + message.topic() + " key=" + message.key()
                            + "\n  " + message.raw() + "\n");
                }
                // TODO: handle each message
            }
        }

        System.out.println("Summary:");
        System.out.println("  messages received:  " + received);
        for (Map.Entry<String, Integer> e : byTopic.entrySet()) {
            System.out.println("    " + String.format("%-17s", e.getKey() + ":") + " " + e.getValue());
        }
        System.out.println("  reports written to: " + outDir.toAbsolutePath().normalize() + " (TODO)");
    }

    /**
     * Minimal JSON reader/writer, provided because Java has no built-in one.
     * parse(): objects -> LinkedHashMap, arrays -> ArrayList, numbers -> BigDecimal (exact),
     * strings -> String, true/false -> Boolean, null -> null.
     * write()/writePretty(): the reverse, for Map, Iterable, String, Number, Boolean and null.
     * BigDecimal is written with toPlainString(), so new BigDecimal("0.10") stays "0.10".
     */
    public static final class Json {
        private final String s;
        private int i;

        private Json(String s) {
            this.s = s;
        }

        public static Object parse(String text) {
            Json p = new Json(text);
            p.ws();
            Object v = p.value();
            p.ws();
            if (p.i != p.s.length()) throw p.error("trailing characters");
            return v;
        }

        public static String write(Object value) {
            StringBuilder sb = new StringBuilder();
            emit(value, sb, -1, 0);
            return sb.toString();
        }

        public static String writePretty(Object value) {
            StringBuilder sb = new StringBuilder();
            emit(value, sb, 2, 0);
            return sb.append('\n').toString();
        }

        private Object value() {
            char c = peek();
            switch (c) {
                case '{': return object();
                case '[': return array();
                case '"': return string();
                case 't': return literal("true", Boolean.TRUE);
                case 'f': return literal("false", Boolean.FALSE);
                case 'n': return literal("null", null);
                default:
                    if (c == '-' || (c >= '0' && c <= '9')) return number();
                    throw error("unexpected character");
            }
        }

        private Map<String, Object> object() {
            Map<String, Object> m = new LinkedHashMap<>();
            i++;
            ws();
            if (peek() == '}') {
                i++;
                return m;
            }
            while (true) {
                ws();
                if (peek() != '"') throw error("expected a string key");
                String key = string();
                ws();
                expect(':');
                ws();
                m.put(key, value());
                ws();
                char c = next();
                if (c == '}') return m;
                if (c != ',') throw error("expected ',' or '}'");
            }
        }

        private List<Object> array() {
            List<Object> a = new ArrayList<>();
            i++;
            ws();
            if (peek() == ']') {
                i++;
                return a;
            }
            while (true) {
                ws();
                a.add(value());
                ws();
                char c = next();
                if (c == ']') return a;
                if (c != ',') throw error("expected ',' or ']'");
            }
        }

        private String string() {
            StringBuilder sb = new StringBuilder();
            i++; // opening quote
            while (true) {
                char c = next();
                if (c == '"') return sb.toString();
                if (c != '\\') {
                    sb.append(c);
                    continue;
                }
                char e = next();
                switch (e) {
                    case '"': sb.append('"'); break;
                    case '\\': sb.append('\\'); break;
                    case '/': sb.append('/'); break;
                    case 'b': sb.append('\b'); break;
                    case 'f': sb.append('\f'); break;
                    case 'n': sb.append('\n'); break;
                    case 'r': sb.append('\r'); break;
                    case 't': sb.append('\t'); break;
                    case 'u':
                        if (i + 4 > s.length()) throw error("bad unicode escape");
                        sb.append((char) Integer.parseInt(s.substring(i, i + 4), 16));
                        i += 4;
                        break;
                    default: throw error("bad escape");
                }
            }
        }

        private BigDecimal number() {
            int start = i;
            if (peek() == '-') i++;
            while (i < s.length() && "0123456789.eE+-".indexOf(s.charAt(i)) >= 0) i++;
            return new BigDecimal(s.substring(start, i));
        }

        private Object literal(String word, Object v) {
            if (!s.startsWith(word, i)) throw error("unexpected token");
            i += word.length();
            return v;
        }

        private void ws() {
            while (i < s.length() && Character.isWhitespace(s.charAt(i))) i++;
        }

        private char peek() {
            if (i >= s.length()) throw error("unexpected end of input");
            return s.charAt(i);
        }

        private char next() {
            char c = peek();
            i++;
            return c;
        }

        private void expect(char c) {
            if (next() != c) throw error("expected '" + c + "'");
        }

        private IllegalArgumentException error(String message) {
            return new IllegalArgumentException("JSON: " + message + " at index " + i);
        }

        private static void emit(Object v, StringBuilder sb, int indent, int depth) {
            if (v == null) {
                sb.append("null");
            } else if (v instanceof String str) {
                quote(str, sb);
            } else if (v instanceof BigDecimal bd) {
                sb.append(bd.toPlainString());
            } else if (v instanceof Number || v instanceof Boolean) {
                sb.append(v);
            } else if (v instanceof Map<?, ?> m) {
                sb.append('{');
                int n = 0;
                for (Map.Entry<?, ?> e : m.entrySet()) {
                    if (n++ > 0) sb.append(',');
                    newline(sb, indent, depth + 1);
                    quote(String.valueOf(e.getKey()), sb);
                    sb.append(indent < 0 ? ":" : ": ");
                    emit(e.getValue(), sb, indent, depth + 1);
                }
                if (n > 0) newline(sb, indent, depth);
                sb.append('}');
            } else if (v instanceof Iterable<?> items) {
                sb.append('[');
                int n = 0;
                for (Object item : items) {
                    if (n++ > 0) sb.append(',');
                    newline(sb, indent, depth + 1);
                    emit(item, sb, indent, depth + 1);
                }
                if (n > 0) newline(sb, indent, depth);
                sb.append(']');
            } else {
                throw new IllegalArgumentException("JSON: can't write " + v.getClass().getName());
            }
        }

        private static void newline(StringBuilder sb, int indent, int depth) {
            if (indent >= 0) sb.append('\n').append(" ".repeat(indent * depth));
        }

        private static void quote(String str, StringBuilder sb) {
            sb.append('"');
            for (int k = 0; k < str.length(); k++) {
                char c = str.charAt(k);
                switch (c) {
                    case '"': sb.append("\\\""); break;
                    case '\\': sb.append("\\\\"); break;
                    case '\n': sb.append("\\n"); break;
                    case '\r': sb.append("\\r"); break;
                    case '\t': sb.append("\\t"); break;
                    default:
                        if (c < 0x20) sb.append(String.format("\\u%04x", (int) c));
                        else sb.append(c);
                }
            }
            sb.append('"');
        }
    }
}
