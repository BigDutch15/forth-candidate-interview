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
 *   const producer = new MockStreamProducer(dataDir);
 *   const batch = producer.poll(10);   // Message[]; [] once every message has been delivered
 *   message.offset / .topic / .key / .value (parsed object) / .raw (the JSON text)
 *
 * How to run (Node 22.18+ or 23.6+ runs TypeScript directly; no dependencies), from the repo root:
 *   node starter/mockStreamProducer.ts                  # reads ./data, writes ./out
 *   node starter/mockStreamProducer.ts path/to/data out # or point it elsewhere
 * On older Node versions use `npx tsx starter/mockStreamProducer.ts` instead.
 */
import * as fs from "node:fs";
import * as path from "node:path";

export const ACCOUNTS_TOPIC = "accounts";
export const TRANSACTIONS_TOPIC = "transactions";

export interface Message {
  readonly offset: number; // position in the delivery log, starting at 0
  readonly topic: string; // "accounts" or "transactions"
  readonly key: string; // the account_id the event is about
  readonly value: Record<string, any>; // the event, parsed with JSON.parse (numbers become JS numbers)
  readonly raw: string; // the event as the original JSON text, if you'd rather parse it yourself
}

export class MockStreamProducer {
  private readonly log: Message[];
  private position = 0;

  constructor(dataDir: string) {
    const lines = fs
      .readFileSync(path.join(dataDir, "stream.jsonl"), "utf8")
      .split(/\r?\n/)
      .filter((line) => line.trim() !== "");
    this.log = lines.map((line, offset) => MockStreamProducer.toMessage(offset, line));
  }

  /** Return the next batch of up to maxRecords messages; [] once everything is delivered. */
  poll(maxRecords = 10): Message[] {
    if (maxRecords < 1) throw new RangeError("maxRecords must be >= 1");
    const batch = this.log.slice(this.position, this.position + maxRecords);
    this.position += batch.length;
    return batch;
  }

  private static toMessage(offset: number, line: string): Message {
    const value = JSON.parse(line);
    const topic = value.event_type === "ACCOUNT_OPENED" ? ACCOUNTS_TOPIC : TRANSACTIONS_TOPIC;
    return { offset, topic, key: value.data.account_id, value, raw: line };
  }
}

function main(argv: string[]): number {
  const repoRoot = path.join(path.dirname(path.resolve(argv[1])), "..");
  const dataDir = argv[2] ?? path.join(repoRoot, "data");
  const outDir = argv[3] ?? path.join(repoRoot, "out");

  const producer = new MockStreamProducer(dataDir);
  let received = 0;
  const byTopic = new Map<string, number>();

  // TODO 1. Read:        call poll() until it returns an empty list
  // TODO 2. Duplicates:  the same event can arrive more than once; decide what makes two messages "the same"
  // TODO 3. Order:       messages don't arrive in the order things happened
  // TODO 4. Balances:    write out/account_balances.json  (see contracts/reports.schema.json)
  // TODO 5. Statements:  write out/daily_statements.json  (see contracts/reports.schema.json)
  for (;;) {
    const batch = producer.poll(10);
    if (batch.length === 0) break;
    for (const message of batch) {
      received++;
      byTopic.set(message.topic, (byTopic.get(message.topic) ?? 0) + 1);
      if (message.offset === 0) {
        console.log(`first message: topic=${message.topic} key=${message.key}\n  ${message.raw}\n`);
      }
      // TODO: handle each message
    }
  }

  console.log("Summary:");
  console.log(`  messages received:  ${received}`);
  for (const [topic, count] of [...byTopic.entries()].sort()) {
    console.log(`    ${(topic + ":").padEnd(17)} ${count}`);
  }
  console.log(`  reports written to: ${outDir} (TODO)`);
  return 0;
}

// Run only when executed directly, not when imported.
if (process.argv[1] && path.basename(process.argv[1]).startsWith("mockStreamProducer")) {
  process.exit(main(process.argv));
}
