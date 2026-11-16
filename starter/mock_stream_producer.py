"""
Mock stream producer for the Account Activity Consumer exercise (see problem_statement.md).
Hand this to the candidate as-is.

It plays back data/stream.jsonl as messages on two topics, "accounts" and "transactions",
in the order a real message system delivered them. What it copies from a real stream: the
same event can arrive more than once, and messages don't arrive in the order things
happened. What it does NOT do: remove duplicates, sort messages, or tell you which ones
matter. That's your program's job.

API:
    producer = MockStreamProducer(data_dir)
    batch = producer.poll(max_records=10)   # list[Message]; [] once every message has been delivered
    message.offset / .topic / .key / .value (parsed dict) / .raw (the JSON text)

How to run (Python 3.9+, no dependencies), from the repo root:
    python3 starter/mock_stream_producer.py                  # reads ./data, writes ./out
    python3 starter/mock_stream_producer.py path/to/data out # or point it elsewhere
"""
from __future__ import annotations

import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

ACCOUNTS_TOPIC = "accounts"
TRANSACTIONS_TOPIC = "transactions"


@dataclass(frozen=True)
class Message:
    offset: int            # position in the delivery log, starting at 0
    topic: str             # "accounts" or "transactions"
    key: str               # the account_id the event is about
    value: dict[str, Any]  # the event, parsed with json.loads (numbers become floats)
    raw: str               # the event as the original JSON text, if you'd rather parse it yourself


class MockStreamProducer:
    def __init__(self, data_dir: str | Path) -> None:
        path = Path(data_dir) / "stream.jsonl"
        lines = [line for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
        self._log = [self._to_message(offset, line) for offset, line in enumerate(lines)]
        self._position = 0

    def poll(self, max_records: int = 10) -> list[Message]:
        """Return the next batch of up to max_records messages; [] once everything is delivered."""
        if max_records < 1:
            raise ValueError("max_records must be >= 1")
        batch = self._log[self._position:self._position + max_records]
        self._position += len(batch)
        return batch

    @staticmethod
    def _to_message(offset: int, line: str) -> Message:
        value = json.loads(line)
        topic = ACCOUNTS_TOPIC if value["event_type"] == "ACCOUNT_OPENED" else TRANSACTIONS_TOPIC
        return Message(offset, topic, value["data"]["account_id"], value, line)


def main(argv: list[str]) -> int:
    repo_root = Path(__file__).resolve().parent.parent
    data_dir = Path(argv[1]) if len(argv) > 1 else repo_root / "data"
    out_dir = Path(argv[2]) if len(argv) > 2 else repo_root / "out"

    producer = MockStreamProducer(data_dir)
    received = 0
    by_topic: dict[str, int] = {}

    # TODO 1. Read:        call poll() until it returns an empty list
    # TODO 2. Duplicates:  the same event can arrive more than once; decide what makes two messages "the same"
    # TODO 3. Order:       messages don't arrive in the order things happened
    # TODO 4. Balances:    write out/account_balances.json  (see contracts/reports.schema.json)
    # TODO 5. Statements:  write out/daily_statements.json  (see contracts/reports.schema.json)
    while True:
        batch = producer.poll(max_records=10)
        if not batch:
            break
        for message in batch:
            received += 1
            by_topic[message.topic] = by_topic.get(message.topic, 0) + 1
            if message.offset == 0:
                print(f"first message: topic={message.topic} key={message.key}\n  {message.raw}\n")
            # TODO: handle each message

    print("Summary:")
    print(f"  messages received:  {received}")
    for topic, count in sorted(by_topic.items()):
        print(f"    {topic + ':':<17} {count}")
    print(f"  reports written to: {out_dir} (TODO)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
