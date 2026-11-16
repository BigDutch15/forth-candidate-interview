# Coding Interview: Account Activity Consumer

## Scenario

Another system at our company keeps track of customer bank accounts. Every time it opens an account
or records a transaction (money going into or out of an account), it sends an event to a message
stream. Other teams want two reports built from those events:

1. The current balance of every account.
2. A daily statement for every account: a day-by-day summary of what happened, like the statement a
   bank sends you, but one for each day.

Your job is to write the program that reads those events and produces the two reports. We give you a
small fake stream to read from, which we call the producer, in four languages.

The values inside the events are already correct, so there's nothing to check or clean. The tricky
part is how the events arrive:

- **The same event can arrive more than once.**
- **Events don't arrive in the order they happened.**

## What you're given

- `starter/`: the producer in Python, TypeScript, Java and C#. Pick whichever language you're most
  comfortable in. Each one is a small program you can run straight away, with TODOs marking where
  your code goes. The comment at the top of each file says how to run it.
- `data/stream.jsonl`: the messages the producer plays back. You're welcome to look at it, but read
  it through the producer, the same way you would read a real stream.
- `contracts/stream_events.schema.json`: the format of every event in the stream.
- `contracts/reports.schema.json`: the format the two reports must follow.

## How the stream works

```
producer = MockStreamProducer(data_dir)
batch = producer.poll(max_records=10)   # up to 10 messages; an empty list once there are none left
```

Each message has:

| Field | Meaning |
|---|---|
| `offset` | The message's position in the stream, starting at 0 |
| `topic` | `accounts` or `transactions` |
| `key` | The `account_id` the event is about |
| `value` | The event itself, already parsed from JSON |
| `raw` | The event as the original JSON text, if you'd rather parse it yourself |

Every event has the same five fields:

| Field | Meaning |
|---|---|
| `event_id` | An ID the sending system attaches to the event |
| `event_type` | `ACCOUNT_OPENED` or `TRANSACTION_RECORDED` |
| `occurred_at` | When it actually happened |
| `published_at` | When the sending system sent it, which can be much later than `occurred_at` |
| `data` | The account or the transaction |

The two kinds of event:

| `event_type` | Topic | What's in `data` |
|---|---|---|
| `ACCOUNT_OPENED` | `accounts` | The account, including its `starting_balance` |
| `TRANSACTION_RECORDED` | `transactions` | One transaction. `amount` is always positive; `type` says which way the money moved: `DEPOSIT` adds it to the balance, `WITHDRAWAL` subtracts it |

What to expect from the stream:

- **You'll receive some events more than once.** The stream sometimes delivers a message again, and
  the sending system resends events when it doesn't hear back in time. (Engineers call this
  "at-least-once" delivery.)
- **Messages arrive out of order**, both between the two topics and within each one. The order you
  receive messages in is not the order things happened in.
- **`occurred_at` and `published_at` are not the same.** Some events are sent long after they
  happened.
- **The stream ends.** For this exercise, `poll()` returns an empty list once everything has been
  delivered. A real stream would keep going forever, so be ready to talk about what would change.

## What you can assume

- Every event matches `contracts/stream_events.schema.json`. You don't need to check or clean any
  values.
- Every transaction really happened. Nothing is pending or cancelled, and everything is in US
  dollars.
- Every transaction belongs to an account that is opened somewhere in the stream.
- Accounts and transactions never change once they've been created.
- No balance ever goes below zero, as long as you apply each account's transactions in the order they
  happened.

## The reports

Write two JSON files that follow **`contracts/reports.schema.json`**:

| File | What it contains |
|---|---|
| `out/account_balances.json` | One row per account: its starting balance, total deposits, total withdrawals, number of transactions and current balance. Also a short summary of the messages you processed. |
| `out/daily_statements.json` | For each account, one statement per day (in UTC) on which it had at least one transaction: the balance at the start of the day, total deposits, total withdrawals, the balance at the end of the day, and that day's transactions, each showing the balance right after it. |

Read the contract before you start. Among other things, it says:

- Write money as a string with exactly two decimal places: `"1234.50"`, `"0.00"`.
- Put each transaction on the statement for the UTC date on which it happened.
- List a day's transactions in time order. If two have the same timestamp, put the lower
  `transaction_id` first.
- A statement's starting balance is the ending balance of that account's previous statement, or the
  account's `starting_balance` for its first statement.
- Sort accounts by `account_id`, and statements by `account_id` and then `date`.
- Running your program twice on the same stream must produce exactly the same files.

The contract describes what the reports must look like, not how to build them. You decide what
counts as a duplicate, what to do with a message you can't use yet, and how to keep the arithmetic
exact. Be ready to explain your choices. If you think the contract is wrong or unclear somewhere, say
so; spotting that counts in your favor.

Things to think about:

- **Duplicates.** What makes two messages "the same"? How would you notice if you'd counted something
  twice?
- **Order.** A statement needs events in the order they happened, but they don't arrive that way.
  What do you need to hold on to, and for how long?
- **Money.** How do you add up amounts like 0.10 and 0.20 without small rounding errors creeping in?
- **A stream that never ends.** If events kept arriving forever, when could you safely produce March
  3rd's statement? What would you keep in memory, and for how long?

## What we're looking for

1. **Communication.** Talk us through your thinking as you go, especially the trade-offs and anything
   you decide to leave for later. We're explicitly looking at how you explain your approach and the
   tools you use, not just at the code.
2. **Planning.** Before writing code, sketch how you'll break the problem into steps (for example:
   read, remove duplicates, sort, calculate, write the reports) and where your program keeps its
   data. Out loud, on the whiteboard or in comments are all fine.
3. **Edge cases.** The stream misbehaves on purpose. We're watching whether you notice problems as
   you go, work out what's causing each one, and decide how to handle it, rather than assuming the
   stream behaves.
4. **A working program.** By the end, we'd like to see your program run from start to finish and
   write both reports in the right format. It's fine if not every edge case is handled, or if some
   parts are left as a TODO with a one-line note on what you'd do with more time.

## Not required

- A real message system (Kafka, Kinesis and so on), Docker, or any other infrastructure.
- Saving anything between runs, or recovering from a crash. These are good things to discuss, but you
  don't need to build them.
- Checking or cleaning the data. It's already correct.
- A JSON Schema validation library. The contract files are there to read, though you're welcome to
  use a validator if your language has one handy.
