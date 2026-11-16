// Mock stream producer for the Account Activity Consumer exercise (see problem_statement.md).
// Hand this to the candidate as-is. See the comment on MockStreamProducer below for what it models.
//
// How to run (.NET 10+ SDK, no .csproj needed), from the repo root:
//   dotnet run starter/MockStreamProducer.cs                    # reads ./data, writes ./out
//   dotnet run starter/MockStreamProducer.cs -- path/to/data out # or point it elsewhere
// On .NET 8/9, create a console project (`dotnet new console`), copy this file in
// place of Program.cs, and `dotnet run -- path/to/data out`.
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;

var dataDir = args.Length > 0 ? args[0] : "data";
var outDir = args.Length > 1 ? args[1] : "out";

var producer = new MockStreamProducer(dataDir);
var received = 0;
var byTopic = new SortedDictionary<string, int>(StringComparer.Ordinal);

// TODO 1. Read:        call Poll() until it returns an empty list
// TODO 2. Duplicates:  the same event can arrive more than once; decide what makes two messages "the same"
// TODO 3. Order:       messages don't arrive in the order things happened
// TODO 4. Balances:    write out/account_balances.json  (see contracts/reports.schema.json)
// TODO 5. Statements:  write out/daily_statements.json  (see contracts/reports.schema.json)
while (true)
{
    var batch = producer.Poll(maxRecords: 10);
    if (batch.Count == 0) break;
    foreach (var message in batch)
    {
        received++;
        byTopic.TryGetValue(message.Topic, out var seen);
        byTopic[message.Topic] = seen + 1;
        if (message.Offset == 0)
            Console.WriteLine($"first message: topic={message.Topic} key={message.Key}\n  {message.Raw}\n");
        // TODO: handle each message
    }
}

Console.WriteLine("Summary:");
Console.WriteLine($"  messages received:  {received}");
foreach (var (topic, count) in byTopic)
    Console.WriteLine($"    {(topic + ":"),-17} {count}");
Console.WriteLine($"  reports written to: {Path.GetFullPath(outDir)} (TODO)");


/// <summary>
/// One delivered message. Offset: position in the delivery log, starting at 0.
/// Topic: "accounts" or "transactions". Key: the account_id the event is about.
/// Value: the event, parsed (use GetDecimal() for amounts of money). Raw: the event as the original JSON text.
/// </summary>
public record StreamMessage(long Offset, string Topic, string Key, JsonElement Value, string Raw);

/// <summary>
/// Plays back data/stream.jsonl as messages on two topics, "accounts" and "transactions",
/// in the order a real message system delivered them.
///
/// What it copies from a real stream: the same event can arrive more than once, and messages
/// don't arrive in the order things happened. What it does NOT do: remove duplicates, sort
/// messages, or tell you which ones matter. That's your program's job.
/// </summary>
public class MockStreamProducer
{
    public const string AccountsTopic = "accounts";
    public const string TransactionsTopic = "transactions";

    private readonly List<StreamMessage> _log;
    private int _position;

    public MockStreamProducer(string dataDir)
    {
        _log = File.ReadLines(Path.Combine(dataDir, "stream.jsonl"))
            .Where(line => line.Trim().Length > 0)
            .Select((line, offset) => ToMessage(offset, line))
            .ToList();
    }

    /// <summary>Return the next batch of up to maxRecords messages; empty once everything is delivered.</summary>
    public IReadOnlyList<StreamMessage> Poll(int maxRecords = 10)
    {
        if (maxRecords < 1) throw new ArgumentOutOfRangeException(nameof(maxRecords), "must be >= 1");
        var batch = _log.Skip(_position).Take(maxRecords).ToList();
        _position += batch.Count;
        return batch;
    }

    private static StreamMessage ToMessage(int offset, string line)
    {
        using var doc = JsonDocument.Parse(line);
        var value = doc.RootElement.Clone();
        var topic = value.GetProperty("event_type").GetString() == "ACCOUNT_OPENED" ? AccountsTopic : TransactionsTopic;
        var key = value.GetProperty("data").GetProperty("account_id").GetString() ?? "";
        return new StreamMessage(offset, topic, key, value, line);
    }
}
