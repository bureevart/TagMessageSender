using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using RabbitMQ.Client;
using TagValueUpdatedMessage;   // proto-класс

internal class Program
{
    // ---------------- CONFIG ----------------

    private const string ConfigFileName = "appsettings.json";

    private sealed class AppConfig
    {
        public string RabbitMqUri { get; set; } = "amqp://user:user@localhost:5672/";
        public string ExchangeName { get; set; } = "tag_value_updated_from_object_model";
        public string ExchangeType { get; set; } = "direct";
        public bool ExchangeDurable { get; set; } = true;
        public bool ExchangeAutoDelete { get; set; } = false;

        // routingKey общий для всех сообщений (как в твоём примере)
        public string RoutingKey { get; set; } = "127.0.0.1:4840";

        // откуда брать теги
        public string TagsApiUrl { get; set; } = "http://localhost:5137/api/Tag/GetAll";
        public string TagsJsonPath { get; set; } = "data.json";

        // режим непрерывной записи
        public int PeriodMs { get; set; } = 200;
        public int ContinuousFrom { get; set; } = 1;
        public int ContinuousTo { get; set; } = 100;

        // публикация пачкой через BasicPublishBatch (быстрее, чем по одному BasicPublish)
        public bool UsePublishBatch { get; set; } = true;
        public bool Persistent { get; set; } = true;
    }

    // модель для JSON (и из файла, и из API)
    private sealed class TagInfo
    {
        public Guid Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNameCaseInsensitive = true
    };

    // ---------------- STATE ----------------

    private static AppConfig _cfg = new();
    private static List<TagInfo> _tags = new();

    private static IConnection? _conn;
    private static IModel? _channel;
    private static IBasicProperties? _props;

    private static async Task<int> Main()
    {
        LoadConfig();

        Console.WriteLine("RabbitMQ Tag Publisher\n");

        while (true)
        {
            PrintStatus();
            PrintMenu();

            Console.Write("> ");
            var input = Console.ReadLine()?.Trim();

            if (string.IsNullOrEmpty(input))
                continue;

            switch (input)
            {
                case "1":
                    SetRabbitMqUri();
                    Reconnect();
                    SaveConfig();
                    break;

                case "2":
                    SetExchangeAndRouting();
                    Reconnect();
                    SaveConfig();
                    break;

                case "3":
                    await LoadTagsFromApi();
                    break;

                case "4":
                    LoadTagsFromJson();
                    break;

                case "5":
                    ShowTagsPreview();
                    break;

                case "6":
                    EnsureConnectedOrThrow();
                    await ModePushOnEnter();
                    break;

                case "7":
                    EnsureConnectedOrThrow();
                    await ModeContinuous();
                    break;

                case "8":
                    ToggleSpeedOptions();
                    SaveConfig();
                    break;

                case "9":
                    Cleanup();
                    return 0;

                default:
                    Console.WriteLine("Unknown command.");
                    break;
            }
        }
    }

    // ---------------- MENU ----------------

    private static void PrintStatus()
    {
        Console.WriteLine("---- Current ----");
        Console.WriteLine($"RabbitMQ:  {_cfg.RabbitMqUri}");
        Console.WriteLine($"Exchange:  {_cfg.ExchangeName} ({_cfg.ExchangeType}) durable={_cfg.ExchangeDurable} autoDelete={_cfg.ExchangeAutoDelete}");
        Console.WriteLine($"RoutingKey:{_cfg.RoutingKey}");
        Console.WriteLine($"Tags API:  {_cfg.TagsApiUrl}");
        Console.WriteLine($"Tags JSON: {_cfg.TagsJsonPath}");
        Console.WriteLine($"Tags loaded: {_tags.Count}");
        Console.WriteLine($"Continuous: {_cfg.ContinuousFrom}..{_cfg.ContinuousTo} every {_cfg.PeriodMs}ms");
        Console.WriteLine($"Speed: batch={_cfg.UsePublishBatch}, persistent={_cfg.Persistent}");
        Console.WriteLine("-----------------\n");
    }

    private static void PrintMenu()
    {
        Console.WriteLine("1) Set RabbitMQ URI");
        Console.WriteLine("2) Set Exchange / RoutingKey");
        Console.WriteLine("3) Load tags from API");
        Console.WriteLine("4) Load tags from JSON file");
        Console.WriteLine("5) Show tags preview");
        Console.WriteLine("6) Publish mode: push all tags on <Enter>");
        Console.WriteLine("7) Publish mode: continuous 1..100 with period (stop with Q)");
        Console.WriteLine("8) Speed options (batch/persistent)");
        Console.WriteLine("9) Exit\n");
    }

    private static void SetRabbitMqUri()
    {
        Console.Write("RabbitMQ URI: ");
        var uri = Console.ReadLine()?.Trim();
        if (!string.IsNullOrEmpty(uri))
            _cfg.RabbitMqUri = uri;
    }

    private static void SetExchangeAndRouting()
    {
        Console.Write("ExchangeName: ");
        var ex = Console.ReadLine()?.Trim();
        if (!string.IsNullOrEmpty(ex))
            _cfg.ExchangeName = ex;

        Console.Write("ExchangeType (direct/topic/fanout/headers): ");
        var t = Console.ReadLine()?.Trim();
        if (!string.IsNullOrEmpty(t))
            _cfg.ExchangeType = t;

        Console.Write("RoutingKey: ");
        var rk = Console.ReadLine()?.Trim();
        if (!string.IsNullOrEmpty(rk))
            _cfg.RoutingKey = rk;
    }

    private static void ToggleSpeedOptions()
    {
        _cfg.UsePublishBatch = !_cfg.UsePublishBatch;
        _cfg.Persistent = !_cfg.Persistent;
        Console.WriteLine($"Now: batch={_cfg.UsePublishBatch}, persistent={_cfg.Persistent}");
    }

    // ---------------- TAGS LOAD ----------------

    private static async Task LoadTagsFromApi()
    {
        Console.Write($"API url (empty = current): ");
        var url = Console.ReadLine()?.Trim();
        if (!string.IsNullOrEmpty(url))
        {
            _cfg.TagsApiUrl = url;
            SaveConfig();
        }

        try
        {
            using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(20) };
            var json = await http.GetStringAsync(_cfg.TagsApiUrl);

            var tags = JsonSerializer.Deserialize<List<TagInfo>>(json, JsonOptions) ?? new List<TagInfo>();
            _tags = tags.Where(t => t.Id != Guid.Empty).DistinctBy(t => t.Id).ToList();

            Console.WriteLine($"Loaded {_tags.Count} tags from API.");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Failed to load tags from API: {ex.Message}");
        }
    }

    private static void LoadTagsFromJson()
    {
        Console.Write($"JSON path (empty = current): ");
        var path = Console.ReadLine()?.Trim();
        if (!string.IsNullOrEmpty(path))
        {
            _cfg.TagsJsonPath = path;
            SaveConfig();
        }

        try
        {
            if (!File.Exists(_cfg.TagsJsonPath))
            {
                Console.WriteLine($"File not found: {_cfg.TagsJsonPath}");
                return;
            }

            var json = File.ReadAllText(_cfg.TagsJsonPath);
            var tags = JsonSerializer.Deserialize<List<TagInfo>>(json, JsonOptions) ?? new List<TagInfo>();
            _tags = tags.Where(t => t.Id != Guid.Empty).DistinctBy(t => t.Id).ToList();

            Console.WriteLine($"Loaded {_tags.Count} tags from JSON.");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Failed to load tags from JSON: {ex.Message}");
        }
    }

    private static void ShowTagsPreview()
    {
        if (_tags.Count == 0)
        {
            Console.WriteLine("No tags loaded.");
            return;
        }

        Console.WriteLine($"Showing first {Math.Min(10, _tags.Count)} tags:");
        foreach (var t in _tags.Take(10))
            Console.WriteLine($"- {t.Id}  {t.Name}");
    }

    // ---------------- PUBLISH MODES ----------------

    private static async Task ModePushOnEnter()
    {
        EnsureTagsLoadedOrWarn();

        Console.WriteLine("\nMode: Push on <Enter>");
        Console.WriteLine("Press <Enter> to publish all tags (Value increments each time).");
        Console.WriteLine("Empty line + Ctrl+Z/Ctrl+D is not needed; type 'q' then Enter to exit.\n");

        var counter = 0;

        while (true)
        {
            Console.Write("> ");
            var line = Console.ReadLine();
            if (line != null && line.Trim().Equals("q", StringComparison.OrdinalIgnoreCase))
                break;

            counter++;
            var value = counter.ToString();

            PublishAllTags(value);
            Console.WriteLine($"Published value={value} to {_tags.Count} tags.");
            await Task.Yield();
        }

        Console.WriteLine();
    }

    private static async Task ModeContinuous()
    {
        EnsureTagsLoadedOrWarn();

        Console.Write($"Period ms (empty = {_cfg.PeriodMs}): ");
        var periodStr = Console.ReadLine()?.Trim();
        if (int.TryParse(periodStr, out var p) && p > 0) _cfg.PeriodMs = p;

        Console.Write($"From (empty = {_cfg.ContinuousFrom}): ");
        var fromStr = Console.ReadLine()?.Trim();
        if (int.TryParse(fromStr, out var f)) _cfg.ContinuousFrom = f;

        Console.Write($"To (empty = {_cfg.ContinuousTo}): ");
        var toStr = Console.ReadLine()?.Trim();
        if (int.TryParse(toStr, out var t)) _cfg.ContinuousTo = t;

        SaveConfig();

        Console.WriteLine("\nMode: Continuous");
        Console.WriteLine($"Publishing values {_cfg.ContinuousFrom}..{_cfg.ContinuousTo} every {_cfg.PeriodMs}ms");
        Console.WriteLine("Press Q to stop.\n");

        using var cts = new CancellationTokenSource();

        // отдельная задачка: ждём нажатие Q
        var keyTask = Task.Run(() =>
        {
            while (!cts.IsCancellationRequested)
            {
                if (Console.KeyAvailable)
                {
                    var key = Console.ReadKey(intercept: true);
                    if (key.Key == ConsoleKey.Q)
                    {
                        cts.Cancel();
                        break;
                    }
                }
                Thread.Sleep(25);
            }
        });

        var current = _cfg.ContinuousFrom;

        try
        {
            while (!cts.IsCancellationRequested)
            {
                PublishAllTags(current.ToString());

                current++;
                if (current > _cfg.ContinuousTo)
                    current = _cfg.ContinuousFrom;

                await Task.Delay(_cfg.PeriodMs, cts.Token);
            }
        }
        catch (OperationCanceledException)
        {
            // ok
        }

        await keyTask;
        Console.WriteLine("\nStopped.\n");
    }

    private static void PublishAllTags(string value)
    {
        EnsureConnectedOrThrow();
        if (_tags.Count == 0) return;

        // ВАЖНО: TagId — константа для каждого тега, Value меняется.
        // Для ускорения: публикуем пачкой через BasicPublishBatch (меньше накладных расходов на вызовы).
        if (_cfg.UsePublishBatch)
        {
            var batch = _channel!.CreateBasicPublishBatch();

            foreach (var tag in _tags)
            {
                var msg = new TagValueUpdatedMessageFromObjectModel
                {
                    TagId = tag.Id.ToString(),
                    Value = value
                };

                batch.Add(
                    exchange: _cfg.ExchangeName,
                    routingKey: _cfg.RoutingKey,
                    mandatory: false,
                    properties: _props!,
                    body: msg.ToByteArray());
            }

            batch.Publish();
        }
        else
        {
            foreach (var tag in _tags)
            {
                var msg = new TagValueUpdatedMessageFromObjectModel
                {
                    TagId = tag.Id.ToString(),
                    Value = value
                };

                _channel!.BasicPublish(
                    exchange: _cfg.ExchangeName,
                    routingKey: _cfg.RoutingKey,
                    basicProperties: _props!,
                    body: msg.ToByteArray());
            }
        }
    }

    // ---------------- RABBIT CONNECTION ----------------

    private static void Reconnect()
    {
        CleanupRabbit();

        try
        {
            var factory = new ConnectionFactory
            {
                Uri = new Uri(_cfg.RabbitMqUri),
                DispatchConsumersAsync = false
            };

            _conn = factory.CreateConnection();
            _channel = _conn.CreateModel();

            _channel.ExchangeDeclare(
                exchange: _cfg.ExchangeName,
                type: _cfg.ExchangeType,
                durable: _cfg.ExchangeDurable,
                autoDelete: _cfg.ExchangeAutoDelete);

            _props = _channel.CreateBasicProperties();
            if (_cfg.Persistent)
                _props.DeliveryMode = 2; // persistent
            else
                _props.DeliveryMode = 1; // transient

            Console.WriteLine("Connected to RabbitMQ.\n");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"RabbitMQ connect failed: {ex.Message}\n");
        }
    }

    private static void EnsureConnectedOrThrow()
    {
        if (_conn == null || !_conn.IsOpen || _channel == null || !_channel.IsOpen)
        {
            Console.WriteLine("Not connected. Trying reconnect...");
            Reconnect();
        }

        if (_conn == null || !_conn.IsOpen || _channel == null || !_channel.IsOpen || _props == null)
            throw new InvalidOperationException("RabbitMQ connection/channel is not available.");
    }

    private static void EnsureTagsLoadedOrWarn()
    {
        if (_tags.Count == 0)
            Console.WriteLine("WARNING: Tags list is empty. Load tags first (API or JSON).");
    }

    private static void CleanupRabbit()
    {
        try { _channel?.Close(); } catch { }
        try { _channel?.Dispose(); } catch { }
        _channel = null;

        try { _conn?.Close(); } catch { }
        try { _conn?.Dispose(); } catch { }
        _conn = null;

        _props = null;
    }

    private static void Cleanup()
    {
        CleanupRabbit();
        SaveConfig();
    }

    // ---------------- CONFIG FILE ----------------

    private static void LoadConfig()
    {
        try
        {
            if (!File.Exists(ConfigFileName))
            {
                _cfg = new AppConfig();
                Reconnect();
                return;
            }

            var json = File.ReadAllText(ConfigFileName);
            _cfg = JsonSerializer.Deserialize<AppConfig>(json, JsonOptions) ?? new AppConfig();
        }
        catch
        {
            _cfg = new AppConfig();
        }

        Reconnect();
    }

    private static void SaveConfig()
    {
        try
        {
            var json = JsonSerializer.Serialize(_cfg, new JsonSerializerOptions { WriteIndented = true });
            File.WriteAllText(ConfigFileName, json);
        }
        catch
        {
            // ignore
        }
    }
}

// маленький polyfill если ты на старом TF без DistinctBy
internal static class LinqCompat
{
    public static IEnumerable<TSource> DistinctBy<TSource, TKey>(
        this IEnumerable<TSource> source,
        Func<TSource, TKey> keySelector)
    {
        var set = new HashSet<TKey>();
        foreach (var item in source)
        {
            if (set.Add(keySelector(item)))
                yield return item;
        }
    }
}
