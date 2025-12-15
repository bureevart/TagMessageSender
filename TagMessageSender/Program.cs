using System;
using System.Collections.Generic;
using System.IO;
using Google.Protobuf;
using RabbitMQ.Client;
using TagValueUpdatedMessage;   // proto-класс
using System.Text.Json;

internal class Program
{
    private const string ExchangeName = "tag_value_updated_from_object_model";
    private const string ExchangeType = "direct";    // как у консьюмера

    // модель для десериализации JSON
    private class TagInfo
    {
        public Guid Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    private static void Main()
    {
        Console.Write("RabbitMQ connection string (amqp://user:pass@host:port/): ");
        var uri = Console.ReadLine();
        if (string.IsNullOrEmpty(uri))
        {
            uri = "amqp://user:user@localhost:5672/";
        }
        
        var options = new JsonSerializerOptions
        {
            PropertyNameCaseInsensitive = true
        };
        // загружаем JSON
        var json = File.ReadAllText("data.json");
        var tags = JsonSerializer.Deserialize<List<TagInfo>>(json, options)
                   ?? new List<TagInfo>();

        if (tags.Count == 0)
        {
            Console.WriteLine("Файл data.json пустой или не удалось распарсить.");
            return;
        }

        var factory = new ConnectionFactory { Uri = new Uri(uri) };

        using var conn = factory.CreateConnection();
        using var channel = conn.CreateModel();

        channel.ExchangeDeclare(
            exchange: ExchangeName,
            type: ExchangeType,
            durable: true,
            autoDelete: false);

        Console.WriteLine("Publisher is ready → press <Enter> to publish all tags; empty line to exit\n");

        var counter = 0;

        while (true)
        {
            var line = Console.ReadLine();
            if (string.IsNullOrEmpty(line)) break; // пустая строка → выход

            counter++;

            foreach (var tag in tags)
            {
                var msg = new TagValueUpdatedMessageFromObjectModel
                {
                    TagId = tag.Id.ToString(),                  // берем id из JSON
                    Value = counter.ToString()       // можно менять под задачу
                };

                var body = msg.ToByteArray();

                var props = channel.CreateBasicProperties();
                props.DeliveryMode = 2; // persistent

                channel.BasicPublish(
                    exchange: ExchangeName,
                    routingKey: "127.0.0.1:4840",
                    basicProperties: props,
                    body: body);

                Console.WriteLine($"Sent: TagId={tag.Id}, Value={msg.Value}");
            }
        }
    }
}
