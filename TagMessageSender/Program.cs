using System;
using Google.Protobuf;
using RabbitMQ.Client;
using TagValueUpdatedMessage;               // namespace, в который сгенерируется proto‑класс

internal class Program
{
    // ------------- настройки очереди / exchange ------------------
    private const string ExchangeName = "tag_value_updated_from_object_model";
    private const string ExchangeType = "direct";    // как у консьюмера
    // -------------------------------------------------------------

    private static void Main()
    {
        Console.Write("RabbitMQ connection string (amqp://user:pass@host:port/): ");
        var uri = Console.ReadLine()!
                  ?? "amqp://user:user@localhost:5672/";

        var factory = new ConnectionFactory { Uri = new Uri(uri) };

        using var conn    = factory.CreateConnection();
        using var channel = conn.CreateModel();

        channel.ExchangeDeclare(
            exchange:   ExchangeName,
            type:       ExchangeType,
            durable:    true,
            autoDelete: false);

        Console.WriteLine("Publisher is ready → press <Enter> to publish; empty line to exit\n");

        var tagId = "3fa85f64-5717-4562-b3fc-2c963f66afa6";   // один и тот же TagId для всех тестовых сообщений
        var counter = 0;

        while (true)
        {
            var line = Console.ReadLine();
            if (string.IsNullOrEmpty(line)) break;          // пустая строка → выходим

            // Генерируем тестовое значение
            var value = (++counter).ToString();

            var msg = new TagValueUpdatedMessageFromObjectModel
            {
                TagId = tagId,
                Value = value
            };

            var body = msg.ToByteArray();                   // сериализация protobuf

            var props = channel.CreateBasicProperties();
            props.DeliveryMode = 2;                         // persistent

            channel.BasicPublish(
                exchange: ExchangeName,
                routingKey: "127.0.0.1:4840",                         // fanout не использует
                basicProperties: props,
                body: body);

            Console.WriteLine($"TagId={tagId}, Value={value}");
        }
    }
}
