#include <iostream>
#include <string>
#include <librdkafka/rdkafka.h>
#include <nlohmann/json.hpp>

using json = nlohmann::json;

int main()
{
    const std::string brokers = "kafka:9092";
    const std::string topic = "orders";
    const std::string group_id = "notification-service";

    char errstr[512];

    rd_kafka_conf_t *conf = rd_kafka_conf_new();
    rd_kafka_conf_set(conf, "bootstrap.servers", brokers.c_str(), NULL, 0);
    rd_kafka_conf_set(conf, "group.id", group_id.c_str(), NULL, 0);
    rd_kafka_conf_set(conf, "auto.offset.reset", "earliest", NULL, 0);

    rd_kafka_t *consumer = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
    rd_kafka_poll_set_consumer(consumer);

    rd_kafka_topic_partition_list_t *topics = rd_kafka_topic_partition_list_new(1);
    rd_kafka_topic_partition_list_add(topics, topic.c_str(), -1);

    rd_kafka_subscribe(consumer, topics);

    std::cout << "📩 Notification service listening for new orders...\n";

    while (true)
    {
        rd_kafka_message_t *msg = rd_kafka_consumer_poll(consumer, 1000);
        if (!msg)
            continue;

        if (msg->err)
        {
            rd_kafka_message_destroy(msg);
            continue;
        }

        std::string payload((char *)msg->payload, msg->len);

        try
        {
            json j = json::parse(payload);

            std::cout << "\n🔔 New Order Notification\n";
            std::cout << "Customer: " << j["customer"] << "\n";
            std::cout << "Amount: ₹" << j["amount"] << "\n";
            std::cout << "Status: " << j["status"] << "\n";

            std::cout << "📨 Sending Email...\n";
            std::cout << "📱 Sending SMS...\n";
            std::cout << "✔ Notification Completed!\n";
        }
        catch (...)
        {
            std::cout << "Invalid JSON received.\n";
        }

        rd_kafka_message_destroy(msg);
    }
}
