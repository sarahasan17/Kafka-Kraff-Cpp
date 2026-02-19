#include <iostream>
#include <string>
#include <chrono>
#include <thread>
#include <librdkafka/rdkafka.h>
#include <nlohmann/json.hpp>

using json = nlohmann::json;

// Delivery callback
static void dr_cb(rd_kafka_t *rk, const rd_kafka_message_t *msg, void *opaque)
{
    if (msg->err)
    {
        std::cerr << "❌ Delivery failed: "
                  << rd_kafka_err2str(msg->err) << std::endl;
    }
    else
    {
        std::cout << "✅ Delivered to "
                  << rd_kafka_topic_name(msg->rkt)
                  << " [partition " << msg->partition << "]"
                  << std::endl;
    }
}

int main()
{
    const std::string brokers = "kafka:9092";
    const std::string topic = "orders";

    char errstr[512];

    // Create Kafka config
    rd_kafka_conf_t *conf = rd_kafka_conf_new();
    rd_kafka_conf_set_dr_msg_cb(conf, dr_cb);

    // Setup configs
    rd_kafka_conf_set(conf, "bootstrap.servers", brokers.c_str(), errstr, sizeof(errstr));
    rd_kafka_conf_set(conf, "acks", "all", errstr, sizeof(errstr));
    rd_kafka_conf_set(conf, "enable.idempotence", "true", errstr, sizeof(errstr));

    // Create producer instance
    rd_kafka_t *producer = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
    rd_kafka_topic_t *rkt = rd_kafka_topic_new(producer, topic.c_str(), NULL);

    // Send 5 JSON events
    for (int i = 1; i <= 5; i++)
    {
        // JSON event data
        json event = {
            {"orderId", i},
            {"customer", "Sara"},
            {"amount", 199.99 + i},
            {"status", "CREATED"},
            {"timestamp", std::time(nullptr)}};

        std::string payload = event.dump();
        std::cout << "📤 Producing JSON: " << payload << std::endl;

        // Produce message
        int result = rd_kafka_produce(
            rkt,
            RD_KAFKA_PARTITION_UA,
            RD_KAFKA_MSG_F_COPY,
            (void *)payload.c_str(),
            payload.size(),
            NULL, 0,
            NULL);

        if (result != 0)
        {
            std::cerr << "❌ Produce error: "
                      << rd_kafka_err2str(rd_kafka_last_error()) << std::endl;
        }

        // Let Kafka process internal events
        rd_kafka_poll(producer, 100);
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    // Wait for delivery reports
    rd_kafka_flush(producer, 5000);

    rd_kafka_topic_destroy(rkt);
    rd_kafka_destroy(producer);

    std::cout << "\n🎉 Produced 5 JSON messages successfully!" << std::endl;
    return 0;
}
