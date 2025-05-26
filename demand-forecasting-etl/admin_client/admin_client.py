from confluent_kafka.admin import AdminClient, NewTopic

admin = AdminClient({'bootstrap.servers': 'localhost:9092'})

new_topics = [
    NewTopic(topic, num_partitions=3, replication_factor=1)
    for topic in ["demo_topic", "demo_topic2"]
    ]

future_result = admin.create_topics(new_topics)

admin = AdminClient({'bootstrap.servers': 'localhost:9092'})
new_topics = [
    NewTopic(topic, num_partitions=3, replication_factor=1)
    for topic in ["demo_topic", "demo_topic2"]
    ]
future_result = admin.create_topics(new_topics)

for topic, item in future_result.items():
    try:
        item.result()
        print("Topic {} created".format(topic))
    except Exception as e:
        print("Failed to create topic {}: {}".format(topic, e))
