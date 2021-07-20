%pyflink
import confluent_kafka
brokers='<broker-endpoints>'
group='None'



consumer = confluent_kafka.Consumer({'bootstrap.servers': brokers,
                                     'group.id': group})
print("=" * 72)
print("Metrics of Kafka")
print("=" * 72)
print("")


print("All Kafka Topics")
print("=" * 72)
#Raw Print all topics including internal
allTopics = consumer.list_topics().topics
print(allTopics)
print("=" * 72)

print("")
print("User Defined Topics")
print("=" * 72)
print("%-50s  %10s" % ("Topic", "Partition Count"))
print("=" * 72)
#Print all user defined topics and collect in dict
pairs = allTopics.items()
for key, value in pairs:
    if key.startswith( '_' ) == False:
        topics.append(key)
        print("%-50s  %10d " % (key,len(value.partitions)))


print("")
print("Topic Commits & Lag")
print("=" * 72)
print("%-50s  %9s  %9s" % ("Topic [Partition]", "Committed", "Lag"))
print("=" * 72)
for topic in topics:
    print(topic)
    # Get the topic's partitions
    metadata = consumer.list_topics(topic, timeout=10)
    if metadata.topics[topic].error is not None:
        raise confluent_kafka.KafkaException(metadata.topics[topic].error)

    # Construct TopicPartition list of partitions to query
    partitions = [confluent_kafka.TopicPartition(topic, p) for p in metadata.topics[topic].partitions]

    # Query committed offsets for this group and the given partitions
    committed = consumer.committed(partitions, timeout=10)

    for partition in committed:
        # Get the partitions low and high watermark offsets.
        (lo, hi) = consumer.get_watermark_offsets(partition, timeout=10, cached=False)

        if partition.offset == confluent_kafka.OFFSET_INVALID:
            offset = "-"
        else:
            offset = "%d" % (partition.offset)

        if hi < 0:
            lag = "no hwmark"  # Unlikely
        elif partition.offset < 0:
            # No committed offset, show total message count as lag.
            # The actual message count may be lower due to compaction
            # and record deletions.
            lag = "%d" % (hi - lo)
        else:
            lag = "%d" % (hi - partition.offset)

        print("%-50s  %9s  %9s" % (
            "{} [{}]".format(partition.topic, partition.partition), offset, lag))


consumer.close()
