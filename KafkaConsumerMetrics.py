from kafka import KafkaConsumer
brokers='<broker-endpoints>'
consumer = KafkaConsumer(bootstrap_servers=brokers)
cmetrics = consumer.metrics()['consumer-metrics']
print("Consumer Metrics")
print("-------------------------------------------------------")
for i in cmetrics :
    print(i, cmetrics[i])