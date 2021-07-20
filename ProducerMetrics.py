%pyflink
from kafka import KafkaProducer
brokers='<broker-endpoints>'
producer = KafkaProducer(bootstrap_servers=brokers)
metrics=producer.metrics()


pmetrics=metrics['producer-metrics']

print("Producer Metrics")
print("-------------------------------------------------------")
for i in pmetrics :
    print(i, pmetrics[i])
