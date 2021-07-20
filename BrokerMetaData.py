%pyflink
from kafka.cluster import ClusterMetadata

brokers='<broker-endpoints>'

cluster = ClusterMetadata(bootstrap_servers=brokers)

print(cluster.brokers())
