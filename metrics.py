import boto3
from datetime import datetime, timedelta
# Create CloudWatch client
cloudwatch = boto3.client('cloudwatch',region_name='us-east-1')

# List metrics through the pagination interface
paginator = cloudwatch.get_paginator('list_metrics')
#for response in paginator.paginate(Dimensions=[{'Name': 'Cluster Name','Value': 'KafkaCluster-NO-TLS' }],
#                                   MetricName='GlobalTopicCount',
#                                   Namespace='AWS/Kafka'):
#    print(response['Metrics'])
    
    
allbuckets = cloudwatch.list_metrics(Namespace='AWS/Kafka',Dimensions=[{'Name': 'Cluster Name','Value': 'KafkaCluster-NO-TLS' }])
#print(allbuckets)
    
allbuckets = cloudwatch.list_metrics(Namespace='AWS/Kafka',Dimensions=[{'Name': 'Cluster Name','Value': 'KafkaCluster-NO-TLS' }])
now = datetime.now()
print("=" * 170)
print("%-20s  %-15s %-15s %-15s %-35s %-40s %-40s" % ("Cluster Name", "Broker ID","Topic","Consumer Group", "MetricName", "Average", "Unit"))
print("=" * 170)
for bucket in allbuckets['Metrics']:
        output = {}
        for key_value in bucket['Dimensions']:
           output[key_value["Name"]] = key_value["Value"]
        #Skipping outofbox topics.
        
        if output.get("Topic") != None and output.get("Topic").startswith("__"):
            continue
        # For each bucket item, look up the cooresponding metrics from CloudWatch
        response = cloudwatch.get_metric_statistics(Namespace='AWS/Kafka',
                                            MetricName=bucket['MetricName'],
                                            Dimensions=bucket['Dimensions'],
                                            Statistics=['Average'],
                                            Period=3600,
                                            StartTime=(now-timedelta(minutes=1)).isoformat(),
                                            EndTime=now.isoformat()
                                            )
        #print(bucket)
        #print(response)


        for item in response["Datapoints"]:
            print("%-20s  %-15s %-15s %-15s %-35s %-40s %-40s" % (
            output.get("Cluster Name"),output.get("Broker ID"),output.get("Topic"),output.get("Consumer Group"),bucket['MetricName'], str(item["Average"]), item["Unit"]))
        #break;
