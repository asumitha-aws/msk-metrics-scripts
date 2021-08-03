import boto3
from datetime import datetime, timedelta
# Create CloudWatch client
cloudwatch = boto3.client('cloudwatch',region_name='us-east-1')

# List metrics through the pagination interface
paginator = cloudwatch.get_paginator('list_metrics')
    
    
allMetrics = cloudwatch.list_metrics(Namespace='AWS/Kafka',Dimensions=[{'Name': 'Cluster Name','Value': 'KafkaCluster-NO-TLS' }])
now = datetime.now()
print("=" * 170)
print("%-20s  %-15s %-15s %-15s %-35s %-40s %-40s" % ("Cluster Name", "Broker ID","Topic","Consumer Group", "MetricName", "Average", "Unit"))
print("=" * 170)
for metric in allMetrics['Metrics']:
        output = {}
        for key_value in metric['Dimensions']:
           output[key_value["Name"]] = key_value["Value"]
        #Skipping outofbox topics.
        
        if output.get("Topic") != None and output.get("Topic").startswith("__"):
            continue
        # For each item, look up the cooresponding metrics from CloudWatch
        response = cloudwatch.get_metric_statistics(Namespace='AWS/Kafka',
                                            MetricName=metric['MetricName'],
                                            Dimensions=metric['Dimensions'],
                                            Statistics=['Average'],
                                            Period=3600,
                                            StartTime=(now-timedelta(minutes=1)).isoformat(),
                                            EndTime=now.isoformat()
                                            )


        for item in response["Datapoints"]:
            print("%-20s  %-15s %-15s %-15s %-35s %-40s %-40s" % (
            output.get("Cluster Name"),output.get("Broker ID"),output.get("Topic"),output.get("Consumer Group"),metric['MetricName'], str(item["Average"]), item["Unit"]))
