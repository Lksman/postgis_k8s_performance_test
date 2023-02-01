import requests
from google.cloud import monitoring_v3


def CloudMonitoringAPI():
    # Instantiates a client
    client = monitoring_v3.MetricServiceClient()

    # The name of the metric descriptor
    name = client.metric_descriptor_path(
        "tu-ws22-spatialdatabases", 