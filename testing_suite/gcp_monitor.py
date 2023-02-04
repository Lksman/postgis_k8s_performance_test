from googleapiclient.discovery import build
from secrets import gcp_secrets
import google.auth
from datetime import datetime
import time

class GCPMonitor():
    def __init__(self, project_id: str):
        self.project_id = project_id
        self.monitor_service = build('monitoring', 'v3', developerKey=gcp_secrets['api_key'])

    # ----------------- SQL ----------------- #

    def get_sql_memory_usage(self, instance_id: str, end_time: datetime.datetime) -> float:
        pass
        
    def get_sql_cpu_utilization(self, instance_id: str) -> float:
        pass

    # ----------------- K8S ----------------- #

    def get_pod_names(self, cluster_name: str, namespace: str) -> list:
        pass

    def get_pod_memory_usage(self, cluster_name: str, namespace: str, pod_name: str) -> float:
        pass
    
    def get_pod_cpu_utilization(self, cluster_nam: str, namespace: str, pod_name: str) -> float:
        pass

    def get_cluster_memory_usage(self, cluster_name: str, namespace: str) -> float:
        pod_metrics = []
        for pod_name in self.get_pod_names(cluster_name, namespace):
            pod_metrics.append(self.get_pod_memory_usage(cluster_name, namespace, pod_name))
            pass


    def get_cluster_cpu_utilization(self, cluster_name: str, namespace: str) -> float:
        pod_metrics = []
        for pod_name in self.get_pod_names(cluster_name, namespace):
            pod_metrics.append(self.get_pod_cpu_utilization(cluster_name, namespace, pod_name))
            pass



if __name__ == "__main__":
    gcp_monitor = GCPMonitor(project_id=gcp_secrets['project_id'])