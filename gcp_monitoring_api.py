import google.auth
from googleapiclient.discovery import build
from google.api_core.client_options import ClientOptions
from google.cloud import container_v1

class GCPMonitor:
    def __init__(self, project_id, credentials):
        self.project_id = project_id
        self.credentials = credentials
        self.credentials, self.project_id = google.auth.default()
        self.service = build('monitoring', 'v3', credentials=self.credentials)
        self.client_options = ClientOptions(api_endpoint='container.googleapis.com')
        self.k8s_engine_client = container_v1.ClusterManagerClient(client_options=self.client_options)

    def get_sql_memory_usage(self, instance_id):
        metric_name = "gce_instance/memory/bytes_used"
        request = self.service.projects().timeSeries().list(
            name="projects/{}".format(self.project_id),
            filter="metric.type={} AND resource.label.instance_id={}".format(metric_name, instance_id)
        )
        response = request.execute()
        if not response.get("timeSeries"):
            # No data for the metric
            return None

        # Get the latest value for the metric
        latest_value = response["timeSeries"][0]["points"][0]["value"]["doubleValue"]

        # Convert to MB
        return latest_value / (1024 * 1024)


    def get_sql_cpu_utilization(self, instance_id):
        metric_name = "gce_instance/cpu/utilization"
        request = self.service.projects().timeSeries().list(
            name="projects/{}".format(self.project_id),
            filter="metric.type={} AND resource.label.instance_id={}".format(metric_name, instance_id)
        )
        response = request.execute()
        if not response.get("timeSeries"):
            # No data for the metric
            return None
        
        latest_value = response["timeSeries"][0]["points"][0]["value"]["doubleValue"]
        
        # Convert to percentage
        return latest_value * 100

    # ----------------- K8S ----------------- #

    def get_pod_names(self, cluster_name, namespace):
        request = self.k8s_engine_client.list_namespaced_pod(namespace=namespace)
        response = request.to_dict()
        pods = response.get("items", [])
        return [pod["metadata"]["name"] for pod in pods]



    def get_k8s_memory_usage(self, cluster_name, namespace, pod_name):
        metric_name = "kubernetes.pod/memory/usage_bytes"
        request = self.service.projects().timeSeries().list(
            name="projects/{}".format(self.project_id),
            filter="metric.type={} AND resource.labels.cluster_name={} AND resource.labels.namespace_name={} AND resource.labels.pod_name={}".format(
                metric_name, cluster_name, namespace, pod_name)
        )
        response = request.execute()
        if not response.get("timeSeries"):
            # No data for the metric
            return None

        # Get the latest value for the metric
        latest_value = response["timeSeries"][0]["points"][0]["value"]["doubleValue"]

        # Convert to MB
        return latest_value / (1024 * 1024)
        


    def get_k8s_cpu_utilization(self, cluster_name, namespace, pod_name):
        metric_name = "kubernetes.pod/cpu/usage_rate"
        request = self.service.projects().timeSeries().list(
            name="projects/{}".format(self.project_id),
            filter="metric.type={} AND resource.labels.cluster_name={} AND resource.labels.namespace_name={} AND resource.labels.pod_name={}".format(
                metric_name, cluster_name, namespace, pod_name)
        )
        response = request.execute()
        if not response.get("timeSeries"):
            # No data for the metric
            return None

        # Get the latest value for the metric
        latest_value = response["timeSeries"][0]["points"][0]["value"]["doubleValue"]

        # Convert to millicores
        return latest_value / (10**6)