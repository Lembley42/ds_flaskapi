from google.cloud import pubsub_v1
from google.auth import jwt


class PubSub():
    def __init__(self, project_id:str) -> None:
        self.project_id = project_id
        self.client = self.Connect()


    def Connect(self) -> pubsub_v1.PublisherClient:
        client = pubsub_v1.PublisherClient()
        return client


    def Get_Topic_Path(self, topic_id:str) -> str:
        return self.client.topic_path(self.project_id, topic_id)


    def Publish(self, topic_path:str, task_id:str, topic_id:str) -> None:
        topic_path = self.client.topic_path(self.project_id, topic_id)
        future = self.client.publish(topic_path, f'{task_id}'.encode("utf-8"))
        future.result()