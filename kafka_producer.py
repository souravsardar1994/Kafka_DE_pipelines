from confluent_kafka import Producer
import json
import time
from abc import ABC, abstractmethod
from typing import Dict, Generator, Any
from PropertyConstants import PropertyConstants


class IEventReader(ABC):
    @abstractmethod
    def read_events(self, limit: int = None):
        pass


class FileEventReader(IEventReader):
    def __init__(self, file_path: str):
        self.file_path = file_path

    def read_events(self, limit: int = None):
        counter = 0
        with open(self.file_path) as lines:
            for line in lines:
                if not line.strip():
                    continue
                try:
                    event = json.loads(line)
                except Exception as e:
                    print(f"Error {e}")
                    continue

                yield event
                counter += 1

                if limit and counter >= limit:
                    break


class EventProducer:
    def __init__(self, topic: str, reader: IEventReader):
        self.topic = topic
        self.reader = reader
        self.conf = {
            'bootstrap.servers': PropertyConstants.BOOTSTRAP_SERVER,
            'security.protocol': 'SASL_SSL',
            'sasl.mechanism': 'PLAIN',
            'sasl.username': PropertyConstants.CLUSTER_API_KEY,
            'sasl.password': PropertyConstants.CLUSTER_API_SECRET,
            'client.id': "sourav-local"
        }
        self.producer = Producer(self.conf)

    def _deliverry_callback(self, err, msg):
        if err:
            print(f"delivery failed : {err}")
        else:
            print(f"delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")

    def produce_events(self, limit: int = 10, delay: float = 0.5):
        for event in self.reader.read_events(limit=limit):
            key = str(event.get("customerId", "unknown")).encode("utf-8")
            value = json.dumps(event).encode("utf-8")

            self.producer.produce(
                self.topic,
                key=key,
                value=value,
                callback=self._deliverry_callback
            )

            self.producer.poll(0)
            time.sleep(delay)
        self.producer.flush(10)


if __name__ == '__main__':
    file_reader = FileEventReader("devices_data/device_04.json")
    producer = EventProducer(topic="device-data", reader=file_reader)
    producer.produce_events(limit=10)
