import json
import time

from confluent_kafka import Producer

from PropertyConstants import *


def read_events(limit: int = None):
    """
    Read events line by line from file and convert into json
    :param limit:
    :return:
    """
    counter = 0
    with open("devices_data/device_05.json") as lines:
        for line in lines:
            if not line.strip():
                continue

            event = json.loads(line)
            yield event
            counter += 1

            if limit and counter >= limit:
                break


def kafka_conn():
    conf = {
        'bootstrap.servers': PropertyConstants.BOOTSTRAP_SERVER,
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': PropertyConstants.CLUSTER_API_KEY,
        'sasl.password': PropertyConstants.CLUSTER_API_SECRET,
        'client.id': "sourav-local"

    }
    return Producer(conf)


def delivery_callback(err, msg):
    if err:
        print(f"{err}")
    else:
        print(f"delivered to {msg.topic()} , Partition : {msg.partition()} @ offset {msg.offset()}")


def produce_events(topic, delay=0.5):
    producer = kafka_conn()
    for line in read_events():
        key = line['customerId']
        value = json.dumps(line)

        producer.produce(
            topic=topic,
            key=key,
            value=value,
            callback=delivery_callback
        )

        producer.poll(0)
        time.sleep(delay)
    producer.flush(10)


if __name__ == '__main__':
    produce_events(
        topic='device-data',
    )
