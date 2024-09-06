import os
import pathlib

from kafka import KafkaProducer
from rich import print as rich_print


def main():
    events = pathlib.Path(os.getenv("EVENTS_FILE"))
    rich_print("Sending events from path", events)
    addr = os.getenv("KAFKA_ADDR")
    topic = os.getenv("KAFKA_TOPIC")
    rich_print("kafka:", addr, "topic:", topic)

    producer = KafkaProducer(
        bootstrap_servers=addr,
        security_protocol="SASL_SSL",
        sasl_mechanism="SCRAM-SHA-512",
        sasl_plain_username=os.getenv("SASL_USER"),
        sasl_plain_password=os.getenv("SASL_PASS"),
    )
    with events.open() as fp:
        cnt = 0
        for line in fp:
            msg = line.encode()
            producer.send(topic, msg)
            cnt += 1

    rich_print("sent", cnt, "lines")
    producer.flush()
    producer.close()


if __name__ == "__main__":
    main()
