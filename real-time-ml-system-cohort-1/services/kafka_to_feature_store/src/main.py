from loguru import logger
from quixstreams import Application
from src import config
import json
from src.hopsworks_api import push_to_featurestore


def kafka_to_feature(
    feature_group_name: str,
    feature_group_version: int,
    kafka_topic: str,
    kafka_broker_address: str,
) -> None:
    app = Application(
        broker_address=kafka_broker_address,
        consumer_group="kafka_to_feature_store",
        auto_offset_reset="latest",
    )
    # Create a consumer and start a polling loop
    with app.get_consumer() as consumer:
        consumer.subscribe(
            topics=[kafka_topic],
        )

        while True:
            msg = consumer.poll(0.1)
            if msg is None:
                continue
            elif msg.error():
                logger.error(msg.error())
                continue
            msg1 = msg.value().decode("utf-8")
            ohlc = json.loads(msg1)
            # breakpoint()
            push_to_featurestore(
                feature_group_name=feature_group_name,
                feature_group_version=feature_group_version,
                Data=ohlc,
            )

            consumer.store_offsets(message=msg)


if __name__ == "__main__":
    kafka_to_feature(
        feature_group_name=config.feature_group_name,
        feature_group_version=config.feature_group_version,
        kafka_broker_address=config.kafka_broker_address,
        kafka_topic=config.kafka_topic,
    )
