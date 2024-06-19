from loguru import logger
from quixstreams import Application
from src.config import config
from datetime import datetime, timezone
import json
from src.hopsworks_api import push_to_featurestore

def get_current_utc_sec() -> int:
    return int(datetime.now(timezone.utc).timestamp())

def kafka_to_feature(
    feature_group_name: str,
    feature_group_version: int,
    kafka_topic: str,
    kafka_broker_address: str,
    kafka_consumer_group: str,
    buffer_size:int,
    save_every_n_sec:int,
    live_or_historical: str
) -> None:
    """
    Reads `ohlc` data from the Kafka topic and writes it to the feature store.
    More specifically, it writes the data to the feature group specified by
    - `feature_group_name` and `feature_group_version`.
    Args:
        kafka_topic (str): The Kafka topic to read from.
        kafka_broker_address (str): The address of the Kafka broker.
        feature_group_name (str): The name of the feature group to write to.
        feature_group_version (int): The version of the feature group to write to.
        buffer_size (int): The number of messages to read from Kafka before writing to the feature store.
        live_or_historical (str): Whether we are saving live data to the Feature or historical data.
        Live data goes to the online feature store
        While historical data goes to the offline feature store.
    Returns:
        None  
    """
    app = Application(
        broker_address=kafka_broker_address,
        consumer_group=kafka_consumer_group,
        auto_offset_reset="earliest" if live_or_historical == 'historical' else 'latest',
    )
    last_saved_to_feature_store_ts = get_current_utc_sec()
    
    # Create a consumer and start a polling loop
    with app.get_consumer() as consumer:
        consumer.subscribe(
            topics=[kafka_topic],
        )
        buffer = []
        while True:
            msg = consumer.poll(1)
            # number of seconds since the last time we saved data to the feature store
            sec_since_last_saved = (get_current_utc_sec() - last_saved_to_feature_store_ts)
            if msg is not None and msg.error():
                # We have a message but it is an error.
                # We just log the error and continue
                logger.error(msg.error())
                continue
                #n_sec = 10
                #logger.debug(f'No new messages in the input topic {kafka_topic}')
            elif (msg is None) and (sec_since_last_saved<save_every_n_sec):
                     # There are no new messages in the input topic and we haven't hit the timer
                    # limit yet. We skip and continue polling messages from Kafka.
                    logger.debug(f'No new messages in the input topic {kafka_topic}')
                    logger.debug(f'Last saved to feature store {sec_since_last_saved} seconds ago')
                    #logger.debug('Exceeded timer limit! We push the data to the feature store.')
                    logger.debug(f'We have not hit the {save_every_n_sec} second limit.')
                    continue
                    
            else:
                if msg is not None:
                    # append the data to the buffer
                    msg1 = msg.value().decode("utf-8")
                    ohlc = json.loads(msg1)
                    buffer.append(ohlc)
                    logger.debug(f'Message was pushed to buffer. Buffer size={len(buffer)}')
                    # if the buffer is full or we have hit the timer limit,
                    # we write the data to the feature store
                    if (len(buffer) >= buffer_size) or (sec_since_last_saved >= save_every_n_sec):
                        # if the buffer is not empty we write the data to the feature store
                        if len(buffer) > 0:
                            push_to_featurestore(
                                feature_group_name=feature_group_name,
                                feature_group_version=feature_group_version,
                                Data=buffer,
                                online_or_offline='online' if live_or_historical == 'live' else 'offline',
                            )
                            continue
                        buffer=[]
                        last_saved_to_feature_store_ts = get_current_utc_sec()
                        #consumer.store_offsets(message=msg)       
            
if __name__ == "__main__":
    logger.debug(config.model_dump())
    try:
        kafka_to_feature(
            feature_group_name=config.feature_group_name,
            feature_group_version=config.feature_group_version,
            kafka_broker_address=config.kafka_broker_address,
            kafka_consumer_group=config.kafka_consumer_group,
            kafka_topic=config.kafka_topic,
            buffer_size = config.buffer_size,
            save_every_n_sec=config.save_every_n_sec,
            live_or_historical = config.live_or_historical,
        )
    except KeyboardInterrupt:
        logger.error("Existing Gracefully...")
