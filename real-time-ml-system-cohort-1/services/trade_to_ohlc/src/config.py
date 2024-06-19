import os
from pydantic_settings import BaseSettings
from dotenv import find_dotenv, load_dotenv

#load_dotenv(find_dotenv())

class Config(BaseSettings):
    # Kafka
    kafka_broker_address: str
    kafka_input_topic_name: str
    kafka_output_topic_name: str
    kafka_consumer_group:str

    # OHLC
    ohlc_window_seconds: int
   
config = Config()
# kafka_broker_address = os.environ['KAFKA_BROKER_ADDRESS']
# kafka_input_topic_name = 'trade'
# kafka_output_topic_name = 'ohlc'
# ohlc_window_seconds = int(os.environ['OHLC_WINDOW_SECONDS'])
# ohlc_window_seconds = 60
