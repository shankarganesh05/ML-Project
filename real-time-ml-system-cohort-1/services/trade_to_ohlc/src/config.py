import os

from dotenv import find_dotenv, load_dotenv

load_dotenv(find_dotenv())

kafka_broker_address = os.environ['KAFKA_BROKER_ADDRESS']
kafka_input_topic_name = 'trade'
kafka_output_topic_name = 'ohlc'
ohlc_window_seconds = int(os.environ['OHLC_WINDOW_SECONDS'])
# ohlc_window_seconds = 60
