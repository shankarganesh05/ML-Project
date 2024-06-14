import os

from dotenv import find_dotenv, load_dotenv

load_dotenv(find_dotenv())

kafka_broker_address=os.environ['KAFKA_BROKER_ADDRESS']
kafka_topic_name='trade'
product_id = ['BTC/USD','ETH/USD']
live_or_historical :str = os.environ['LIVE_OR_HISTORICAL']
last_n_days :int = int(os.environ['LAST_N_DAYS'])