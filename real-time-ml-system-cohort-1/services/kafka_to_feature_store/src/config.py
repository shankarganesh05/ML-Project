import os
from pydantic_settings import BaseSettings
from dotenv import find_dotenv, load_dotenv

#load_dotenv(find_dotenv())
class Config(BaseSettings):
    kafka_broker_address:str
    kafka_topic:str
    kafka_consumer_group:str
    feature_group_name:str
    feature_group_version:int
    buffer_size:int
    live_or_historical:str
    hopsworks_project_name:str
    hopsworks_api_key:str
    save_every_n_sec:int = 600

# kafka_broker_address = os.environ["KAFKA_BROKER_ADDRESS"]
# kafka_topic = "ohlc"
# feature_group_name = os.environ["FEATURE_GROUP_NAME"]
# feature_group_version = int(os.environ["FEATURE_GROUP_VERSION"])
# hopsworks_project_name = os.environ["HOPSWORKS_PRODUCT_NAME"]
# hopsworks_api_key = os.environ["HOPSWORKS_API_KEY"]
# buffer_size :int = 1000
# live_or_historical = os.environ["LIVE_OR_HISTORICAL"]
config = Config()