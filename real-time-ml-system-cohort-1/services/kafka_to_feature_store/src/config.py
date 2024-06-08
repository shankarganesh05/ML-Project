import os

from dotenv import find_dotenv, load_dotenv

load_dotenv(find_dotenv())

kafka_broker_address = os.environ["KAFKA_BROKER_ADDRESS"]
kafka_topic = "ohlc"
feature_group_name = os.environ["FEATURE_GROUP_NAME"]
feature_group_version = int(os.environ["FEATURE_GROUP_VERSION"])
hopsworks_project_name = os.environ["HOPSWORKS_PRODUCT_NAME"]
hopsworks_api_key = os.environ["HOPSWORKS_API_KEY"]
