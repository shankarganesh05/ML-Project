import os
from typing import List,Optional
from dotenv import find_dotenv, load_dotenv
from pydantic_settings import BaseSettings


#load_dotenv(find_dotenv())

class Config(BaseSettings):
    kafka_broker_address: str
    kafka_topic_name: str
    product_id: List[str]
    live_or_historical: str
    last_n_days: Optional[int] = 1

config = Config()
# kafka_broker_address=os.environ['KAFKA_BROKER_ADDRESS']
# kafka_topic_name=os.environ['KAFKA_TOPIC_NAME']
# product_id = os.environ['PRODUCT_ID']
# live_or_historical :str = os.environ['LIVE_OR_HISTORICAL']
# last_n_days :int = int(os.environ['LAST_N_DAYS'])