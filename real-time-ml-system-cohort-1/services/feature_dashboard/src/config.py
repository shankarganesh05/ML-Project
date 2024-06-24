import os
from pydantic_settings import BaseSettings

#load_dotenv(find_dotenv())
class Config(BaseSettings):
    feature_group_name:str
    feature_group_version:int
    feature_view_name:str
    feature_view_version:int
    hopsworks_project_name:str
    hopsworks_api_key:str
    product_id:str
    
config = Config()