import pandas as pd
import hopsworks
from src.config import config
from loguru import logger
from src.plot import plot_candles
from typing import List,Optional,Dict
import time
def get_feature_view():
    
    """
    Returns:feature_view

    """
    project = hopsworks.login(
        project=config.hopsworks_project_name, api_key_value=config.hopsworks_api_key
    )

    feature_store = project.get_feature_store()
    # Get Feature Group
    feature_group = feature_store.get_feature_group(
        name=config.feature_group_name, version=config.feature_group_version
    )
    # Get or Create Feature View
    feature_view = feature_store.get_or_create_feature_view(
        # name of the feature view
        name=config.feature_view_name, version=config.feature_view_version,
        query=feature_group.select_all()
    )
    return feature_view


def get_feature_from_the_store(
        online_or_offline:str,
        # feature_group_version:int,
        # feature_view_name:str,
        # feature_view_version:int,
)-> pd.DataFrame:
    """
    Args:
    feature_group_name:str
    feature_group_version:int
    feature_view_name:str

    Returns:pd.dataframe

    """
    logger.debug("Getting Feature View")
    feature_view = get_feature_view()
    if online_or_offline =='offline':
        features : pd.DataFrame = feature_view.get_batch_data()
    
    else:
        features: pd.DataFrame = feature_view.get_feature_vectors(
            entry = get_primary_keys(last_n_minutes=20),
            return_type ='pandas'
        )

    return features

def get_primary_keys(last_n_minutes: int) -> List[dict]:
    """
    Args:
        last_n_minutes: int
    Returns:
        List[dict]:
    """
    logger.debug("Getting primary keys")
    current_utc = int(time.time() *1000)
    current_utc= current_utc-(current_utc*60000)
    # generate a list of timestamps in miliseconds for the last 'last_n_minutes' minutes
    timestamps = [current_utc - (i * 60000) for i in range(last_n_minutes)]
    # create a list of dictionaries with the 'timestamp' key
    primary_keys = [
        {
            'product_id': config.product_id,
            'timestamp': timestamp,} for timestamp in timestamps]
    return primary_keys

if __name__ == "__main__":
    data = get_feature_from_the_store(online_or_offline)
    print(data.head())

        
