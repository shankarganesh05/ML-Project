import pandas as pd
import hopsworks
from src.config import config
from loguru import logger
from src.plot import plot_candles
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
        # feature_group_name:str,
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
    
    features : pd.DataFrame = feature_view.get_batch_data()

    return features

if __name__ == "__main__":
    data = get_feature_from_the_store()
    print(data.head())

        
