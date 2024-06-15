from typing import List,Dict
import hopsworks
import pandas as pd
from typing import Dict
from src import config


def push_to_featurestore(
    feature_group_name: str, feature_group_version: int, Data: List[Dict],online_or_offline: str,
) -> None:
    """
    Pushes the given `data` to the feature store, writing it to the feature group
    with name `feature_group_name` and version `feature_group_version`.

    Args:
        feature_group_name (str): The name of the feature group to write to.
        feature_group_version (int): The version of the feature group to write to.
        data (List[Dict]): A list of dictionaries containing the data to write.
    Returns:
        None
    """
    project = hopsworks.login(
        project=config.hopsworks_project_name, api_key_value=config.hopsworks_api_key
    )

    feature_store = project.get_feature_store()
    ohlc_feature_group = feature_store.get_or_create_feature_group(
        name=feature_group_name,
        version=feature_group_version,
        online_enabled=True,
        description="Pushing OHLC to Feature store",
        primary_key=["product_id", "timestamp"],
        event_time="timestamp",
    )

    data = pd.DataFrame(Data)
    # breakpoint()
    ohlc_feature_group.insert(
        data, write_options={"start_offline_materialization": True if online_or_offline == "offline" else False}
    )
