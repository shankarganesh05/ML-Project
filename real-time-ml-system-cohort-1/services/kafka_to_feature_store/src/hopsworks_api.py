import hopsworks
import pandas as pd
from typing import Dict
from src import config


def push_to_featurestore(
    feature_group_name: str, feature_group_version: int, Data: Dict
) -> None:
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

    data = pd.DataFrame([Data])
    # breakpoint()
    ohlc_feature_group.insert(
        data, write_options={"start_offline_materialization": False}
    )
