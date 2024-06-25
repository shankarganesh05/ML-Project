import hopsworks
from loguru import logger
import pandas as pd
from hsfs.feature_group import FeatureGroup
from tools.config import config

class OhlcDataWriter:
        
        def __init__(self, feature_group_name, feature_group_version,hopsworks_project_name,hopsworks_api_key):
            self.feature_group_name = feature_group_name
            self.feature_group_version = feature_group_version
            self.hopsworks_project_name = hopsworks_project_name
            self.hopsworks_api_key = hopsworks_api_key

        def write_from_csv(self, csv_file_path: str):
        
            feature_group = self._get_feature_group()


            data = pd.read_csv(csv_file_path)

            feature_group.insert(data, write_options={'start_offline_materialization': True})
            logger.info("Inserted data into feature group")
        
        def _get_feature_group(self):
            
            project = hopsworks.login(
                project=self.hopsworks_project_name,
                api_key_value=self.hopsworks_api_key,
            )
            feature_store = project.get_feature_store()
            feature_group = feature_store.get_or_create_feature_group(
                name=self.feature_group_name,
                version=self.feature_group_version,
                description='OHLC data coming from Kraken',
                primary_key=['product_id', 'timestamp'],
                event_time='timestamp',
                online_enabled=True,
            )

            return feature_group
def main(csv_file:str):
        writer = OhlcDataWriter(
            feature_group_name=config.feature_group_name,
            feature_group_version=config.feature_group_version,
            hopsworks_project_name=config.hopsworks_project_name,
            hopsworks_api_key=config.hopsworks_api_key,
        )
        writer.write_from_csv(csv_file)
       
if __name__ == '__main__':

    from fire import Fire
    Fire(main)

