from typing import List,Dict
from datetime import timedelta
from quixstreams import Application
from src import config
from loguru import logger
def trade_to_ohlc(
        kafka_input_topic:str,
        kafka_output_topic:str,
        kafka_broker_address:str,
        ohlc_window_seconds:int,
) -> None:
    """"Reads trades from Kafka input topic
    Aggregates them into OHLC Candles using specified window and s
    saves ohlc data into another kafka topic"""
    app = Application(broker_address=kafka_broker_address,consumer_group="trade_to_ohlc",auto_offset_reset="earliest")
    input_topic = app.topic(name=kafka_input_topic,value_serializer='json')
    output_topic = app.topic(name=kafka_output_topic,value_serializer='json')

    sdf = app.dataframe(input_topic)
    def init_ohlc_candle(value:Dict) -> Dict:
        """
        Initialize the OHLC candle with the first trade
        """

        return {
            "open": value["price"],
            "high": value["price"],
            "low": value["price"],
            "close": value["price"],
            "product_id": value["product_id"]
        }
    def update_ohlc_candle(ohlc_candle:Dict,trade:Dict) -> Dict:
        """
        Update the OHLC candle with the new trade and return the updated candle

        Args:
            ohlc_candle : dict : The current OHLC candle
            trade : dict : The incoming trade
        
        Returns:
            dict : The updated OHLC candle
        """

        return {
            "open": ohlc_candle["open"],
            "high": max(ohlc_candle["high"],trade["price"]),
            "low": min(ohlc_candle["low"],trade["price"]),
            "close": trade["price"],
            "product_id": trade["product_id"]
        }
    # apply tranformations to the incoming data - start
    # Here we need to define how we transform the incoming trades into OHLC candles
    sdf = sdf.tumbling_window(duration_ms=timedelta(seconds=ohlc_window_seconds))
    sdf = sdf.reduce(reducer=update_ohlc_candle, initializer=init_ohlc_candle).current()

    # Unpacking the DataFrames
    sdf['open'] = sdf['value']['open']
    sdf['close'] = sdf['value']['close']
    sdf['high'] = sdf['value']['high']
    sdf['low'] = sdf['value']['low']
    sdf["product_id"] = sdf["value"]["product_id"]
    # adding a timestamp key
    sdf['timestamp'] = sdf['end']
    # let's keep only the keys we want in our final message                            
    sdf = sdf[['timestamp','open','close','high','low','product_id']]

    sdf = sdf.update(logger.info)
    sdf = sdf.to_topic(output_topic)
    # kick-off the streaming application
    app.run(sdf)

if __name__ == '__main__':
    trade_to_ohlc(
        kafka_input_topic = config.kafka_input_topic_name,
        kafka_output_topic=config.kafka_output_topic_name,
        kafka_broker_address=config.kafka_broker_address,
        ohlc_window_seconds=config.ohlc_window_seconds,)