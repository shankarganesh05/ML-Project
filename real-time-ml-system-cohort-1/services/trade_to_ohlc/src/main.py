from datetime import timedelta
from typing import Dict

from loguru import logger
from quixstreams import Application

from src import config


def trade_to_ohlc(
    kafka_input_topic: str,
    kafka_output_topic: str,
    kafka_broker_address: str,
    ohlc_window_seconds: int,
) -> None:
    """ "Reads trades from Kafka input topic
    Aggregates them into OHLC Candles using specified window and s
    saves ohlc data into another kafka topic"""
    app = Application(
        broker_address=kafka_broker_address,
        consumer_group='trade_to_ohlc',
        auto_offset_reset='earliest',
    )
    input_topic = app.topic(name=kafka_input_topic, value_serializer='json')
    output_topic = app.topic(name=kafka_output_topic, value_serializer='json')

    sdf = app.dataframe(input_topic)

    def init_ohlc_candle(value: Dict) -> Dict:
        return {
            'open': value['price'],
            'high': value['price'],
            'low': value['price'],
            'close': value['price'],
            'product_id': value['product_id'],
        }

    def update_ohlc_candle(ohlc_candle: Dict, trade: Dict) -> Dict:
        return {
            'open': ohlc_candle['open'],
            'high': max(ohlc_candle['high'], trade['price']),
            'low': min(ohlc_candle['low'], trade['price']),
            'close': trade['price'],
            'product_id': trade['product_id'],
        }

    sdf = sdf.tumbling_window(duration_ms=timedelta(seconds=ohlc_window_seconds))
    sdf = sdf.reduce(reducer=update_ohlc_candle, initializer=init_ohlc_candle).current()

    # Unpacking the DataFrames
    sdf['open'] = sdf['value']['open']
    sdf['close'] = sdf['value']['close']
    sdf['High'] = sdf['value']['high']
    sdf['Low'] = sdf['value']['low']
    sdf['product_id'] = sdf['value']['product_id']

    sdf['Timestamp'] = sdf['end']

    sdf = sdf[['Timestamp', 'open', 'close', 'High', 'Low', 'product_id']]

    sdf = sdf.update(logger.info)

    sdf = sdf.to_topic(output_topic)

    app.run(sdf)


if __name__ == '__main__':
    trade_to_ohlc(
        kafka_input_topic=config.kafka_input_topic_name,
        kafka_output_topic=config.kafka_output_topic_name,
        kafka_broker_address=config.kafka_broker_address,
        ohlc_window_seconds=config.ohlc_window_seconds,
    )
