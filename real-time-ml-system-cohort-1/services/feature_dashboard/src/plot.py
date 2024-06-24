import pandas as pd
from typing import Optional
from bokeh.plotting import figure
from datetime import timedelta

def plot_candles(df:pd.DataFrame,title:Optional[str]='',window_seconds:Optional[int]=60,)-> 'figure':
    """
    Plot candlestick chart from a pandas dataframe containing OHLCV data.

    Args:
        df (pd.DataFrame): DataFrame containing OHLCV data.

    Returns:
        bokeh.plotting.figure: Figure object containing the candlestick chart.
    """
    
    # df.index = pd.to_datetime(df.index,unit='ms')
    df['date'] = pd.to_datetime(df['timestamp'],unit='ms')
    inc = df.close > df.open
    dec = df.open > df.close

    w = 1000 * window_seconds / 2# half day in ms

    TOOLS = "pan,wheel_zoom,box_zoom,reset,save"

    x_max = df['date'].max() + timedelta(minutes=5)
    x_min = df['date'].min() - timedelta(minutes=5)

    # Create a new plot with a title and axis labels
    p = figure(x_axis_type="datetime",tools=TOOLS,width=1000,title=title,x_range=(x_min, x_max))
    p.grid.grid_line_alpha = 0.3
    p.segment(df.date, df.high, df.date, df.low, color="black")
    p.vbar(df.date[inc], w, df.open[inc], df.close[inc], fill_color='#70bd40', line_color="black")
    p.vbar(df.date[dec], w, df.open[dec], df.close[dec], fill_color='#F2583E', line_color="black")

    return p