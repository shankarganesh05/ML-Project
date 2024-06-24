import streamlit as st
from src.backend import get_feature_from_the_store
from src.plot import plot_candles
from loguru import logger
st.write("""
OHLC DASHBOARD SHANKAR """)
online_or_offline = st.sidebar.selectbox("Select the Store", ("online", "offline"))

logger.info(online_or_offline)
with st.container():
    placeholder_chart = st.empty()

data = get_feature_from_the_store(online_or_offline)
st.bokeh_chart(plot_candles(data.tail(1440)))