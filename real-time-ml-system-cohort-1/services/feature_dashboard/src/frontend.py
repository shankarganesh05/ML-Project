import streamlit as st
from src.backend import get_feature_from_the_store
from src.plot import plot_candles
st.write("""
OHLC DASHBOARD SHANKAR """)
online_or_offline = st.sidebar.selectbox("Select the Store", ("online", "offline"))

data = get_feature_from_the_store(online_or_offline)
st.bokeh_chart(plot_candles(data.tail(1440)))