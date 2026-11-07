"""Lightweight, clearly labeled showcase dashboard for Streamlit Community Cloud."""
from datetime import date, timedelta

import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st


st.set_page_config(
    page_title="E-Commerce Analytics Demo",
    page_icon="📊",
    layout="wide",
)

COLORS = {"view": "#8b75ff", "addtocart": "#35c6b3", "transaction": "#ffb45f"}


@st.cache_data
def make_demo_data() -> pd.DataFrame:
    """Build repeatable example events; no external or personal data is used."""
    rng = np.random.default_rng(20261107)
    rows = 18_000
    start = pd.Timestamp("2025-01-01", tz="UTC")
    times = start + pd.to_timedelta(rng.integers(0, 90 * 24 * 60, rows), unit="m")
    types = rng.choice(["view", "addtocart", "transaction"], rows, p=[0.82, 0.14, 0.04])
    items = rng.choice(np.arange(10001, 10101), rows, p=np.linspace(1, 2, 100) / np.linspace(1, 2, 100).sum())
    visitors = rng.integers(100000, 106500, rows)
    frame = pd.DataFrame({"event_time": times, "event_type": types, "item_id": items, "visitor_id": visitors})
    frame["event_date"] = frame["event_time"].dt.date
    return frame.sort_values("event_time").reset_index(drop=True)


events = make_demo_data()
st.markdown("# E-Commerce Behavior Analytics")
st.caption("Portfolio preview · historical-style example data · no live store or streaming pipeline is connected")
st.info(
    "This hosted preview uses generated sample events so it can run without Kafka, Spark, or the large local dataset. "
    "For the full replay pipeline, run the Docker Compose version from the project README.",
    icon="ℹ️",
)

min_day = events["event_date"].min()
max_day = events["event_date"].max()
with st.sidebar:
    st.header("Explore the sample")
    date_range = st.date_input("Event date range", value=(min_day, max_day), min_value=min_day, max_value=max_day)
    event_type = st.multiselect("Event type", ["view", "addtocart", "transaction"], default=["view", "addtocart", "transaction"])
    st.caption("All values are generated for demonstration. They do not represent real customers or sales.")

if isinstance(date_range, (tuple, list)) and len(date_range) == 2:
    start_day, end_day = date_range
else:
    start_day = end_day = date_range
filtered = events[
    events["event_date"].between(start_day, end_day)
    & events["event_type"].isin(event_type)
]

views = int((filtered.event_type == "view").sum())
carts = int((filtered.event_type == "addtocart").sum())
purchases = int((filtered.event_type == "transaction").sum())
visitors_count = int(filtered.visitor_id.nunique())
conversion = (purchases / views * 100) if views else 0

c1, c2, c3, c4 = st.columns(4)
c1.metric("Sample events", f"{len(filtered):,}")
c2.metric("Sample visitors", f"{visitors_count:,}")
c3.metric("Transaction events", f"{purchases:,}")
c4.metric("Transactions / views", f"{conversion:.2f}%")

left, right = st.columns([1.65, 1])
with left:
    st.subheader("Event activity over time")
    daily = filtered.groupby(["event_date", "event_type"]).size().rename("events").reset_index()
    if daily.empty:
        st.warning("No sample events match these filters.")
    else:
        fig = px.area(daily, x="event_date", y="events", color="event_type", color_discrete_map=COLORS)
        fig.update_layout(legend_title_text="Event", xaxis_title="Date", yaxis_title="Events", margin=dict(l=8, r=8, t=15, b=8))
        st.plotly_chart(fig, use_container_width=True)
with right:
    st.subheader("Event mix")
    mix = filtered.groupby("event_type").size().rename("events").reset_index()
    if mix.empty:
        st.info("Choose at least one event type.")
    else:
        fig = px.pie(mix, names="event_type", values="events", hole=0.62, color="event_type", color_discrete_map=COLORS)
        fig.update_layout(legend_title_text="Event", margin=dict(l=8, r=8, t=15, b=8))
        st.plotly_chart(fig, use_container_width=True)

left, right = st.columns([1, 1])
with left:
    st.subheader("Most viewed sample items")
    top = filtered[filtered.event_type == "view"].groupby("item_id").size().nlargest(10).sort_values().rename("views").reset_index()
    if not top.empty:
        fig = px.bar(top, x="views", y="item_id", orientation="h", labels={"views": "Views", "item_id": "Sample item ID"}, color_discrete_sequence=[COLORS["view"]])
        fig.update_layout(showlegend=False, margin=dict(l=8, r=8, t=15, b=8))
        st.plotly_chart(fig, use_container_width=True)
with right:
    st.subheader("Recent sample events")
    recent = filtered.tail(12).sort_values("event_time", ascending=False).copy()
    recent["event_time"] = recent["event_time"].dt.strftime("%Y-%m-%d %H:%M UTC")
    recent = recent.rename(columns={"event_time": "Event time", "event_type": "Event", "item_id": "Sample item", "visitor_id": "Sample visitor"})
    st.dataframe(recent[["Event time", "Event", "Sample item", "Sample visitor"]], hide_index=True, use_container_width=True)

st.download_button(
    "Download filtered sample events (CSV)",
    data=filtered.to_csv(index=False).encode("utf-8"),
    file_name="ecommerce_sample_events.csv",
    mime="text/csv",
)
st.caption("Source: generated demonstration data. Retailrocket source records contain views, cart additions, and transactions; this preview is not connected to them.")
