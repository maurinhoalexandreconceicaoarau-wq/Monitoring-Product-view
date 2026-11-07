"""Cloud-friendly visual preview of the E-Commerce Monitor dashboard."""
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st


st.set_page_config(page_title="E-Commerce Monitor", page_icon="📊", layout="wide")

EVENT_TYPES = ["view", "addtocart", "transaction"]
EVENT_COLORS = {"view": "#9b7cff", "addtocart": "#42d4bd", "transaction": "#ffb65f"}
PAGES = ["Dashboard", "Capture Data", "Event Stream", "Analytics", "Pipeline", "Database", "Visualization", "Tableau Exports"]
PAGE_LABELS = {
    "Dashboard": "▦  Dashboard",
    "Capture Data": "⇧  Data Ingestion",
    "Event Stream": "≋  Preprocessing",
    "Analytics": "◒  Transaction Analytics",
    "Pipeline": "⌘  Pipeline Status",
    "Database": "▤  Database",
    "Visualization": "▤  Product Analytics",
    "Tableau Exports": "⇩  Reports",
}

st.markdown("""
<style>
:root{color-scheme:dark}
.stApp{background:radial-gradient(ellipse at 80% -20%,#211345 0,transparent 38%),#0b0d20;color:#eeeefe}
[data-testid="stHeader"]{background:rgba(11,13,32,.94)}
.block-container{max-width:1680px;padding:1.4rem 2rem 3rem}
h1,h2,h3,h4{letter-spacing:-.035em;color:#f2f0ff}
h1{font-size:1.9rem!important;font-weight:750}
[data-testid="stSidebar"]{background:linear-gradient(180deg,#090b1d,#0c0d21 72%,#100b25);border-right:1px solid #272347}
[data-testid="stSidebarContent"]{padding:1.25rem .9rem}
[data-testid="stRadio"] [role="radiogroup"]{gap:.32rem;background:transparent;border:0;padding:.2rem}
[data-testid="stRadio"] label{padding:.62rem .7rem;border-radius:9px;color:#aeadc5!important}
[data-testid="stRadio"] label:has(input:checked){background:linear-gradient(95deg,#5938ba,#352467);color:#fff!important;box-shadow:0 4px 16px #613bea40}
[data-testid="stMetric"]{background:linear-gradient(145deg,#181832,#12152b);border:1px solid #302b59;border-radius:15px;padding:15px 17px;min-height:112px;box-shadow:0 8px 24px #08091455}
[data-testid="stMetricLabel"]{color:#aaa9c5!important;font-size:.78rem;letter-spacing:.02em}
[data-testid="stMetricValue"]{color:#f6f4ff;font-size:1.75rem;font-weight:720}
[data-testid="stVerticalBlockBorderWrapper"]{background:linear-gradient(145deg,#15172d,#111326);border:1px solid #302b50;border-radius:16px;box-shadow:0 8px 26px #08091430}
.stButton>button,[data-testid="stDownloadButton"] button{border-radius:10px;border:1px solid #7c5cf3;background:#7254ea;color:#fff;font-weight:650}
.stButton>button:hover,[data-testid="stDownloadButton"] button:hover{background:#8869ff;color:#fff;border-color:#9a82ff}
[data-testid="stDataFrame"]{border:1px solid #302f48;border-radius:10px;overflow:hidden}
[data-testid="stAlert"]{border-radius:11px;background:#1a1929}
[data-testid="stPlotlyChart"]{border-radius:12px}
.side-brand{font-size:1.25rem;font-weight:850;letter-spacing:-.04em;color:#faf8ff;margin:.25rem 0 0}
.side-sub{font-size:.68rem;color:#9998b5;line-height:1.5;margin:.2rem 0 1.3rem}
.side-status{border:1px solid #2d2d46;border-radius:12px;background:#151625;padding:.7rem .75rem;margin:.65rem 0;color:#c2c0da;font-size:.76rem}
.eyebrow{color:#9b8be2;font-size:.7rem;font-weight:750;letter-spacing:.08em;text-transform:uppercase}
.page-subtitle{font-size:.88rem;color:#a5a4bf;margin:.35rem 0 1.2rem}
.mode-pill{display:inline-block;border:1px solid #493d74;background:#211d39;color:#c7b8ff;border-radius:999px;padding:.32rem .62rem;font-size:.72rem;font-weight:650}
.dash-hero{display:flex;justify-content:space-between;align-items:flex-end;gap:1rem;margin:.25rem 0 1rem;padding:.2rem 0 .75rem;border-bottom:1px solid #262541}
.dash-hero h2{font-size:1.55rem!important;margin:0;color:#f3f0ff}.dash-hero p{margin:.22rem 0 0;color:#9695b5;font-size:.78rem}
.dash-note{padding:.65rem .72rem;border:1px solid #4b3e24;border-radius:9px;background:#211c17;color:#e5c98f;font-size:.72rem;line-height:1.5}
.dash-tag{display:inline-block;padding:.2rem .48rem;border:1px solid #39335e;border-radius:999px;color:#b9a9ff;background:#211d3d;font-size:.62rem;white-space:nowrap}
hr{border-color:#2b2a42}
@media(max-width:800px){.block-container{padding:.8rem 1rem 2rem}.dash-hero{align-items:flex-start;flex-direction:column}}
</style>
""", unsafe_allow_html=True)


@st.cache_data
def demo_events() -> pd.DataFrame:
    """Create repeatable, anonymous example data for the hosted showcase."""
    rng = np.random.default_rng(20261107)
    size = 18_000
    start = pd.Timestamp("2025-01-01", tz="UTC")
    times = start + pd.to_timedelta(rng.integers(0, 90 * 24 * 60, size), unit="m")
    kinds = rng.choice(EVENT_TYPES, size, p=[0.82, 0.14, 0.04])
    item_ids = rng.choice(np.arange(10001, 10101), size)
    visitor_ids = rng.integers(100000, 106500, size)
    frame = pd.DataFrame({"event_time": times, "event_type": kinds, "item_id": item_ids, "visitor_id": visitor_ids})
    frame["event_date"] = frame["event_time"].dt.date
    return frame.sort_values("event_time").reset_index(drop=True)


def dark_chart(fig, height=330):
    fig.update_layout(
        height=height,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font_color="#d7d4eb",
        legend_title_text="",
        margin=dict(l=8, r=8, t=16, b=8),
        xaxis=dict(gridcolor="#292943", zerolinecolor="#292943"),
        yaxis=dict(gridcolor="#292943", zerolinecolor="#292943"),
    )
    return fig


events = demo_events()
with st.sidebar:
    st.markdown('<div class="side-brand">E-Commerce Monitor</div><div class="side-sub">RETAILROCKET ANALYTICS<br>Historical Behavior Dashboard</div>', unsafe_allow_html=True)
    page = st.radio("WORKSPACE", PAGES, format_func=lambda value: PAGE_LABELS[value], label_visibility="visible", key="cloud_main_page")
    st.markdown('<div class="side-status">◉ &nbsp; Portfolio Preview<br><span style="color:#87869f">Sample data · no live pipeline</span></div>', unsafe_allow_html=True)
    st.caption("Click events are unavailable in the source project.")
    st.markdown("#### Explore the sample")
    min_day, max_day = events["event_date"].min(), events["event_date"].max()
    dates = st.date_input("Event date range", value=(min_day, max_day), min_value=min_day, max_value=max_day, key="cloud_dates")
    if "cloud_event_types" not in st.session_state:
        st.session_state["cloud_event_types"] = EVENT_TYPES.copy()
    selected_types = st.multiselect("Event type", EVENT_TYPES, key="cloud_event_types")
    st.caption("All figures are generated examples, not real customer or sales data.")

if isinstance(dates, (tuple, list)) and len(dates) == 2:
    start_day, end_day = dates
else:
    start_day = end_day = dates
# Empty selection means all types so it cannot produce an empty dashboard.
effective_types = selected_types or EVENT_TYPES
filtered = events[events["event_date"].between(start_day, end_day) & events["event_type"].isin(effective_types)].copy()

views = int(filtered["event_type"].eq("view").sum())
carts = int(filtered["event_type"].eq("addtocart").sum())
transactions = int(filtered["event_type"].eq("transaction").sum())
visitors = int(filtered["visitor_id"].nunique())
products = int(filtered["item_id"].nunique())
conversion = transactions / views * 100 if views else 0.0

st.markdown('<div class="eyebrow">E-COMMERCE REAL-TIME ANALYTICS · PORTFOLIO PREVIEW</div>', unsafe_allow_html=True)
st.markdown('<div class="page-subtitle">Historical-style example events &nbsp; <span class="mode-pill">Generated sample · offline analytics</span></div>', unsafe_allow_html=True)


def kpis():
    cols = st.columns(4)
    values = [
        ("Sample events", f"{len(filtered):,}", "Generated event records"),
        ("Sample visitors", f"{visitors:,}", "Distinct example IDs"),
        ("Product views", f"{views:,}", f"{carts:,} cart additions"),
        ("Transactions / views", f"{conversion:.2f}%", f"{transactions:,} transaction events"),
    ]
    for col, (label, value, help_text) in zip(cols, values):
        col.metric(label, value, help_text)


def dashboard_page():
    st.markdown('<div class="dash-hero"><div><h2>Dashboard Overview</h2><p>E-commerce behavior monitoring · generated example events</p></div><span class="dash-tag">HISTORICAL SAMPLE</span></div>', unsafe_allow_html=True)
    kpis()
    left, right = st.columns([1.65, 1])
    daily = filtered.groupby(["event_date", "event_type"]).size().rename("events").reset_index()
    with left:
        with st.container(border=True):
            st.markdown("#### Daily event trend")
            if daily.empty:
                st.info("No sample records in this date range.")
            else:
                fig = px.area(daily, x="event_date", y="events", color="event_type", color_discrete_map=EVENT_COLORS)
                st.plotly_chart(dark_chart(fig, 310), use_container_width=True)
    with right:
        with st.container(border=True):
            st.markdown("#### Purchase funnel")
            funnel = pd.DataFrame({"Stage": ["Views", "Add to cart", "Transactions"], "Events": [views, carts, transactions]})
            fig = go.Figure(go.Funnel(y=funnel["Stage"], x=funnel["Events"], marker={"color": [EVENT_COLORS[x] for x in EVENT_TYPES]}))
            st.plotly_chart(dark_chart(fig, 310), use_container_width=True)
    left, right = st.columns([1, 1])
    top = filtered[filtered["event_type"].eq("view")].groupby("item_id").size().nlargest(8).sort_values().rename("views").reset_index()
    recent = filtered.tail(10).sort_values("event_time", ascending=False).copy()
    recent["event_time"] = recent["event_time"].dt.strftime("%Y-%m-%d %H:%M UTC")
    with left:
        with st.container(border=True):
            st.markdown("#### Most viewed sample products")
            if top.empty:
                st.info("No view events in the selected sample.")
            else:
                fig = px.bar(top, x="views", y="item_id", orientation="h", color_discrete_sequence=[EVENT_COLORS["view"]], labels={"views": "Views", "item_id": "Sample product ID"})
                st.plotly_chart(dark_chart(fig, 285), use_container_width=True)
    with right:
        with st.container(border=True):
            st.markdown("#### Recent sample events")
            recent = recent.rename(columns={"event_time": "Event time", "event_type": "Event", "item_id": "Sample product", "visitor_id": "Sample visitor"})
            st.dataframe(recent[["Event time", "Event", "Sample product", "Sample visitor"]], hide_index=True, use_container_width=True, height=285)
    st.markdown('<div class="dash-note"><b>Data note.</b> This preview uses generated sample events. It does not connect to Retailrocket CSV files, Kafka, or Spark. The full local dashboard remains in app.py.</div>', unsafe_allow_html=True)


def render_page():
    if page == "Dashboard":
        dashboard_page()
    elif page == "Capture Data":
        st.markdown("## Data Ingestion Preview")
        st.info("Live replay is not available in the hosted preview. The sample events below are pre-generated for exploration.")
        with st.form("cloud_capture_request"):
            request_col, button_col, _ = st.columns([1, 1, 3])
            request_col.number_input("Number of events", min_value=1, max_value=5000, value=100, step=100, key="cloud_requested_count")
            submitted = button_col.form_submit_button("Request Data", type="primary", use_container_width=True)
        if submitted:
            if filtered.empty:
                st.session_state.pop("cloud_capture_result", None)
                st.warning("No example events match the selected date and event filters. Adjust the filters and request again.")
            else:
                result_count = min(int(requested_count), len(filtered))
                request_number = int(st.session_state.get("cloud_capture_number", 0)) + 1
                st.session_state["cloud_capture_number"] = request_number
                sample = filtered.sample(n=result_count, random_state=20261107 + request_number)
                sample = sample.sort_values("event_time", ascending=False).reset_index(drop=True)
                st.session_state["cloud_capture_result"] = {
                    "capture_id": f"DEMO-{request_number:04d}",
                    "requested": int(st.session_state["cloud_requested_count"]),
                    "returned": result_count,
                    "events": sample,
                }
        kpis()
        result = st.session_state.get("cloud_capture_result")
        if result:
            display_rows = result["events"]
            st.caption(f"Request {result['capture_id']} completed · {result['returned']:,} of {result['requested']:,} events returned.")
            if result["returned"] < result["requested"]:
                st.caption("Fewer matching rows were available than requested.")
        else:
            display_rows = filtered.tail(100).sort_values("event_time", ascending=False)
            st.caption(f"Showing up to 100 matching event rows · {len(filtered):,} events match the current filters.")
        st.dataframe(display_rows, hide_index=True, use_container_width=True)
        if result:
            st.download_button(
                "Download requested data as CSV",
                display_rows.to_csv(index=False).encode("utf-8"),
                f"{result['capture_id'].lower()}_sample_events.csv",
                "text/csv",
            )
    elif page == "Event Stream":
        st.markdown("## Sample Event Stream")
        st.caption("Example event rows; no events are being streamed or processed live.")
        st.dataframe(filtered.tail(500).sort_values("event_time", ascending=False), hide_index=True, use_container_width=True)
    elif page == "Analytics":
        st.markdown("## Transaction Analytics")
        kpis()
        a, b = st.columns(2)
        mix = filtered.groupby("event_type").size().rename("events").reset_index()
        with a:
            with st.container(border=True):
                st.markdown("#### Event mix")
                if not mix.empty:
                    fig = px.pie(mix, names="event_type", values="events", hole=.58, color="event_type", color_discrete_map=EVENT_COLORS)
                    st.plotly_chart(dark_chart(fig), use_container_width=True)
        with b:
            with st.container(border=True):
                st.markdown("#### Product activity")
                product_stats = filtered.pivot_table(index="item_id", columns="event_type", values="visitor_id", aggfunc="count", fill_value=0).reset_index()
                st.dataframe(product_stats.sort_values("view", ascending=False).head(20) if "view" in product_stats else product_stats.head(20), hide_index=True, use_container_width=True)
    elif page == "Pipeline":
        st.markdown("## Pipeline Status")
        st.info("This public preview runs as a single Streamlit app. Kafka and Spark are omitted here; they remain available in the Docker Compose version.")
        a, b, c = st.columns(3)
        a.metric("Preview app", "Online")
        b.metric("Sample data", f"{len(filtered):,} rows")
        c.metric("Live processing", "Disabled")
        with st.container(border=True):
            st.markdown("#### Preview architecture")
            st.code("Generated sample events  →  Streamlit analytics dashboard", language="text")
    elif page == "Database":
        st.markdown("## Sample Data Explorer")
        st.caption("No persistent database is connected in the hosted preview.")
        st.dataframe(filtered, hide_index=True, use_container_width=True)
    elif page == "Visualization":
        st.markdown("## Product Analytics")
        kpis()
        a, b = st.columns(2)
        daily = filtered.groupby(["event_date", "event_type"]).size().rename("events").reset_index()
        with a:
            with st.container(border=True):
                st.markdown("#### Event trend")
                if not daily.empty:
                    st.plotly_chart(dark_chart(px.line(daily, x="event_date", y="events", color="event_type", color_discrete_map=EVENT_COLORS, markers=True)), use_container_width=True)
        with b:
            with st.container(border=True):
                st.markdown("#### Top products by views")
                top = filtered[filtered["event_type"].eq("view")].groupby("item_id").size().nlargest(15).sort_values().rename("views").reset_index()
                if not top.empty:
                    fig = px.bar(top, x="views", y="item_id", orientation="h", color_discrete_sequence=[EVENT_COLORS["view"]], labels={"item_id": "Sample product ID", "views": "Views"})
                    st.plotly_chart(dark_chart(fig, 400), use_container_width=True)
    elif page == "Tableau Exports":
        st.markdown("## Reports and Exports")
        st.caption("Download the currently filtered generated sample rows for exploration.")
        metrics = pd.DataFrame([{"sample_events": len(filtered), "sample_visitors": visitors, "views": views, "cart_additions": carts, "transactions": transactions, "transactions_per_view_pct": round(conversion, 3), "sample_products": products}])
        st.download_button("Download sample events as CSV", filtered.to_csv(index=False).encode("utf-8"), "ecommerce_sample_events.csv", "text/csv")
        st.download_button("Download sample metrics as CSV", metrics.to_csv(index=False).encode("utf-8"), "ecommerce_sample_metrics.csv", "text/csv")


render_page()
st.caption("Generated portfolio data · no real customers · no live Kafka/Spark processing")
