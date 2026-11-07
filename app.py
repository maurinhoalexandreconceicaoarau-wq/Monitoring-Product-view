"""Streamlit UI for Retailrocket Kafka and Spark real-time simulation."""
import csv
import html
import gzip
import io
import json
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from dashboard_backend import (
    ARTIFACTS, METRICS, SPARK_EVENTS, SPARK_STATE, DATA, dashboard_data,
    health_data, read_json, request_capture,
)
from pipeline.common import REPLAY_STATE
from pipeline.storage import query_batches, query_events

TITLE = "E-Commerce Real-Time Analytics: Monitoring Click, View, and No Purchase Using Apache Kafka and Apache Spark"
PAGES = ["Dashboard", "Capture Data", "Event Stream", "Analytics", "Pipeline", "Database", "Visualization", "Tableau Exports"]

st.set_page_config(page_title="Monitoriza Click, View, No Purchase Web", page_icon="📊", layout="wide")
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
[data-testid="stMetric"]{background:linear-gradient(145deg,#181832,#12152b);border:1px solid #302b59;border-radius:15px;padding:15px 17px;min-height:112px;box-shadow:0 8px 24px #08091455}
[data-testid="stMetricLabel"]{color:#aaa9c5!important;font-size:.78rem;letter-spacing:.02em}
[data-testid="stMetricValue"]{color:#f6f4ff;font-size:1.75rem;font-weight:720}
[data-testid="stMetricDelta"]{font-size:.76rem}
[data-testid="stVerticalBlockBorderWrapper"]{background:linear-gradient(145deg,#15172d,#111326);border:1px solid #302b50;border-radius:16px;box-shadow:0 8px 26px #08091430}
.stButton>button,[data-testid="stDownloadButton"] button{border-radius:10px;border:1px solid #7c5cf3;background:#7254ea;color:#fff;font-weight:650;transition:.18s ease}
.stButton>button:hover,[data-testid="stDownloadButton"] button:hover{background:#8869ff;color:#fff;border-color:#9a82ff;transform:translateY(-1px)}
[data-testid="stRadio"] [role="radiogroup"]{gap:.32rem;background:transparent;border:0;padding:.2rem}
[data-testid="stRadio"] label{padding:.62rem .7rem;border-radius:9px;color:#aeadc5!important}
[data-testid="stRadio"] label:has(input:checked){background:linear-gradient(95deg,#5938ba,#352467);color:#fff!important;box-shadow:0 4px 16px #613bea40}
[data-testid="stDataFrame"]{border:1px solid #302f48;border-radius:10px;overflow:hidden}
.dark-table-wrap{max-width:100%;overflow:auto;border:1px solid #302b50;border-radius:12px;background:#101225;scrollbar-color:#53477e #111326}
.dark-app-table{width:100%;border-collapse:separate;border-spacing:0;background:#101225;color:#e8e6f5;font-size:.82rem}
.dark-app-table thead th{position:sticky;top:0;z-index:1;padding:.72rem .8rem;background:#19172e;color:#cfc4ff;text-align:left;font-weight:700;border-bottom:1px solid #393253;white-space:nowrap}
.dark-app-table tbody td{padding:.66rem .8rem;background:#101225;color:#e4e3ef;border-bottom:1px solid #27283e;white-space:nowrap}
.dark-app-table tbody tr:nth-child(even) td{background:#14162a}
.dark-app-table tbody tr:hover td{background:#201b3a}
.dark-table-empty{padding:1rem;color:#aaa9c5;border:1px solid #302b50;border-radius:12px;background:#101225}
.small-note{color:#aaa9c5;font-size:.82rem}
.brand{color:#b59bff;font-weight:850;letter-spacing:.06em;font-size:1.02rem}
.subhead{color:#aaa9c5;margin-top:-.3rem}
.eyebrow{color:#9b8be2;font-size:.7rem;font-weight:750;letter-spacing:.14em;text-transform:uppercase}
.page-title{font-size:1.95rem;font-weight:780;letter-spacing:-.045em;margin:.2rem 0 .15rem;color:#f5f3ff}
.page-subtitle{font-size:.88rem;color:#a5a4bf;margin-bottom:1.2rem}
.mode-pill{display:inline-block;border:1px solid #493d74;background:#211d39;color:#c7b8ff;border-radius:999px;padding:.38rem .72rem;font-size:.74rem;font-weight:650}
.side-brand{font-size:1.25rem;font-weight:850;letter-spacing:-.04em;color:#faf8ff;margin:.25rem 0 0}
.side-sub{font-size:.68rem;color:#9998b5;line-height:1.5;margin:.2rem 0 1.3rem}
.side-status{border:1px solid #2d2d46;border-radius:12px;background:#151625;padding:.7rem .75rem;margin:.65rem 0;color:#c2c0da;font-size:.76rem}
[data-testid="stAlert"]{border-radius:11px;background:#1a1929}
[data-testid="stVerticalBlockBorderWrapper"] h3,[data-testid="stVerticalBlockBorderWrapper"] h4{color:#eeeaff}
[data-testid="stVerticalBlockBorderWrapper"] [data-testid="stCaptionContainer"]{color:#9594b0}
[data-testid="stPlotlyChart"]{border-radius:12px}
hr{border-color:#2b2a42}
@media(max-width:800px){.block-container{padding:.8rem 1rem 2rem}.page-title{font-size:1.55rem}}
.viz-heading{padding:1.05rem 1.2rem;border:1px solid #31364d;border-radius:16px;background:linear-gradient(115deg,#1b2637,#172030 58%,#20243b);margin:.6rem 0 1rem}
.viz-kicker{font-size:.68rem;letter-spacing:.16em;text-transform:uppercase;color:#72d7d5;font-weight:800}
.viz-heading h2{font-size:1.4rem;margin:.22rem 0;color:#f4f5ff}
.viz-heading p{margin:0;color:#aab5c8;font-size:.82rem}
.viz-note{color:#9ba9bd;font-size:.76rem;padding:.1rem .15rem .25rem}
.flow-panel{position:relative;overflow:hidden;margin:.45rem 0 1.1rem;padding:1rem 1.2rem .9rem;border:1px solid #343550;border-radius:16px;background:radial-gradient(ellipse at 18% 0%,#242543 0,transparent 52%),linear-gradient(115deg,#171a2d,#141725 72%,#19172d);box-shadow:0 12px 36px #08091445}
.flow-panel:before{content:"";position:absolute;inset:-80% -30%;pointer-events:none;background:linear-gradient(105deg,transparent 44%,#aaa0ff08 50%,transparent 56%);animation:flow-sheen 9s linear infinite}
.flow-head{display:flex;justify-content:space-between;align-items:center;gap:1rem;position:relative;z-index:1}
.flow-title{font-size:.72rem;letter-spacing:.14em;text-transform:uppercase;color:#aaa7c8;font-weight:800}
.flow-live{font-size:.7rem;font-weight:750;letter-spacing:.06em;color:#80e2cf;display:flex;align-items:center;gap:.45rem}
.flow-live:before{content:"";width:7px;height:7px;border-radius:50%;background:#5ce0bd;box-shadow:0 0 0 0 #5ce0bd70;animation:flow-pulse 1.7s infinite}
.flow-live.idle{color:#aaa9c5}.flow-live.idle:before{background:#77758f;box-shadow:none;animation:none}
.flow-nodes{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:.7rem;position:relative;z-index:1;margin-top:.9rem}
.flow-node{min-width:0;padding:.68rem .78rem;border:1px solid #33344d;border-radius:12px;background:#111423c9;transition:transform .2s ease,border-color .2s ease,box-shadow .2s ease}
.flow-node:hover{transform:translateY(-3px);border-color:#7163b3;box-shadow:0 8px 22px #05061080}
.flow-node-top{display:flex;align-items:center;gap:.48rem;color:#aaa9c5;font-size:.68rem;font-weight:700;text-transform:uppercase;letter-spacing:.07em}
.flow-node-dot{width:8px;height:8px;flex:0 0 auto;border-radius:50%;background:#77758f}
.flow-node-dot.good{background:#5ce0bd;box-shadow:0 0 12px #5ce0bd75}.flow-node-dot.busy{background:#b19aff;box-shadow:0 0 12px #9879ff90;animation:flow-pulse 1.35s infinite}.flow-node-dot.bad{background:#ff7187;box-shadow:0 0 10px #ff718777}
.flow-node-value{margin-top:.4rem;color:#f1efff;font-size:1.12rem;font-weight:760;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.flow-node-note{margin-top:.12rem;color:#8888a6;font-size:.68rem;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.flow-track{height:3px;margin:.88rem .12rem .55rem;position:relative;z-index:1;overflow:hidden;border-radius:99px;background:#2a2b41}
.flow-track:after{content:"";position:absolute;inset:0 auto 0 0;width:var(--flow-progress,0%);border-radius:99px;background:linear-gradient(90deg,#5ecbd0,#9a7cff,#61dfc1);transition:width .8s ease}
.flow-particle{position:absolute;z-index:2;top:50%;left:-8px;width:6px;height:6px;border-radius:50%;transform:translateY(-50%);background:#e5dcff;box-shadow:0 0 12px 3px #9c83ffb0;opacity:0}
.flow-track.active .flow-particle{opacity:1;animation:flow-travel 2.7s linear infinite}.flow-track.active .flow-particle:nth-child(2){animation-delay:.65s}.flow-track.active .flow-particle:nth-child(3){animation-delay:1.3s}.flow-track.active .flow-particle:nth-child(4){animation-delay:1.95s}
.flow-foot{position:relative;z-index:1;display:flex;justify-content:space-between;gap:.8rem;flex-wrap:wrap;color:#9695b2;font-size:.7rem}
.dash-hero{display:flex;justify-content:space-between;align-items:flex-end;gap:1rem;margin:.25rem 0 1rem;padding:.2rem 0 .75rem;border-bottom:1px solid #262541}
.dash-hero h2{font-size:1.55rem!important;margin:0;color:#f3f0ff}.dash-hero p{margin:.22rem 0 0;color:#9695b5;font-size:.78rem}
.dash-kpi{height:100%;min-height:112px;position:relative;overflow:hidden;padding:.82rem .88rem;border:1px solid #302b59;border-radius:13px;background:linear-gradient(145deg,#171a38,#11152b 74%);box-shadow:0 8px 24px #06071966;transition:transform .2s ease,border-color .2s ease,box-shadow .2s ease}
.dash-kpi:after{content:"";position:absolute;top:0;left:0;width:100%;height:2px;background:linear-gradient(90deg,transparent,var(--accent,#a58bff),transparent);opacity:.9}
.dash-kpi:hover{transform:translateY(-3px);border-color:#7060b6;box-shadow:0 12px 28px #07071999}
.dash-kpi-top{display:flex;align-items:center;justify-content:space-between;gap:.5rem;color:#aaa9c7;font-size:.68rem;font-weight:650}
.dash-kpi-icon{display:grid;place-items:center;width:27px;height:27px;border-radius:8px;background:color-mix(in srgb,var(--accent,#a58bff) 18%,transparent);color:var(--accent,#a58bff);font-size:.85rem}
.dash-kpi-value{margin-top:.48rem;color:#f5f3ff;font-size:1.35rem;line-height:1.1;font-weight:780;letter-spacing:-.035em}
.dash-kpi-foot{margin-top:.25rem;color:#777995;font-size:.65rem}
.dash-panel{height:100%;padding:1rem 1rem .7rem;border:1px solid #2e2c50;border-radius:14px;background:linear-gradient(145deg,#15182e,#111426);box-shadow:0 8px 24px #07081755}
.dash-panel-title{display:flex;align-items:center;justify-content:space-between;gap:.6rem;margin:0 0 .12rem;color:#efedff;font-size:.92rem;font-weight:720}
.dash-panel-sub{margin:0 0 .6rem;color:#898aa9;font-size:.69rem}
.dash-tag{display:inline-block;padding:.2rem .48rem;border:1px solid #39335e;border-radius:999px;color:#b9a9ff;background:#211d3d;font-size:.62rem;white-space:nowrap}
.dash-status-list{display:grid;gap:.55rem;margin-top:.55rem}
.dash-status-row{display:flex;align-items:center;justify-content:space-between;gap:.6rem;padding:.55rem .65rem;border:1px solid #282943;border-radius:9px;background:#101326}
.dash-status-name{color:#b4b3ce;font-size:.73rem}.dash-status-value{display:flex;align-items:center;gap:.36rem;color:#cfcae6;font-size:.68rem;font-weight:700;text-transform:uppercase}
.dash-status-dot{width:7px;height:7px;border-radius:50%;background:#77758f}.dash-status-dot.good{background:#53d8b2;box-shadow:0 0 9px #53d8b277}.dash-status-dot.bad{background:#ff7187;box-shadow:0 0 9px #ff718777}.dash-status-dot.busy{background:#a58bff;box-shadow:0 0 9px #a58bff88;animation:flow-pulse 1.5s infinite}
.dash-note{padding:.65rem .72rem;border:1px solid #4b3e24;border-radius:9px;background:#211c17;color:#e5c98f;font-size:.72rem;line-height:1.5}
.dash-quality{display:flex;gap:.45rem;flex-wrap:wrap;margin-top:.6rem}.dash-quality span{padding:.3rem .48rem;border:1px solid #2c2b49;border-radius:7px;color:#a5a4c2;background:#111426;font-size:.64rem}
.pipe-hero{display:flex;align-items:center;gap:.9rem;margin:.15rem 0 .9rem;padding:.2rem 0 .85rem;border-bottom:1px solid #302651}
.pipe-hero-icon{display:grid;place-items:center;width:48px;height:48px;border:1px solid #7046cf;border-radius:14px;background:linear-gradient(145deg,#351568,#17163c);color:#c996ff;font-size:1.55rem;box-shadow:0 0 24px #702cff30}
.pipe-hero h2{margin:0!important;font-size:1.55rem!important;color:#f5f2ff}.pipe-hero p{margin:.18rem 0 0;color:#aaa6c6;font-size:.76rem}
.pipe-shell{padding:1rem;border:1px solid #3c2c78;border-radius:15px;background:linear-gradient(145deg,#0d1028,#090d21);box-shadow:0 10px 32px #06071977}
.pipe-section-head{display:flex;justify-content:space-between;align-items:center;gap:.7rem;margin-bottom:.72rem;color:#f1eeff;font-size:.9rem;font-weight:740}
.pipe-operating{display:inline-flex;align-items:center;gap:.4rem;padding:.3rem .58rem;border:1px solid #174e55;border-radius:999px;background:#0c292e;color:#4ee0c1;font-size:.65rem;font-weight:750;text-transform:uppercase;letter-spacing:.04em}
.pipe-operating.warn{border-color:#635020;background:#2a2311;color:#f2c86e}.pipe-operating.off{border-color:#552838;background:#281520;color:#ff8aa0}
.pipe-stage{position:relative;min-height:122px;padding:.72rem .62rem .6rem;border:1px solid #39276c;border-radius:12px;background:linear-gradient(150deg,#171438,#0d142c 80%);box-shadow:inset 0 1px #ffffff08;transition:transform .18s ease,border-color .18s ease,box-shadow .18s ease}
.pipe-stage:hover{transform:translateY(-3px);border-color:#8c57ed;box-shadow:0 8px 22px #4817ae30}
.pipe-stage.good{border-color:#15555a}.pipe-stage.bad{border-color:#63263e}.pipe-stage.active{border-color:#6443b0;box-shadow:0 0 18px #772fff24;animation:pipe-glow 2s ease-in-out infinite}
.pipe-stage-title{color:#eeeaff;font-size:.72rem;font-weight:740;line-height:1.25}.pipe-stage-icon{display:grid;place-items:center;width:34px;height:34px;margin-bottom:.48rem;border:1px solid #6641b8;border-radius:9px;background:linear-gradient(145deg,#5725a9,#201a58);color:#e4c9ff;font-size:1.05rem}
.pipe-stage-note{margin-top:.25rem;color:#aaa6c8;font-size:.64rem;line-height:1.35;overflow-wrap:anywhere}.pipe-stage-status{position:absolute;right:.45rem;bottom:.42rem;width:13px;height:13px;border-radius:4px;background:#29c9ad;color:#071a20;text-align:center;font-size:.58rem;font-weight:900;line-height:13px}
.pipe-stage-status.bad{background:#e75275;color:#fff}.pipe-stage-status.neutral{background:#6e668b;color:#fff}
.pipe-arrow{height:100%;display:grid;place-items:center;color:#8dbdff;font-size:1.25rem;text-shadow:0 0 9px #51a6ff88}
.pipe-kpi{height:100%;min-height:106px;padding:.72rem .78rem;border:1px solid #412675;border-radius:12px;background:linear-gradient(150deg,#17133a,#0e142a);position:relative;overflow:hidden}
.pipe-kpi:before{content:"";position:absolute;left:0;top:0;bottom:0;width:3px;background:var(--accent,#a58bff)}
.pipe-kpi-label{color:#b3afd0;font-size:.65rem;line-height:1.3}.pipe-kpi-value{margin-top:.38rem;color:#f6f2ff;font-size:1.28rem;font-weight:780;letter-spacing:-.03em}.pipe-kpi-note{margin-top:.14rem;color:#8584a5;font-size:.62rem}
.pipe-mini-title{margin:0 0 .3rem;color:#efedff;font-size:.82rem;font-weight:720}.pipe-mini-note{margin:0 0 .5rem;color:#8e8ba9;font-size:.65rem}
.pipe-status-item{display:flex;justify-content:space-between;align-items:flex-start;gap:.6rem;padding:.56rem 0;border-bottom:1px solid #252442}.pipe-status-item:last-child{border-bottom:0}
.pipe-status-label{color:#b8b4d1;font-size:.68rem}.pipe-status-value{color:#e3def8;font-size:.67rem;font-weight:680;text-align:right;overflow-wrap:anywhere}
.pipe-log-row{display:grid;grid-template-columns:72px 1fr auto;gap:.5rem;padding:.5rem 0;border-bottom:1px solid #23213f;font-size:.65rem}.pipe-log-row:last-child{border-bottom:0}.pipe-log-time{color:#8582a5}.pipe-log-main{color:#d9d4f1}.pipe-log-tag{color:#62d9bd}
.pipe-architecture{display:flex;justify-content:space-between;align-items:center;gap:.25rem;flex-wrap:wrap;margin:.4rem 0 .8rem}
.pipe-architecture-node{flex:1 1 90px;min-width:76px;text-align:center;color:#d7d0f2;font-size:.59rem;line-height:1.25}
.pipe-architecture-icon{display:grid;place-items:center;width:38px;height:38px;margin:0 auto .28rem;border:1px solid #633ac1;border-radius:10px;background:#21174b;color:#d6afff;font-size:1.05rem}
.pipe-architecture-arrow{color:#93bbff;font-size:.9rem}
@keyframes pipe-glow{0%,100%{box-shadow:0 0 9px #772fff18}50%{box-shadow:0 0 20px #772fff42}}
@keyframes flow-pulse{0%{box-shadow:0 0 0 0 #5ce0bd60}70%{box-shadow:0 0 0 7px #5ce0bd00}100%{box-shadow:0 0 0 0 #5ce0bd00}}
@keyframes flow-travel{0%{left:0;opacity:0}8%{opacity:1}88%{opacity:1}100%{left:100%;opacity:0}}
@keyframes flow-sheen{0%{transform:translateX(-35%)}100%{transform:translateX(35%)}}
@media(max-width:700px){.flow-nodes{grid-template-columns:repeat(2,minmax(0,1fr))}.flow-head{align-items:flex-start;flex-direction:column}}
</style>
""", unsafe_allow_html=True)

page_icons = {"Dashboard":"▦  Dashboard", "Capture Data":"⇧  Data Ingestion", "Event Stream":"≋  Preprocessing",
              "Analytics":"◒  Transaction Analytics", "Pipeline":"⌘  Kafka & Spark", "Visualization":"▤  Product Analytics",
              "Database":"▤  Database", "Tableau Exports":"⇩  Reports"}
st.sidebar.markdown('<div class="side-brand">E-Commerce Monitor</div><div class="side-sub">RETAILROCKET ANALYTICS<br>Apache Kafka + Apache Spark</div>', unsafe_allow_html=True)
page = st.sidebar.radio("WORKSPACE", PAGES, format_func=lambda value: page_icons.get(value, value), label_visibility="visible", key="main_page")
st.sidebar.markdown('<div class="side-status">◉ &nbsp; Historical Event Replay<br><span style="color:#87869f">Real-Time Simulation · Retailrocket</span></div>', unsafe_allow_html=True)
st.sidebar.caption("Click event: unavailable in source data")
st.markdown(f'<div class="eyebrow" style="letter-spacing:.035em">{html.escape(TITLE)}</div><div class="page-subtitle">Historical Retailrocket behavior data &nbsp; <span class="mode-pill">Historical Event Replay / Real-Time Simulation</span></div>', unsafe_allow_html=True)


def fmt(value):
    try:
        return f"{int(value or 0):,}"
    except (TypeError, ValueError):
        return "0"


def dataframe(rows):
    return pd.DataFrame(rows) if rows else pd.DataFrame()


def render_dark_table(data, height=560, min_width=720):
    """Render a consistently dark, scrollable table across every dashboard page."""
    frame = data if isinstance(data, pd.DataFrame) else pd.DataFrame(data)
    if frame.empty:
        st.markdown('<div class="dark-table-empty">No rows available.</div>', unsafe_allow_html=True)
        return
    table = frame.to_html(index=False, escape=True, classes="dark-app-table", border=0)
    st.markdown(
        f'<div class="dark-table-wrap" style="max-height:{int(height)}px">'
        f'<div style="min-width:{int(min_width)}px">{table}</div></div>',
        unsafe_allow_html=True,
    )


def capture_monitor_figure(events):
    """Build a cumulative Kafka/Spark chart from real per-event timestamps."""
    buckets={}
    seen=set()
    for event in events:
        event_id=event.get("event_id")
        if not event_id or event_id in seen:
            continue
        seen.add(event_id)
        for field,series in (("kafka_acknowledged_at","Kafka acknowledged"),
                             ("spark_processed_at","Spark processed")):
            raw=event.get(field)
            if not raw:
                continue
            timestamp=pd.to_datetime(raw,utc=True,errors="coerce")
            if pd.isna(timestamp):
                continue
            # Group adjacent real arrivals into two-second buckets for a
            # readable trend while retaining the source timestamps.
            bucket=int(timestamp.timestamp()//2)*2
            slot=buckets.setdefault(bucket,{"Kafka acknowledged":0,"Spark processed":0})
            slot[series]+=1
    if not buckets:
        return None
    running={"Kafka acknowledged":0,"Spark processed":0}
    rows=[]
    for bucket,counts in sorted(buckets.items()):
        for series in running:
            running[series]+=counts[series]
            rows.append({"Time":pd.to_datetime(bucket,unit="s",utc=True),
                         "Stage":series,"Events":running[series]})
    fig=px.line(pd.DataFrame(rows),x="Time",y="Events",color="Stage",markers=True,
                color_discrete_map={"Kafka acknowledged":"#42c9df","Spark processed":"#ad7bff"})
    fig.update_traces(line={"shape":"hv","width":3},marker={"size":5},
                      hovertemplate="%{x|%H:%M:%S UTC}<br>%{fullData.name}: %{y:,} events<extra></extra>")
    fig.update_layout(template="plotly_dark",paper_bgcolor="rgba(0,0,0,0)",plot_bgcolor="rgba(0,0,0,0)",
                      height=320,margin=dict(l=10,r=15,t=10,b=10),legend_title_text="",
                      xaxis_title="Actual pipeline timestamp · 2-second buckets",yaxis_title="Cumulative events")
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(gridcolor="#29264a",rangemode="tozero")
    return fig


def status_label(component):
    value = component.get("status", "unknown") if isinstance(component, dict) else str(component)
    if value in ("running", "reachable", "completed"):
        st.success(f"{value.upper()}")
    elif value in ("idle", "ready"):
        st.info(f"{value.upper()}")
    elif value in ("stopped", "unreachable", "failed", "error", "stale"):
        st.error(f"{value.upper()}")
    else:
        st.info(value.upper())


def daily_chart(data):
    rows = data.get("daily_trend", [])
    if not rows:
        st.info("Belum ada event yang berhasil diproses Spark untuk membentuk tren harian.")
        return
    df = pd.DataFrame(rows).rename(columns={"date": "Event date", "views": "Views", "addtocart": "Add to cart", "transactions": "Transactions"})
    long = df.melt(id_vars="Event date", var_name="Event type", value_name="Events")
    fig = px.line(long, x="Event date", y="Events", color="Event type", markers=True,
                  color_discrete_map={"Views":"#a58bff", "Add to cart":"#25c9c3", "Transactions":"#ff9d55"})
    fig.update_layout(template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                      margin=dict(l=12,r=12,t=10,b=10), height=330, legend_title_text="",
                      xaxis_title="Historical event date", yaxis_title="Processed event count", hovermode="x unified")
    fig.update_xaxes(showgrid=False); fig.update_yaxes(gridcolor="#273b5a", rangemode="tozero")
    st.plotly_chart(fig, width="stretch", config={"displayModeBar":False})


def product_chart(products):
    if not products:
        st.info("Belum ada agregasi produk di output Spark.")
        return
    rows = products[:10]
    df = pd.DataFrame(rows).rename(columns={"item_id":"Product ID", "views":"Views", "cart_additions":"Add to cart", "transactions":"Transactions"})
    df["Product ID"] = df["Product ID"].map(lambda x: f"#{x}")
    long = df.melt(id_vars="Product ID", value_vars=["Views","Add to cart","Transactions"], var_name="Event type", value_name="Event count")
    fig = px.bar(long, x="Product ID", y="Event count", color="Event type", barmode="group",
                 color_discrete_map={"Views":"#9b7bff", "Add to cart":"#27c8c0", "Transactions":"#ff9d55"},
                 hover_data={"Product ID":True,"Event type":True,"Event count":":,"})
    fig.update_layout(template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                      margin=dict(l=10,r=10,t=15,b=10), height=420, legend_title_text="", yaxis_title="Actual processed events", xaxis_title="Retailrocket Product ID")
    fig.update_xaxes(showgrid=False); fig.update_yaxes(gridcolor="#273b5a", rangemode="tozero")
    st.plotly_chart(fig, width="stretch", config={"displayModeBar":False})


def purchase_funnel(data, height=290):
    rows = data.get("funnel", [])
    if not rows:
        st.info("Belum ada event funnel yang diproses Spark.")
        return
    labels = [row.get("name", "") for row in rows]
    values = [int(row.get("count", 0) or 0) for row in rows]
    fig = go.Figure(go.Funnel(
        y=labels,
        x=values,
        textinfo="value+percent initial",
        textposition="auto",
        textfont={"size": 13, "color": "#fbfaff"},
        marker={"color": ["#9b7bff", "#32c9c1", "#ff9d63"],
                "line": {"color": "#25243a", "width": 1}},
        connector={"line": {"color": "#4a4668", "width": 1, "dash": "dot"}},
        opacity=.96,
        hovertemplate="<b>%{y}</b><br>%{x:,.0f} events<extra></extra>"
    ))
    fig.update_layout(
        template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=8, r=8, t=14, b=8), height=height,
        font={"family": "Inter, Segoe UI, sans-serif", "color": "#d9d6ec", "size": 12},
        xaxis={"visible": False, "fixedrange": True},
        yaxis={"title": "", "showgrid": False, "zeroline": False, "tickfont": {"color": "#c4c0dc", "size": 12}},
        showlegend=False
    )
    st.plotly_chart(fig, width="stretch", config={"displayModeBar":False, "responsive":True})


def render_pipeline_animation(data):
    """Show a moving pipeline trace only while the replay and both services are actually active."""
    pipeline=data.get("pipeline",{})
    replay=pipeline.get("replay",{})
    kafka=pipeline.get("kafka",{})
    spark=pipeline.get("spark",{})
    summary=data.get("capture_summary",{})
    active=(replay.get("status")=="running" and kafka.get("status")=="reachable" and spark.get("status")=="running")
    requested=int(summary.get("requested",0) or 0)
    acknowledged=int(summary.get("kafka_acknowledged",0) or 0)
    processed=int(summary.get("spark_processed",0) or 0)
    waiting=int(summary.get("waiting_for_spark",0) or 0)
    progress=(min(100,max(0,processed/requested*100)) if requested else 0)

    def state_class(status, busy=False):
        if status in ("running","reachable","completed"):
            return "busy" if busy else "good"
        if status in ("failed","error","stopped","unreachable","stale"):
            return "bad"
        return ""

    nodes=[
        ("Retailrocket replay",replay.get("status","not_started"),f"{fmt(requested)} requested",state_class(replay.get("status"),active)),
        ("Kafka · retailrocket-events",kafka.get("status","unknown"),f"{fmt(acknowledged)} acknowledged",state_class(kafka.get("status"),active)),
        ("Spark Structured Streaming",spark.get("status","unknown"),f"{fmt(processed)} written · {fmt(waiting)} waiting",state_class(spark.get("status"),active)),
        ("Dashboard analytics","updated" if int(data.get("overview",{}).get("total_events",0) or 0)>0 else "waiting",
         f"{fmt(data.get('overview',{}).get('total_events',0))} processed events",
         "good" if int(data.get("overview",{}).get("total_events",0) or 0)>0 else "")]
    cards=[]
    for label,status,note,dot in nodes:
        cards.append(f'<div class="flow-node"><div class="flow-node-top"><span class="flow-node-dot {dot}"></span>{html.escape(str(label))}</div><div class="flow-node-value">{html.escape(str(status).upper())}</div><div class="flow-node-note">{html.escape(str(note))}</div></div>')
    mode="ACTIVE · REPLAYING" if active else "STANDBY · VERIFIED STATUS"
    live_class="flow-live" if active else "flow-live idle"
    panel=(f'<section class="flow-panel"><div class="flow-head"><span class="flow-title">Event flow · live component state</span>'
        f'<span class="{live_class}">{mode}</span></div><div class="flow-nodes">{"".join(cards)}</div>'
        f'<div class="flow-track {"active" if active else ""}" style="--flow-progress:{progress:.1f}%">'
        '<i class="flow-particle"></i><i class="flow-particle"></i><i class="flow-particle"></i><i class="flow-particle"></i></div>'
        f'<div class="flow-foot"><span>Kafka acknowledgements and Spark writes come from pipeline outputs.</span>'
        f'<span>Capture progress: {fmt(processed)} / {fmt(requested) if requested else "no active request"}</span></div></section>')
    st.markdown(panel,unsafe_allow_html=True)


def render_dashboard(data):
    o=data["overview"]
    st.markdown('<div class="dash-hero"><div><h2>Dashboard Overview</h2><p>E-commerce behavior monitoring · processed Retailrocket event data</p></div><span class="dash-tag">Historical Event Replay / Real-Time Simulation</span></div>',unsafe_allow_html=True)

    # KPI values are all measured in the source/Spark output; unavailable business metrics are intentionally omitted.
    cards=[
        ("Processed events",o["total_events"],"Spark committed output","▦","#a58bff"),
        ("Product views",o["views"],"Original view events","◉","#36c9d0"),
        ("Unique visitors",o["unique_visitors"],"Distinct visitor IDs","♙","#59cda8"),
        ("No-purchase sessions",o["no_purchase_sessions"],f'{o["no_purchase_percent"]:.1f}% of eligible sessions',"↘","#ff987b"),
    ]
    cols=st.columns(4,gap="small")
    for col,(label,value,foot,icon,accent) in zip(cols,cards):
        col.markdown(f'<div class="dash-kpi" style="--accent:{accent}"><div class="dash-kpi-top"><span>{html.escape(label)}</span><span class="dash-kpi-icon">{icon}</span></div><div class="dash-kpi-value">{fmt(value)}</div><div class="dash-kpi-foot">{html.escape(foot)}</div></div>',unsafe_allow_html=True)

    st.markdown("")
    left,right=st.columns([1.75,1],gap="small")
    with left:
        with st.container(border=True):
            st.markdown('<div class="dash-panel-title">Event activity trend <span class="dash-tag">Original event date</span></div><p class="dash-panel-sub">Views, cart additions and transactions processed by Spark</p>',unsafe_allow_html=True)
            daily_chart(data)
    with right:
        with st.container(border=True):
            st.markdown('<div class="dash-panel-title">Event distribution <span class="dash-tag">Source event counts</span></div><p class="dash-panel-sub">Repeated actions count as separate events</p>',unsafe_allow_html=True)
            counts=[int(o.get("views",0)),int(o.get("addtocart",0)),int(o.get("transactions",0))]
            if sum(counts):
                fig=go.Figure(go.Pie(labels=["Product views","Add to cart","Transactions"],values=counts,hole=.68,sort=False,
                    marker={"colors":["#36c9d0","#a58bff","#ff987b"],"line":{"color":"#17182d","width":3}},
                    textinfo="label+percent",textposition="outside",hovertemplate="%{label}<br>%{value:,} events · %{percent}<extra></extra>"))
                fig.update_layout(template="plotly_dark",paper_bgcolor="rgba(0,0,0,0)",plot_bgcolor="rgba(0,0,0,0)",
                    margin=dict(l=8,r=8,t=12,b=8),height=310,showlegend=False,
                    annotations=[dict(text=f"{fmt(sum(counts))}<br><span style='font-size:10px'>events</span>",x=.5,y=.5,showarrow=False,font=dict(size=16,color="#f4f0ff"))])
                st.plotly_chart(fig,width="stretch",config={"displayModeBar":False,"responsive":True})
            else: st.info("Event distribution will appear after Spark processes events.")

    st.markdown("")
    left,right=st.columns([1.45,1],gap="small")
    with left:
        with st.container(border=True):
            st.markdown('<div class="dash-panel-title">Popular products <span class="dash-tag">Top item IDs</span></div><p class="dash-panel-sub">Actual view, cart-addition, and transaction-associated counts · product names are unavailable</p>',unsafe_allow_html=True)
            product_rows=data.get("products",[])[:8]
            if product_rows:
                pdf=pd.DataFrame(product_rows).rename(columns={"item_id":"Product ID","views":"Views","cart_additions":"Add to cart","transactions":"Transactions"})
                for column in ("Views","Add to cart","Transactions"):
                    if column not in pdf: pdf[column]=0
                pdf["Total events"]=pdf[["Views","Add to cart","Transactions"]].sum(axis=1)
                pdf=pdf[["Product ID","Views","Add to cart","Transactions","Total events"]]
                render_dark_table(pdf,height=320,min_width=650)
            else:
                st.info("Product rankings appear after Spark produces item-level aggregations.")
    with right:
        with st.container(border=True):
            st.markdown('<div class="dash-panel-title">Purchase funnel <span class="dash-tag">Event totals</span></div><p class="dash-panel-sub">Repeated event counts; not unique-user conversion</p>',unsafe_allow_html=True)
            purchase_funnel(data,height=300)

    st.markdown("")
    left,middle,right=st.columns([1,1,1.2],gap="small")
    with left:
        with st.container(border=True):
            st.markdown('<div class="dash-panel-title">Session outcomes</div><p class="dash-panel-sub">30-minute inactivity session rule</p>',unsafe_allow_html=True)
            pct=float(o.get("no_purchase_percent",0))
            eligible=int(o.get("eligible_sessions",0)); no_purchase=int(o.get("no_purchase_sessions",0))
            if eligible:
                fig=go.Figure(go.Pie(labels=["Without recorded purchase","Transaction in session"],values=[no_purchase,max(0,eligible-no_purchase)],hole=.7,sort=False,
                    marker={"colors":["#ff987b","#59cda8"],"line":{"color":"#17182d","width":3}},
                    hovertemplate="%{label}<br>%{value:,} sessions · %{percent}<extra></extra>",textinfo="none"))
                fig.update_layout(template="plotly_dark",paper_bgcolor="rgba(0,0,0,0)",plot_bgcolor="rgba(0,0,0,0)",
                    margin=dict(l=8,r=8,t=10,b=2),height=205,showlegend=True,legend=dict(orientation="h",y=-.08,font=dict(size=9)),
                    annotations=[dict(text=f"{pct:.1f}%",x=.5,y=.5,showarrow=False,font=dict(size=22,color="#f4f0ff"))])
                st.plotly_chart(fig,width="stretch",config={"displayModeBar":False,"responsive":True})
                st.caption(f"{fmt(no_purchase)} of {fmt(eligible)} eligible sessions had no transaction recorded within that session.")
            else:
                st.info("Session outcomes appear after eligible sessions are processed.")
    with middle:
        with st.container(border=True):
            st.markdown('<div class="dash-panel-title">Click monitoring</div><p class="dash-panel-sub">Source availability</p>',unsafe_allow_html=True)
            st.markdown('<div class="dash-note"><b>Unavailable in Retailrocket.</b><br>This dataset has no separate genuine click event. Product views remain labeled as views.</div>',unsafe_allow_html=True)
            st.markdown(f'<div class="dash-quality"><span>{fmt(o["views"])} product views</span><span>{fmt(o["unique_visitors"])} visitors</span><span>{fmt(o["unique_products"])} products</span></div>',unsafe_allow_html=True)
    with right:
        with st.container(border=True):
            h=data["pipeline"]
            st.markdown('<div class="dash-panel-title">Big Data pipeline</div><p class="dash-panel-sub">Verified service and replay state</p>',unsafe_allow_html=True)
            replay=h.get("replay",{}); kafka=h.get("kafka",{}); spark=h.get("spark",{})
            rows=[("Kafka broker",kafka.get("status","unknown")),("Spark processor",spark.get("status","unknown")),("Historical replay",replay.get("status","not_started")),("Click data","unavailable")]
            html_rows=[]
            for name,state in rows:
                dot="good" if state in ("running","reachable","completed") else "bad" if state in ("stopped","unreachable","failed","error","stale") else "busy" if state=="starting" else ""
                html_rows.append(f'<div class="dash-status-row"><span class="dash-status-name">{html.escape(name)}</span><span class="dash-status-value"><i class="dash-status-dot {dot}"></i>{html.escape(str(state))}</span></div>')
            st.markdown(f'<div class="dash-status-list">{"".join(html_rows)}</div>',unsafe_allow_html=True)
            st.caption(f"Topic · {html.escape(str(replay.get('topic','retailrocket-events')))} · Published {fmt(replay.get('published',0))} · Spark batch {html.escape(str(spark.get('last_batch_id','—')))}")

    q=data.get("data_quality",{})
    st.markdown("")
    published_total=int(h.get("replay",{}).get("published",0) or 0)
    source_records=q.get("source_records")
    source_records_label=fmt(source_records) if source_records is not None else ("Auditing…" if q.get("source_audit_status")=="scanning" else "—")
    st.markdown(f'<div class="dash-quality"><span>Source records · {source_records_label}</span><span>Valid processed · {fmt(q.get("valid_records_processed"))}</span><span>Invalid · {fmt(q.get("invalid_records"))}</span><span>Duplicates · {fmt(q.get("duplicates"))}</span><span>Kafka published · {fmt(published_total)}</span></div>',unsafe_allow_html=True)
    st.caption("Historical Retailrocket replay · auto-refresh runs only while a replay or Spark batch is active · product IDs only · genuine click events and product titles are not present in the supplied source.")


def render_capture(data):
    st.subheader("Capture Data from Retailrocket")
    st.info("Historical Event Replay / Real-Time Simulation. Data is read from the existing dataset; the original files remain unchanged.")
    st.warning("Click events are not available. The producer sends original view, addtocart, and transaction events.")
    notice=st.session_state.pop("capture_started_notice", None)
    if notice:
        st.success(notice)
    with st.form("capture_request"):
        c1,c2,c3=st.columns([1,1,1])
        amount=c1.number_input("Events to request",min_value=1,max_value=100000,value=1000,step=100)
        rate=c2.number_input("Replay rate (events/second)",min_value=0.0,max_value=5000.0,value=10.0,step=10.0,key="capture_replay_rate_v2",help="0 means no delay. Use a modest rate if Spark is still processing previous batches.")
        submitted=c3.form_submit_button("Request Data",type="primary",width="stretch")
    if submitted:
        result,code=request_capture(amount,rate)
        if code==202:
            st.session_state["capture_target"]=result["target_total"]
            st.session_state["capture_started_notice"]=f"Replay started: {fmt(result['requested'])} events requested; target total {fmt(result['target_total'])}."
            st.session_state["force_event_poll"]=True
            st.rerun(scope="app")
        else:
            st.error(result.get("error","Capture request failed."))
            if result.get("kafka"): st.json(result["kafka"])
    rp=data["pipeline"].get("replay",{})
    spark= data["pipeline"].get("spark",{})
    summary=data.get("capture_summary",{})
    if not rp.get("capture_id") and not rp.get("requested"):
        st.session_state.pop("capture_target", None)
    requested=int(summary.get("requested") or st.session_state.get("capture_target",0) or 0)
    acknowledged=int(summary.get("kafka_acknowledged",0) or 0)
    processed=int(summary.get("spark_processed",0) or 0)
    waiting=int(summary.get("waiting_for_spark",0) or 0)
    if rp.get("status")=="error":
        st.error(f"Replay stopped after {fmt(acknowledged)} of {fmt(requested)} requested acknowledgements. "
                 f"{rp.get('error', 'The producer stopped unexpectedly.')}")
        remaining=max(0,requested-acknowledged)
        if remaining and st.button(f"Resume remaining {fmt(remaining)} events",type="primary"):
            result,code=request_capture(remaining,rate,resume=True)
            if code==202:
                st.session_state["capture_target"]=result["requested"]
                st.session_state["capture_started_notice"]=(
                    f"Replay resumed: sending the remaining {fmt(result['remaining'])} events "
                    f"for the original {fmt(result['requested'])}-event request.")
                st.session_state["force_event_poll"]=True
                st.rerun(scope="app")
            else:
                st.error(result.get("error","Could not resume the replay."))

    st.markdown("#### Live pipeline trace")
    st.caption("Follow each request from the historical source, through Kafka acknowledgement, to Spark preprocessing and output.")
    if not summary.get("capture_id"):
        st.info("The currently displayed replay predates per-event trace recording. The next Request Data action will create a trace with Kafka acknowledgements and Spark batch details.")
    stages=st.columns(4,gap="small")
    stage_info=[
        ("01 · RETAILROCKET",f"{fmt(requested)} requested", "Source events"),
        ("02 · KAFKA TOPIC",f"{fmt(acknowledged)} acknowledged", "Broker confirms each message"),
        ("03 · SPARK STREAM",f"{fmt(processed)} written", "Matched by event ID in Spark output"),
        ("04 · PREPROCESSING",f"{fmt(waiting)} waiting", spark.get("active_batch_status",spark.get("last_batch_status","waiting for events")))
    ]
    for col,(title,value,detail) in zip(stages,stage_info):
        with col.container(border=True):
            st.markdown(f"<div class='eyebrow'>{title}</div>",unsafe_allow_html=True)
            st.markdown(f"<div style='font-size:1.28rem;font-weight:750;color:#f5f2ff;margin:.35rem 0'>{value}</div>",unsafe_allow_html=True)
            st.caption(detail)
    if requested:
        st.progress(min(1.0, processed/requested), text=f"Spark has written {fmt(processed)} of {fmt(requested)} requested events")
    st.markdown("#### Live event arrival & processing")
    st.caption("Each line is calculated from this request's actual Kafka acknowledgement timestamps and matching Spark output timestamps. The chart shows the latest 2,000 traced events; totals above cover the complete request.")
    chart=capture_monitor_figure(data.get("capture_events",[]))
    if chart is not None:
        st.plotly_chart(chart,width="stretch",config={"displayModeBar":False,"responsive":True},key="capture_live_monitor")
    elif requested:
        st.info("Waiting for the first Kafka acknowledgement timestamp. The chart will populate as records arrive.")
    else:
        st.info("Request data to start the live Kafka and Spark monitoring chart.")
    st.caption(f"Replay: {rp.get('status','not started')} · Kafka: {data['pipeline'].get('kafka',{}).get('status','unknown')} · Spark: {spark.get('status','unknown')} · Last Spark batch: {spark.get('last_batch_id','—')}")
    if spark.get("active_batch_status") and spark.get("active_batch_status") != "idle":
        st.info(f"Spark batch {spark.get('active_batch_id','—')}: {spark.get('active_batch_status')} · Kafka rows received: {fmt(spark.get('active_batch_received',0))} · Validated: {fmt(spark.get('active_batch_schema_valid',0))}")
    elif spark.get("last_batch_id") is not None and spark.get("last_batch_received") is not None:
        with st.expander(f"Latest Spark preprocessing result · batch {spark.get('last_batch_id')}"):
            q1,q2,q3,q4,q5=st.columns(5)
            q1.metric("Kafka rows read",fmt(spark.get("last_batch_received",0)))
            q2.metric("Valid output",fmt(spark.get("last_batch_valid",0)))
            q3.metric("Invalid",fmt(spark.get("last_batch_invalid",0)))
            q4.metric("Duplicates ignored",fmt(spark.get("last_batch_duplicates",0)))
            q5.metric("Duration",f"{spark.get('last_batch_seconds','—')} s")
            st.caption(f"Spark completed this batch at {spark.get('last_successful_execution','time unavailable')}.")
    elif spark.get("last_batch_id") is not None:
        st.caption("Detailed per-batch preprocessing counts will appear after Spark completes a batch with the current trace-enabled processor.")

    st.markdown("#### Event-by-event evidence")
    st.caption("Kafka partition and offset come from the producer's broker acknowledgement. Spark status appears only after the matching event ID is found in a committed Spark output file. The table shows the latest 2,000 acknowledged events for this request.")
    rows=[]
    for e in data.get("capture_events",[]):
        rows.append({"Source event ID":e.get("event_id"),"Type":e.get("event_type"),"Historical event time":e.get("event_time"),
                     "Producer ingest time":e.get("ingested_at"),"Kafka":e.get("kafka_status"),
                     "Partition":e.get("kafka_partition"),"Offset":e.get("kafka_offset"),
                     "Spark":e.get("processing_status"),"Spark batch":e.get("spark_batch_id"),
                     "Spark processed at":e.get("spark_processed_at")})
    render_dark_table(dataframe(rows),height=560,min_width=1450)
    if data.get("capture_events"):
        event_ids=[event.get("event_id") for event in data["capture_events"] if event.get("event_id")]
        selected_id=st.selectbox("Inspect a source event",event_ids,key="capture_event_inspector")
        selected=next((event for event in data["capture_events"] if event.get("event_id")==selected_id),None)
        if selected:
            left,right=st.columns(2)
            with left:
                st.markdown("**Retailrocket payload**")
                st.json({key:selected.get(key) for key in ["event_id","event_type","event_time","event_time_ms","visitor_id","session_id","item_id","transaction_id","source","capture_id"]})
            with right:
                st.markdown("**Pipeline receipt and Spark result**")
                st.json({key:selected.get(key) for key in ["kafka_status","kafka_acknowledged_at","kafka_partition","kafka_offset","processing_status","spark_batch_id","spark_processed_at","preprocessing_status"]})


def render_stream(data):
    st.subheader("Real-Time Event Stream")
    st.caption("Historical event time, producer ingestion time, and Spark processing time are shown separately.")
    rows=[{"Event ID":e.get("event_id"),"Event type":e.get("type"),"Historical event time":e.get("event_time"),
           "Producer ingest time":e.get("ingested_at"),"Kafka partition":e.get("kafka_partition"),"Kafka offset":e.get("kafka_offset"),
           "Spark batch":e.get("spark_batch_id"),"Spark processed at":e.get("processed_at"),"Product ID":e.get("item_id"),
           "Preprocessing":e.get("preprocessing_status","valid"),"Status":e.get("status")} for e in data.get("events",[])]
    render_dark_table(dataframe(rows),height=560,min_width=1450)


def render_analytics(data):
    o=data["overview"]; q=data["data_quality"]
    st.markdown('<div class="eyebrow">ANALYTICS WORKSPACE · SPARK PROCESSED DATA</div>',unsafe_allow_html=True)
    st.subheader("Product & session intelligence")
    st.caption("Explore source event counts and session outcomes calculated from validated Spark outputs. These are historical Retailrocket events replayed through Kafka, not live store traffic.")

    summary=st.columns(5)
    for col,label,value in zip(summary,["Events processed","Products observed","Unique visitors","Eligible sessions","No-purchase rate"],
                               [o["total_events"],o["unique_products"],o["unique_visitors"],o["eligible_sessions"],f"{o['no_purchase_percent']:.2f}%"]):
        col.metric(label,fmt(value) if not isinstance(value,str) else value)

    left,right=st.columns([1.6,1],gap="medium")
    with left:
        with st.container(border=True):
            st.markdown("#### Product engagement comparison")
            st.caption("Top products ranked by total view, cart, and transaction-associated events. Hover for exact counts.")
            product_chart(data.get("products",[])[:10])
    with right:
        with st.container(border=True):
            st.markdown("#### Purchase funnel")
            st.caption("Event volumes by original event type; repeat actions are counted separately.")
            purchase_funnel(data,height=330)
            st.caption("Not a unique visitor conversion rate.")

    leaders=data.get("product_leaders",{})
    st.markdown("#### Category leaders")
    lead_cols=st.columns(3)
    for col,key,color in zip(lead_cols,["views","cart_additions","transactions"],["#9b7bff","#25c9c3","#ff9d63"]):
        leader=leaders.get(key,{})
        with col.container(border=True):
            st.markdown(f"<div class='eyebrow'>{leader.get('label',key)}</div>",unsafe_allow_html=True)
            st.markdown(f"<div style='font-size:1.55rem;font-weight:780;color:{color};margin:.4rem 0'>Product {leader.get('item_id') or '—'}</div>",unsafe_allow_html=True)
            st.caption(f"{fmt(leader.get('count',0))} matching source events")

    with st.container(border=True):
        st.markdown("#### Product detail table")
        st.caption("Retailrocket provides numeric item IDs rather than product titles. This table contains event counts from Spark-processed records.")
        rows=[{"Product ID":x["item_id"],"Views":x["views"],"Add to cart":x["cart_additions"],"Transactions":x["transactions"],"Total events":x["views"]+x["cart_additions"]+x["transactions"]} for x in data.get("products",[])]
        render_dark_table(dataframe(rows),height=360,min_width=650)

    st.markdown("")
    qleft,qright=st.columns([1.45,1],gap="medium")
    with qleft:
        with st.container(border=True):
            st.markdown("#### Data quality & processing coverage")
            st.caption("Counts are read from Spark output and pipeline quality metrics. Source records are the dataset size; processed records may be a replay subset.")
            qc=st.columns(4)
            for col,label,key in zip(qc,["Source records","Valid processed","Invalid","Duplicates ignored"],["source_records","valid_records_processed","invalid_records","duplicates"]):
                value=q.get(key)
                col.metric(label,fmt(value) if value is not None else ("Auditing…" if q.get("source_audit_status")=="scanning" else "—"))
            missing_tx=q.get("missing_transaction_id")
            st.caption(f"Source CSV audit: {q.get('source_audit_status','unknown')} · Missing transaction IDs: {fmt(missing_tx) if missing_tx is not None else 'pending'} · Counts are computed from the local source file and replayed subset.")
            source_types=q.get("source_event_types",{})
            if source_types:
                st.caption("Full source event types · " + " · ".join(f"{name}: {fmt(count)}" for name,count in sorted(source_types.items())))
            valid=int(q.get("valid_records_processed",0)); invalid=int(q.get("invalid_records",0))
            measured=valid+invalid
            if measured:
                fig=go.Figure([
                    go.Bar(x=[valid],y=["Spark validation"],orientation="h",name="Valid",marker_color="#25c9c3",
                           text=[f"Valid · {valid:,}"],textposition="inside",hovertemplate="%{x:,} valid records<extra></extra>"),
                    go.Bar(x=[invalid],y=["Spark validation"],orientation="h",name="Invalid",marker_color="#ff647c",
                           text=[f"Invalid · {invalid:,}"],textposition="inside",hovertemplate="%{x:,} invalid records<extra></extra>")])
                fig.update_layout(template="plotly_dark",paper_bgcolor="rgba(0,0,0,0)",plot_bgcolor="rgba(0,0,0,0)",
                    margin=dict(l=10,r=10,t=8,b=10),height=105,showlegend=False,barmode="stack",xaxis_title="Records checked",yaxis={"visible":False},
                    font={"color":"#e6e3f4"})
                fig.update_xaxes(gridcolor="#273b5a",rangemode="tozero")
                st.plotly_chart(fig,width="stretch",config={"displayModeBar":False,"responsive":True})
            else:
                st.info("Validation breakdown appears after Spark records batch-level valid and invalid counts.")
    with qright:
        with st.container(border=True):
            st.markdown("#### Session classification")
            st.metric("Sessions without recorded purchase",fmt(o["no_purchase_sessions"]),f"{o['no_purchase_percent']:.2f}% of eligible sessions")
            st.progress(min(1.0,max(0.0,float(o.get("no_purchase_percent",0))/100)),text="Share of eligible sessions without a transaction")
            st.caption("A session is a visitor's events separated from the next event by more than 30 minutes. Eligible sessions contain a view or add-to-cart. No purchase means no transaction recorded in that session; it does not mean the visitor never purchased.")
            with st.expander("Interpretation & data limitations"):
                st.markdown("- **Counting unit:** event totals count repeated actions; session metrics count visitor sessions.\n- **Session boundary:** a new session begins after more than 30 minutes of inactivity.\n- **Purchase status:** a transaction must occur within that session to count as purchased.\n- **Product names:** the source has item IDs, not product titles.\n- **Click events:** no separate genuine click event exists in this dataset.")


def render_pipeline(data):
    h=data["pipeline"]
    q=data["data_quality"]
    replay=h.get("replay",{})
    kafka=h.get("kafka",{})
    spark=h.get("spark",{})
    reconciliation=data.get("reconciliation",{})
    overview=data["overview"]
    capture_health=health_data()
    producer_active=bool(capture_health.get("capture_process_running"))
    producer_state="running" if producer_active else replay.get("status","idle")
    source_state="available" if DATA.is_file() else "missing"
    source_count=q.get("source_records")
    published=int(reconciliation.get("producer_acknowledged_total",0) or 0)
    processed=int(reconciliation.get("spark_unique_processed_total",0) or 0)
    retained=reconciliation.get("kafka_retained_offset_range")
    database=data.get("database",{})
    db_state=database.get("status","error")
    active=spark.get("status")=="running" and kafka.get("status")=="reachable" and producer_active
    if active:
        health_text="Replay is flowing"
        health_class=""
    elif kafka.get("status")=="reachable" and spark.get("status")=="running":
        health_text="Pipeline ready"
        health_class=""
    elif kafka.get("status")=="unreachable" or spark.get("status") in ("stale","stopped","error") or db_state!="available":
        health_text="Attention required"
        health_class="off"
    else:
        health_text="Waiting for services"
        health_class="warn"

    st.markdown('<div class="pipe-hero"><div class="pipe-hero-icon">⎇</div><div><h2>Data Pipeline Monitoring</h2><p>Follow historical Retailrocket data through replay, Kafka, Spark, and dashboard analytics</p></div></div>',unsafe_allow_html=True)
    with st.container(border=True):
        head=f'<div class="pipe-section-head"><span>◈ &nbsp; Pipeline Flow</span><span class="pipe-operating {health_class}">● &nbsp; {html.escape(health_text)}</span></div>'
        st.markdown(head,unsafe_allow_html=True)
        batch_note=f'Last batch {spark.get("last_batch_id")}' if spark.get("last_batch_id") is not None else "Awaiting first batch"
        prep_state=spark.get("active_batch_status") or spark.get("last_batch_status") or "waiting"
        stages=[
            ("Retailrocket source","▤",source_state,f'{fmt(source_count) if source_count is not None else "—"} source rows',"Original CSV files"),
            ("Replay producer","▰",producer_state,f'{fmt(published)} Kafka acknowledgements',"Historical event replay"),
            ("Kafka topic","◉",kafka.get("status","unknown"),f'{fmt(retained) if retained is not None else "—"} retained offset range',str(replay.get("topic","retailrocket-events"))),
            ("Spark processing","✿",spark.get("status","unknown"),f'{fmt(processed)} unique processed',str(prep_state)),
            ("SQLite event store","▤",db_state,f'{fmt(database.get("processed_events",0))} persisted events',"Local durable database"),
            ("Streamlit dashboard","◫","serving","Database Explorer available","Refreshes every 3 seconds"),
        ]
        ratios=[]
        for i in range(len(stages)):
            ratios.append(1)
            if i < len(stages)-1: ratios.append(.16)
        stage_cols=st.columns(ratios,gap="small")
        for i,(title,icon,state,value,note) in enumerate(stages):
            tone="active" if title=="Replay producer" and producer_active else "good" if state in ("available","reachable","running","ready","serving","completed") else "bad" if state in ("missing","unreachable","stopped","error","failed","stale") else "active" if state in ("starting",) else ""
            check="✓" if tone=="good" else "!" if tone=="bad" else "·"
            stage_cols[i*2].markdown(f'<div class="pipe-stage {tone}"><div class="pipe-stage-icon">{icon}</div><div class="pipe-stage-title">{html.escape(title)}</div><div class="pipe-stage-note">{html.escape(str(value))}<br>{html.escape(str(note))}</div><span class="pipe-stage-status {"bad" if tone=="bad" else "neutral" if not tone else ""}">{check}</span></div>',unsafe_allow_html=True)
            if i < len(stages)-1:
                stage_cols[i*2+1].markdown('<div class="pipe-arrow">➜</div>',unsafe_allow_html=True)
    st.caption("Spark validates and deduplicates Kafka events. The dashboard persists committed Spark output in SQLite; the original Retailrocket CSV files remain unchanged.")

    st.markdown("")
    last_seconds=spark.get("last_batch_seconds")
    kpis=[
        ("Source dataset rows",fmt(source_count) if source_count is not None else "—","Full historical CSV","#9d66ff"),
        ("Kafka acknowledged",fmt(published),"Producer confirmations","#2dbfdf"),
        ("Kafka retained range",fmt(retained) if retained is not None else "Unavailable","Partition offsets; may include duplicates","#ef9a41"),
        ("Spark unique output",fmt(processed),"Committed processed event IDs","#d14bda"),
        ("SQLite persisted",fmt(database.get("processed_events",0)),"Unique Spark output rows stored","#16bda7"),
        ("Latest batch duration",f'{last_seconds} s' if last_seconds is not None else "—",batch_note,"#f2a644"),
    ]
    for row_start in (0,3):
        cols=st.columns(3,gap="small")
        for col,(label,value,note,accent) in zip(cols,kpis[row_start:row_start+3]):
            col.markdown(f'<div class="pipe-kpi" style="--accent:{accent}"><div class="pipe-kpi-label">{html.escape(label)}</div><div class="pipe-kpi-value">{html.escape(str(value))}</div><div class="pipe-kpi-note">{html.escape(str(note))}</div></div>',unsafe_allow_html=True)
        if row_start==0: st.markdown("")

    st.markdown("")
    progress_col,trend_col,status_col=st.columns([1,1.3,1],gap="small")
    with progress_col:
        with st.container(border=True):
            st.markdown('<div class="pipe-mini-title">Pipeline Progress</div><p class="pipe-mini-note">Spark committed output compared with producer acknowledgements</p>',unsafe_allow_html=True)
            if published:
                percent=min(100.0,processed/published*100)
                fig=go.Figure(go.Pie(values=[processed,max(0,published-processed)],labels=["Spark unique processed","Awaiting / count difference"],hole=.72,sort=False,
                    marker={"colors":["#8a50ed","#252447"],"line":{"color":"#11132b","width":2}},
                    hovertemplate="%{label}<br>%{value:,} events<extra></extra>",textinfo="none"))
                fig.update_layout(template="plotly_dark",paper_bgcolor="rgba(0,0,0,0)",plot_bgcolor="rgba(0,0,0,0)",
                    height=210,margin=dict(l=0,r=0,t=0,b=0),showlegend=False,
                    annotations=[dict(text=f"{percent:.1f}%<br><span style='font-size:9px'>processed/ack</span>",x=.5,y=.5,showarrow=False,font=dict(size=19,color="#f4f0ff"))])
                st.plotly_chart(fig,width="stretch",config={"displayModeBar":False,"responsive":True})
            else:
                st.info("No Kafka acknowledgement total is available yet.")
            st.caption(f"{fmt(processed)} Spark unique IDs · {fmt(published)} producer acknowledgements")
    with trend_col:
        with st.container(border=True):
            st.markdown('<div class="pipe-mini-title">Spark Data Flow Trend</div><p class="pipe-mini-note">Committed valid output by actual Spark micro-batch</p>',unsafe_allow_html=True)
            batches=data.get("batch_trend",[])
            if batches:
                bdf=pd.DataFrame(batches).sort_values("batch_id")
                fig=px.area(bdf,x="batch_id",y="processed_events",markers=True,
                    color_discrete_sequence=["#9d49ff"])
                fig.update_traces(line={"width":2.5,"color":"#bb62ff"},fillcolor="rgba(129,65,255,.23)",
                    marker={"size":6,"color":"#43d7e5"},hovertemplate="Spark batch %{x}<br>%{y:,} processed events<extra></extra>")
                fig.update_layout(template="plotly_dark",paper_bgcolor="rgba(0,0,0,0)",plot_bgcolor="rgba(0,0,0,0)",
                    height=230,margin=dict(l=5,r=8,t=8,b=5),showlegend=False,xaxis_title="Spark batch",yaxis_title="Processed rows")
                fig.update_xaxes(showgrid=False); fig.update_yaxes(gridcolor="#29264a",rangemode="tozero")
                st.plotly_chart(fig,width="stretch",config={"displayModeBar":False,"responsive":True})
            else:
                st.info("Batch trend appears after Spark writes processed output.")
    with status_col:
        with st.container(border=True):
            st.markdown('<div class="pipe-mini-title">Kafka &amp; Spark Status</div><p class="pipe-mini-note">Reachability and processor freshness</p>',unsafe_allow_html=True)
            heartbeat_age=spark.get("heartbeat_age_seconds")
            partition_list=", ".join(str(x) for x in kafka.get("partitions",[])) or "—"
            status_items=[
                ("Kafka broker",kafka.get("status","unknown")),
                ("Topic",replay.get("topic","retailrocket-events")),
                ("Partitions",partition_list),
                ("Spark processor",spark.get("status","unknown")),
                ("Spark heartbeat age",f'{heartbeat_age} s' if heartbeat_age is not None else "not received"),
                ("Last successful batch",spark.get("last_successful_execution") or "not recorded"),
            ]
            for label,value in status_items:
                st.markdown(f'<div class="pipe-status-item"><span class="pipe-status-label">{html.escape(str(label))}</span><span class="pipe-status-value">{html.escape(str(value))}</span></div>',unsafe_allow_html=True)

    st.markdown("")
    quality_col,events_col=st.columns([1,1.15],gap="small")
    valid=int(q.get("valid_records_processed",0) or 0)
    invalid=int(q.get("invalid_records",0) or 0)
    duplicate=int(q.get("duplicates",0) or 0)
    checked=valid+invalid+duplicate
    def pct_count(count,denom):
        return f"{count/denom*100:.1f}%" if denom else "—"
    quality_rows=[
        {"Metric":"Retailrocket source rows","Count":fmt(source_count) if source_count is not None else "—","Share":("100% of CSV" if source_count is not None else "—"),"Status":q.get("source_audit_status","unknown")},
        {"Metric":"Spark valid unique output","Count":fmt(valid),"Share":pct_count(valid,published),"Status":"committed" if valid else "waiting"},
        {"Metric":"Invalid records","Count":fmt(invalid),"Share":pct_count(invalid,checked),"Status":"checked" if checked else "waiting"},
        {"Metric":"Duplicate IDs ignored","Count":fmt(duplicate),"Share":pct_count(duplicate,checked),"Status":"checked" if checked else "waiting"},
        {"Metric":"SQLite persisted events","Count":fmt(database.get("processed_events",0)),"Share":pct_count(int(database.get("processed_events",0) or 0),processed),"Status":db_state},
        {"Metric":"Kafka retained offset range","Count":fmt(retained) if retained is not None else "Unavailable","Share":"—","Status":"offset estimate"},
    ]
    with quality_col:
        with st.container(border=True):
            st.markdown('<div class="pipe-mini-title">Data Quality</div><p class="pipe-mini-note">Source audit and actual Spark pipeline counters</p>',unsafe_allow_html=True)
            render_dark_table(pd.DataFrame(quality_rows),height=270,min_width=590)
            st.caption(f"Full source audit: {q.get('source_audit_status','unknown')} · Missing transaction IDs in source: {fmt(q.get('missing_transaction_id')) if q.get('missing_transaction_id') is not None else 'pending'} (expected on non-transaction rows).")
    with events_col:
        with st.container(border=True):
            st.markdown('<div class="pipe-mini-title">Recent Processed Events</div><p class="pipe-mini-note">Sample of real records in committed Spark output</p>',unsafe_allow_html=True)
            event_rows=[{"Event time":event.get("event_time"),"Type":event.get("type"),"Product ID":event.get("item_id"),
                         "Spark batch":event.get("spark_batch_id"),"Processed at":event.get("processed_at"),"Status":event.get("preprocessing_status","valid")}
                        for event in data.get("events",[])[:8]]
            if event_rows:
                render_dark_table(pd.DataFrame(event_rows),height=270,min_width=780)
            else:
                st.info("No Spark-processed sample events are available yet.")

    with st.container(border=True):
        st.markdown('<div class="pipe-mini-title">Pipeline Architecture</div><p class="pipe-mini-note">The deployed flow: historical records are replayed into Kafka, then Spark validates and aggregates them for the dashboard.</p>',unsafe_allow_html=True)
        architecture=[("Retailrocket","▤"),("Replay producer","▰"),("Kafka topic","◉"),("Spark validate · deduplicate · aggregate","✿"),("SQLite event store","▥"),("Streamlit","◫")]
        pieces=[]
        for index,(label,icon) in enumerate(architecture):
            pieces.append(f'<div class="pipe-architecture-node"><span class="pipe-architecture-icon">{icon}</span>{html.escape(label)}</div>')
            if index<len(architecture)-1: pieces.append('<span class="pipe-architecture-arrow">➜</span>')
        st.markdown(f'<div class="pipe-architecture">{"".join(pieces)}</div>',unsafe_allow_html=True)
        st.info("Evidence is taken from the local Retailrocket source audit, Kafka metadata/offsets, producer acknowledgements, Spark output and heartbeat. CPU and memory usage are not collected by this demo, so they are not reported.")
    if spark.get("status")=="stale":
        st.error("Spark has no recent heartbeat. The last saved state is not treated as proof that the processor is alive.")
    st.caption("Kafka retained records are estimated from partition end offsets minus beginning offsets; they may include duplicate records or be affected by retention. Producer acknowledgements and Spark unique output are separate counts.")


def render_visualization(data):
    o=data["overview"]
    st.markdown('<div class="viz-heading"><div class="viz-kicker">VISUAL EXPLORER · SPARK OUTPUTS</div><h2>Behavior &amp; Product Intelligence</h2><p>Interactive views over validated Retailrocket events · Historical Event Replay / Real-Time Simulation</p></div>', unsafe_allow_html=True)
    st.markdown('<div class="viz-note">All charts use processed Spark output. Date charts use original event time; replay arrival is monitored separately. Product labels are source item IDs.</div>', unsafe_allow_html=True)

    # KPI strip follows the compact card layout of the supplied visual reference.
    cards=st.columns(6)
    for col,label,value in zip(cards,["Processed events","Product views","Unique visitors","Unique products","No-purchase sessions","No-purchase rate"],
                              [o["total_events"],o["views"],o["unique_visitors"],o["unique_products"],o["no_purchase_sessions"],f'{o["no_purchase_percent"]:.1f}%']):
        col.metric(label,fmt(value) if isinstance(value,(int,float)) and not isinstance(value,bool) else value)

    # Wide trend chart with compact event mix alongside it.
    left,right=st.columns([1.7,1],gap="medium")
    with left, st.container(border=True):
        st.markdown("#### Event trend · original event date")
        st.caption("Views, cart additions, and transactions by day. These are event totals, not unique-user conversions.")
        rows=data.get("daily_trend",[])
        if rows:
            df=pd.DataFrame(rows).rename(columns={"date":"Event date","views":"Product views","addtocart":"Add to cart","transactions":"Transactions"})
            long=df.melt(id_vars="Event date",var_name="Event type",value_name="Events")
            fig=px.area(long,x="Event date",y="Events",color="Event type",markers=True,
                color_discrete_map={"Product views":"#64c9d2","Add to cart":"#9b83ff","Transactions":"#ff9b78"},
                line_group="Event type")
            fig.update_traces(mode="lines+markers",line={"width":2.4},marker={"size":5},opacity=.84)
            fig.update_layout(template="plotly_dark",paper_bgcolor="rgba(0,0,0,0)",plot_bgcolor="rgba(0,0,0,0)",
                margin=dict(l=8,r=8,t=8,b=8),height=330,legend_title_text="",hovermode="x unified",
                xaxis_title="Historical date",yaxis_title="Processed events")
            fig.update_xaxes(showgrid=False); fig.update_yaxes(gridcolor="#2b3b50",rangemode="tozero")
            st.plotly_chart(fig,width="stretch",config={"displayModeBar":False,"responsive":True})
        else: st.info("Trend appears after Spark writes processed event records.")
    with right, st.container(border=True):
        st.markdown("#### Event mix")
        st.caption("Source event counts. There is no separate genuine click event in Retailrocket.")
        counts=[int(o.get("views",0)),int(o.get("addtocart",0)),int(o.get("transactions",0))]
        if sum(counts):
            fig=go.Figure(go.Pie(labels=["Product views","Add to cart","Transactions"],values=counts,hole=.69,sort=False,
                marker={"colors":["#64c9d2","#9b83ff","#ff9b78"],"line":{"color":"#171e2c","width":3}},
                textinfo="label+percent",textposition="outside",hovertemplate="%{label}<br>%{value:,} events · %{percent}<extra></extra>"))
            fig.update_layout(template="plotly_dark",paper_bgcolor="rgba(0,0,0,0)",plot_bgcolor="rgba(0,0,0,0)",
                margin=dict(l=10,r=10,t=12,b=10),height=330,showlegend=False,
                annotations=[dict(text=f"{fmt(sum(counts))}<br><span style='font-size:10px'>events</span>",x=.5,y=.5,showarrow=False,font=dict(size=17,color="#f4f0ff"))])
            st.plotly_chart(fig,width="stretch",config={"displayModeBar":False,"responsive":True})
        else: st.info("Event-mix chart appears after Spark processing.")

    # Product ranking and purchase funnel.
    left,right=st.columns([1.25,1],gap="medium")
    with left, st.container(border=True):
        st.markdown("#### Product engagement ranking")
        st.caption("Top item IDs by views, cart additions, and transaction-associated events. Names are not present in this dataset.")
        product_chart(data.get("products",[])[:10])
    with right, st.container(border=True):
        st.markdown("#### Purchase funnel · event counts")
        st.caption("Repeated actions count separately; this is not a unique-user conversion path.")
        purchase_funnel(data,height=365)

    # Session outcomes and temporal distribution.
    left,right=st.columns([1,1.35],gap="medium")
    with left, st.container(border=True):
        st.markdown("#### Session outcomes")
        st.caption("A session is visitor activity split after 30 minutes of inactivity. No purchase means no transaction recorded in that session.")
        eligible=int(o.get("eligible_sessions",0)); no_purchase=int(o.get("no_purchase_sessions",0))
        purchased=max(0,eligible-no_purchase)
        if eligible:
            fig=go.Figure(go.Pie(labels=["No recorded transaction","Transaction recorded"],values=[no_purchase,purchased],hole=.7,sort=False,
                marker={"colors":["#ff9b78","#63cdbb"],"line":{"color":"#171e2c","width":3}},
                hovertemplate="%{label}<br>%{value:,} sessions · %{percent}<extra></extra>",textinfo="label+percent",textposition="outside"))
            fig.update_layout(template="plotly_dark",paper_bgcolor="rgba(0,0,0,0)",plot_bgcolor="rgba(0,0,0,0)",
                margin=dict(l=8,r=8,t=15,b=12),height=320,showlegend=False,
                annotations=[dict(text=f"{eligible:,}<br><span style='font-size:10px'>eligible</span>",x=.5,y=.5,showarrow=False,font=dict(size=17,color="#f4f0ff"))])
            st.plotly_chart(fig,width="stretch",config={"displayModeBar":False,"responsive":True})
            st.metric("Sessions without recorded purchase",fmt(no_purchase),f'{o.get("no_purchase_percent",0):.2f}% of eligible sessions')
        else: st.info("Session outcomes appear when eligible sessions are processed.")
    with right, st.container(border=True):
        st.markdown("#### Event activity by hour")
        st.caption("Original event-time buckets among the latest 24 distinct hours present in processed data.")
        hours=data.get("hourly_trend",[])
        if hours:
            x=[row["hour"] for row in hours]
            fig=go.Figure()
            for key,label,color in [("views","Views","#64c9d2"),("addtocart","Add to cart","#9b83ff"),("transactions","Transactions","#ff9b78")]:
                fig.add_trace(go.Bar(x=x,y=[int(row.get(key,0)) for row in hours],name=label,marker_color=color,
                    hovertemplate="%{x}<br>%{y:,} events<extra>"+label+"</extra>"))
            fig.update_layout(template="plotly_dark",paper_bgcolor="rgba(0,0,0,0)",plot_bgcolor="rgba(0,0,0,0)",
                barmode="group",margin=dict(l=8,r=8,t=8,b=8),height=320,legend_title_text="",
                xaxis_title="Original event hour",yaxis_title="Events")
            fig.update_xaxes(showgrid=False,tickangle=-35); fig.update_yaxes(gridcolor="#2b3b50",rangemode="tozero")
            st.plotly_chart(fig,width="stretch",config={"displayModeBar":False,"responsive":True})
        else: st.info("Hourly chart appears when Spark outputs contain event timestamps.")

    with st.container(border=True):
        st.markdown("#### Session behavior patterns")
        st.caption("Eligible sessions grouped by their observed outcome: any transaction, cart activity without a transaction, or views without cart/transaction. These categories are session counts.")
        segments=data.get("session_segments",[])
        if segments:
            sdf=pd.DataFrame(segments).sort_values("count",ascending=True)
            fig=px.bar(sdf,x="count",y="name",orientation="h",text="count",
                color="name",color_discrete_map={"Transaction recorded":"#63cdbb","Cart, no transaction":"#ff9b78","Views only":"#9b83ff"})
            fig.update_traces(texttemplate="%{x:,}",textposition="outside",hovertemplate="%{y}<br>%{x:,} sessions<extra></extra>")
            fig.update_layout(template="plotly_dark",paper_bgcolor="rgba(0,0,0,0)",plot_bgcolor="rgba(0,0,0,0)",
                margin=dict(l=8,r=35,t=8,b=8),height=270,showlegend=False,xaxis_title="Eligible sessions",yaxis_title="")
            fig.update_xaxes(showgrid=False); fig.update_yaxes(showgrid=False)
            st.plotly_chart(fig,width="stretch",config={"displayModeBar":False,"responsive":True})
        else: st.info("Session behavior appears after session events are processed.")

    # Actual Spark batch output and data-quality chart.
    left,right=st.columns([1.3,1],gap="medium")
    with left, st.container(border=True):
        st.markdown("#### Spark processing by micro-batch")
        st.caption("Counts are derived from each processed event's Spark batch ID, showing actual output volume.")
        batches=data.get("batch_trend",[])
        if batches:
            bdf=pd.DataFrame(batches)
            fig=px.bar(bdf,x="batch_id",y="processed_events",color="processed_events",color_continuous_scale=["#547caa","#64c9d2","#9b83ff"],
                hover_data={"batch_id":True,"processed_events":":,"})
            fig.update_layout(template="plotly_dark",paper_bgcolor="rgba(0,0,0,0)",plot_bgcolor="rgba(0,0,0,0)",
                margin=dict(l=8,r=8,t=8,b=8),height=300,showlegend=False,coloraxis_showscale=False,
                xaxis_title="Spark batch ID",yaxis_title="Processed events")
            fig.update_xaxes(showgrid=False); fig.update_yaxes(gridcolor="#2b3b50",rangemode="tozero")
            st.plotly_chart(fig,width="stretch",config={"displayModeBar":False,"responsive":True})
        else: st.info("Spark batch chart appears after processed output is available.")
    with right, st.container(border=True):
        st.markdown("#### Data quality · replayed records")
        st.caption("Processed valid records, invalid records, and duplicate event IDs. Source dataset size is shown separately from this replay sample.")
        q=data.get("data_quality",{})
        quality=[int(q.get("valid_records_processed",0)),int(q.get("invalid_records",0)),int(q.get("duplicates",0))]
        if sum(quality):
            fig=go.Figure(go.Bar(x=["Valid unique","Invalid","Duplicates"],y=quality,
                marker_color=["#63cdbb","#ff7187","#f3bd67"],
                text=[fmt(value) for value in quality],textposition="outside",
                hovertemplate="%{x}: %{y:,} records<extra></extra>"))
            fig.update_layout(template="plotly_dark",paper_bgcolor="rgba(0,0,0,0)",plot_bgcolor="rgba(0,0,0,0)",
                margin=dict(l=8,r=8,t=22,b=8),height=300,showlegend=False,yaxis_title="Records")
            fig.update_xaxes(showgrid=False); fig.update_yaxes(gridcolor="#2b3b50",rangemode="tozero")
            st.plotly_chart(fig,width="stretch",config={"displayModeBar":False,"responsive":True})
        else: st.info("Quality chart appears after Spark processing.")

    # Add complementary views derived only from Spark outputs and the actual
    # producer/Kafka/Spark trace. These are descriptive event counts, not
    # fabricated sales or person-level conversion metrics.
    left,right=st.columns([1.25,1],gap="medium")
    with left, st.container(border=True):
        st.markdown("#### Product interaction heatmap")
        st.caption("Top product IDs and their actual Spark-processed view, cart-addition, and transaction-associated event counts.")
        products=data.get("products",[])
        if products:
            product_frame=pd.DataFrame(products).fillna(0)
            for key in ("views","cart_additions","transactions"):
                if key not in product_frame: product_frame[key]=0
            product_frame["total_events"]=product_frame[["views","cart_additions","transactions"]].sum(axis=1)
            product_frame=product_frame.nlargest(12,"total_events").sort_values("total_events",ascending=True)
            z=product_frame[["views","cart_additions","transactions"]].astype(int).values.tolist()
            labels=[f"#{item}" for item in product_frame["item_id"]]
            fig=go.Figure(go.Heatmap(
                x=["Views","Add to cart","Transaction items"],y=labels,z=z,
                text=[[fmt(value) for value in row] for row in z],texttemplate="%{text}",
                colorscale=[[0,"#17192d"],[.35,"#275d78"],[.7,"#4166bd"],[1,"#a477ff"]],
                colorbar={"title":"Events","tickformat":"~s"},
                hovertemplate="Product %{y}<br>%{x}: %{z:,} events<extra></extra>"))
            fig.update_layout(template="plotly_dark",paper_bgcolor="rgba(0,0,0,0)",plot_bgcolor="rgba(0,0,0,0)",
                margin=dict(l=8,r=8,t=12,b=8),height=max(330,26*len(labels)+90),
                xaxis_title="Observed event type",yaxis_title="Retailrocket item ID")
            fig.update_xaxes(side="top",showgrid=False); fig.update_yaxes(showgrid=False)
            st.plotly_chart(fig,width="stretch",config={"displayModeBar":False,"responsive":True})
        else: st.info("Product interaction heatmap appears after Spark produces item-level output.")
    with right, st.container(border=True):
        st.markdown("#### Event action ratios")
        st.caption("Repeated event counts relative to the previous funnel stage. This is not a unique-visitor conversion rate.")
        view_count=int(o.get("views",0) or 0)
        cart_count=int(o.get("addtocart",0) or 0)
        transaction_count=int(o.get("transactions",0) or 0)
        ratio_labels=[]; ratio_values=[]
        if view_count:
            ratio_labels.append("Add-to-cart events ÷ views")
            ratio_values.append(100*cart_count/view_count)
        if cart_count:
            ratio_labels.append("Transaction events ÷ cart events")
            ratio_values.append(100*transaction_count/cart_count)
        if ratio_values:
            fig=go.Figure(go.Bar(x=ratio_values,y=ratio_labels,orientation="h",
                marker={"color":["#45c9c4","#a27aff"][:len(ratio_values)],"line":{"color":"#d4c6ff","width":.5}},
                text=[f"{value:.2f}%" for value in ratio_values],textposition="outside",
                hovertemplate="%{y}<br>%{x:.2f}% of prior event count<extra></extra>"))
            fig.update_layout(template="plotly_dark",paper_bgcolor="rgba(0,0,0,0)",plot_bgcolor="rgba(0,0,0,0)",
                margin=dict(l=12,r=38,t=12,b=12),height=300,showlegend=False,
                xaxis_title="Ratio of event counts (%)",yaxis_title="")
            fig.update_xaxes(showgrid=False); fig.update_yaxes(showgrid=False,autorange="reversed")
            st.plotly_chart(fig,width="stretch",config={"displayModeBar":False,"responsive":True})
        else: st.info("Action ratios appear when the required event counts are available.")

    left,right=st.columns([1,1],gap="medium")
    with left, st.container(border=True):
        st.markdown("#### Weekly event pattern")
        st.caption("Event counts grouped by weekday from the latest 30 distinct event dates available in the daily Spark trend.")
        daily=data.get("daily_trend",[])
        if daily:
            weekday_frame=pd.DataFrame(daily)
            weekday_frame["parsed_date"]=pd.to_datetime(weekday_frame["date"],errors="coerce")
            weekday_frame=weekday_frame.dropna(subset=["parsed_date"])
            weekday_frame["weekday_index"]=weekday_frame["parsed_date"].dt.dayofweek
            weekday_frame["weekday"]=weekday_frame["parsed_date"].dt.day_name()
            weekday_order=["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
            weekday_summary=weekday_frame.groupby(["weekday_index","weekday"],as_index=False)[["views","addtocart","transactions"]].sum()
            weekday_summary=weekday_summary.sort_values("weekday_index")
            weekday_long=weekday_summary.melt(id_vars=["weekday_index","weekday"],
                value_vars=["views","addtocart","transactions"],var_name="Event type",value_name="Events")
            weekday_long["Event type"]=weekday_long["Event type"].map({"views":"Views","addtocart":"Add to cart","transactions":"Transactions"})
            fig=px.bar(weekday_long,x="weekday",y="Events",color="Event type",barmode="group",
                category_orders={"weekday":weekday_order},
                color_discrete_map={"Views":"#64c9d2","Add to cart":"#9b83ff","Transactions":"#ff9b78"})
            fig.update_layout(template="plotly_dark",paper_bgcolor="rgba(0,0,0,0)",plot_bgcolor="rgba(0,0,0,0)",
                margin=dict(l=8,r=8,t=12,b=8),height=310,legend_title_text="",
                xaxis_title="Original event weekday",yaxis_title="Processed events")
            fig.update_xaxes(showgrid=False,tickangle=-25); fig.update_yaxes(gridcolor="#2b3b50",rangemode="tozero")
            st.plotly_chart(fig,width="stretch",config={"displayModeBar":False,"responsive":True})
        else: st.info("Weekly event pattern appears after Spark outputs include event dates.")
    with right, st.container(border=True):
        st.markdown("#### Latest capture progress")
        st.caption("Actual counts from the most recent request: requested events, Kafka acknowledgements, Spark-processed events, and records still waiting.")
        capture=data.get("capture_summary",{})
        capture_values=[int(capture.get(key,0) or 0) for key in
            ("requested","kafka_acknowledged","spark_processed","waiting_for_spark")]
        if any(capture_values):
            fig=go.Figure(go.Bar(x=["Requested","Kafka ack","Spark processed","Waiting for Spark"],y=capture_values,
                marker_color=["#68758c","#49c8c2","#9b83ff","#ff9b78"],
                text=[fmt(value) for value in capture_values],textposition="outside",
                hovertemplate="%{x}<br>%{y:,} events<extra></extra>"))
            fig.update_layout(template="plotly_dark",paper_bgcolor="rgba(0,0,0,0)",plot_bgcolor="rgba(0,0,0,0)",
                margin=dict(l=8,r=8,t=22,b=8),height=310,showlegend=False,
                xaxis_title="Pipeline stage",yaxis_title="Events")
            fig.update_xaxes(showgrid=False,tickangle=-18); fig.update_yaxes(gridcolor="#2b3b50",rangemode="tozero")
            st.plotly_chart(fig,width="stretch",config={"displayModeBar":False,"responsive":True})
        else: st.info("Capture progress appears after a data request has been made.")

    st.markdown("#### Spark processing latency")
    st.caption("Measured from producer ingestion timestamp to Spark processing timestamp for the latest processed event sample; this is replay pipeline delay, not source event age.")
    latency_rows=[]
    for event in data.get("events",[]):
        ingested=pd.to_datetime(event.get("ingested_at"),utc=True,errors="coerce")
        processed=pd.to_datetime(event.get("processed_at"),utc=True,errors="coerce")
        if pd.notna(ingested) and pd.notna(processed):
            seconds=(processed-ingested).total_seconds()
            if seconds>=0:
                latency_rows.append({"Event type":event.get("type","unknown"),"Latency seconds":seconds})
    if latency_rows:
        latency_frame=pd.DataFrame(latency_rows)
        median_latency=float(latency_frame["Latency seconds"].median())
        p95_latency=float(latency_frame["Latency seconds"].quantile(.95))
        latency_cards=st.columns(3)
        latency_cards[0].metric("Events in sample",fmt(len(latency_frame)))
        latency_cards[1].metric("Median pipeline delay",f"{median_latency:.2f} s")
        latency_cards[2].metric("95th percentile delay",f"{p95_latency:.2f} s")
        fig=px.histogram(latency_frame,x="Latency seconds",color="Event type",nbins=24,
            marginal="box",barmode="overlay",opacity=.8,
            color_discrete_map={"view":"#64c9d2","addtocart":"#9b83ff","transaction":"#ff9b78"})
        fig.update_layout(template="plotly_dark",paper_bgcolor="rgba(0,0,0,0)",plot_bgcolor="rgba(0,0,0,0)",
            margin=dict(l=8,r=8,t=12,b=8),height=370,legend_title_text="",
            xaxis_title="Ingestion-to-Spark delay (seconds)",yaxis_title="Processed event sample")
        fig.update_xaxes(showgrid=False); fig.update_yaxes(gridcolor="#2b3b50",rangemode="tozero")
        st.plotly_chart(fig,width="stretch",config={"displayModeBar":False,"responsive":True})
    else:
        st.info("Latency distribution appears when processed events contain both ingestion and Spark processing timestamps.")

    st.caption("Source: Retailrocket historical dataset → Kafka topic `retailrocket-events` → Spark Structured Streaming outputs. Replay is a real-time simulation, not live store traffic. Genuine click events are unavailable in the source dataset.")


def events_csv_gzip():
    columns=["event_id","event_type","event_time","ingested_at","processed_at","visitor_id","session_id","item_id","transaction_id","source"]
    output=io.BytesIO(); seen=set()
    with gzip.GzipFile(fileobj=output,mode="wb",mtime=0) as compressed:
        class Sink:
            def write(self, value): compressed.write(value.encode("utf-8"))
        writer=csv.DictWriter(Sink(),fieldnames=columns,extrasaction="ignore",lineterminator="\n")
        writer.writeheader()
        for path in sorted(SPARK_EVENTS.glob("part-*.json")) if SPARK_EVENTS.exists() else []:
            try:
                with path.open(encoding="utf-8") as source:
                    for line in source:
                        try: event=json.loads(line)
                        except ValueError: continue
                        event_id=event.get("event_id")
                        if event_id is not None:
                            if event_id in seen: continue
                            seen.add(event_id)
                        writer.writerow(event)
            except OSError: continue
    return output.getvalue()


def render_exports(data):
    st.subheader("Tableau Exports")
    st.caption("Download processed Spark outputs as CSV files for Tableau Desktop. These exports contain real processed records and current aggregate metrics.")
    q=data["overview"]
    metrics={**q,"source":data["source"],"mode":data["mode"]}
    metrics_csv=pd.DataFrame([metrics]).to_csv(index=False).encode("utf-8")
    c1,c2=st.columns(2)
    c1.download_button("Download current metrics CSV",metrics_csv,"retailrocket_spark_metrics.csv","text/csv",width="stretch")
    if c2.button("Prepare Spark events export",help="Reads committed Spark JSON output and removes duplicate event IDs."):
        with st.spinner("Reading Spark processed events and preparing compressed CSV…"):
            st.session_state["events_export_gzip"]=events_csv_gzip()
    if st.session_state.get("events_export_gzip"):
        st.download_button("Download Spark events CSV (GZIP)",st.session_state["events_export_gzip"],"retailrocket_spark_processed_events.csv.gz","application/gzip",width="stretch")
    st.info("The source Retailrocket dataset is historical. The dashboard does not describe replay as live store traffic.")


def render_database(data):
    db = data.get("database", {})
    st.markdown('<div class="viz-heading"><div class="viz-kicker">DURABLE SPARK OUTPUT · SQLITE</div><h2>Processed Event Database</h2><p>Inspect event records committed by Spark and stored locally by the dashboard backend.</p></div>', unsafe_allow_html=True)
    if db.get("status") == "error":
        st.error(f"Database storage error: {db.get('error') or 'unknown error'}")
    st.caption(f"Database file: `{db.get('path', 'artifacts/retailrocket_analytics.db')}` · SQLite is local and requires no extra Docker service.")
    metrics = st.columns(5)
    for col, label, value in zip(metrics,
            ["Stored Spark events", "Unique visitors", "Unique products", "Spark batches", "Database size"],
            [db.get("processed_events", 0), db.get("unique_visitors", 0), db.get("unique_products", 0),
             db.get("spark_batches", 0), f'{(db.get("size_bytes", 0) or 0) / 1024:.1f} KB']):
        col.metric(label, fmt(value) if isinstance(value, (int, float)) else value)
    st.markdown("#### Stored event types")
    type_counts = db.get("event_types", {}) or {}
    if type_counts:
        cols = st.columns(3)
        for col, typ in zip(cols, ("view", "addtocart", "transaction")):
            col.metric(typ.replace("addtocart", "Add to cart").title(), fmt(type_counts.get(typ, 0)))
    st.caption(f"Last Spark event stored: {db.get('last_processed_at') or 'No event stored yet'} · Pending database writes: {fmt(db.get('pending_events', 0))}")

    filter_cols = st.columns([1, 1, 1])
    event_type = filter_cols[0].selectbox("Event type", ["All", "view", "addtocart", "transaction"])
    product_id = filter_cols[1].text_input("Product item ID", placeholder="Optional exact item ID")
    limit = filter_cols[2].selectbox("Rows to show", [100, 250, 500, 1000, 5000], index=1)
    try:
        rows = query_events(limit, None if event_type == "All" else event_type, product_id or None)
    except Exception as exc:
        rows = []
        st.error(f"Could not read database rows: {exc}")
    if rows:
        df = pd.DataFrame(rows)
        render_dark_table(df,height=430,min_width=1100)
        st.download_button("Download these database rows as CSV", df.to_csv(index=False).encode("utf-8"),
                           "retailrocket_database_events.csv", "text/csv")
    else:
        st.info("No matching Spark-processed events are stored yet. Request data and wait for Spark to commit an output batch.")

    with st.expander("Recent Spark batch records"):
        try:
            batches = query_batches(100)
            if batches:
                render_dark_table(pd.DataFrame(batches),height=360,min_width=950)
            else:
                st.caption("No Spark batch records have been saved yet.")
        except Exception as exc:
            st.error(f"Could not read Spark batch records: {exc}")


def pipeline_is_busy():
    replay=read_json(REPLAY_STATE,{})
    spark=read_json(SPARK_STATE,{})
    metrics=read_json(METRICS,{})
    accounted=sum(int(metrics.get(key,0) or 0) for key in ("processed_events","invalid_records","duplicates"))
    pending=int(replay.get("published",0) or 0)>accounted
    return (replay.get("status")=="running" or spark.get("status")=="starting" or
            (spark.get("status")=="running" and
             (pending or spark.get("active_batch_id") is not None or spark.get("active_batch_status") not in (None,"idle"))))


polling_enabled=bool(st.session_state.get("force_event_poll",False) or pipeline_is_busy())


@st.fragment(run_every=3 if polling_enabled else None)
def render_selected_page():
    data=dashboard_data()
    page=st.session_state.get("main_page","Dashboard")
    replay=data["pipeline"].get("replay",{})
    kafka=data["pipeline"].get("kafka",{})
    spark=data["pipeline"].get("spark",{})
    capture_active=health_data().get("capture_process_running",False)
    metrics=data.get("spark_metrics",{})
    accounted=sum(int(metrics.get(key,0) or 0) for key in ("processed_events","invalid_records","duplicates"))
    pending=int(replay.get("published",0) or 0)>accounted
    pipeline_busy=(replay.get("status")=="running" or spark.get("status")=="starting" or
                   (spark.get("status")=="running" and
                    (pending or spark.get("active_batch_id") is not None or spark.get("active_batch_status") not in (None,"idle"))) or
                   capture_active)
    if pipeline_busy != polling_enabled:
        st.session_state["force_event_poll"]=pipeline_busy
        st.rerun(scope="app")
    st.session_state["force_event_poll"]=pipeline_busy
    replaying=replay.get("status")=="running" and kafka.get("status")=="reachable" and spark.get("status")=="running"
    if replaying:
        st.success(f"Replay aktif · Kafka published {fmt(replay.get('published',0))} · Spark batch {spark.get('last_batch_id','—')}")
    else:
        st.caption(f"Pipeline state · Replay {replay.get('status','not_started')} · Kafka {kafka.get('status','unknown')} · Spark {spark.get('status','not_started')}")
    if page in ("Dashboard","Capture Data","Visualization"):
        render_pipeline_animation(data)
    if page=="Dashboard": render_dashboard(data)
    elif page=="Capture Data": render_capture(data)
    elif page=="Event Stream": render_stream(data)
    elif page=="Analytics": render_analytics(data)
    elif page=="Pipeline": render_pipeline(data)
    elif page=="Database": render_database(data)
    elif page=="Visualization": render_visualization(data)
    elif page=="Tableau Exports": render_exports(data)

render_selected_page()
st.caption("Dataset: historical Retailrocket records · Dashboard auto-refreshes only during replay or Spark processing · Click events are unavailable in the source data.")
