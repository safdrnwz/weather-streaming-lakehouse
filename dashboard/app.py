"""
dashboard/app.py
================
Premium real-time weather dashboard for the streaming lakehouse.

Reads gold/silver from weather_warehouse and renders an attractive, live
dashboard (KPIs, alerts, map, gauges, trends). Run with:

    streamlit run dashboard/app.py

Keep realtime/orchestrator.py running so the numbers update live.
"""

import sys
from datetime import datetime
from pathlib import Path

import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))  # project root -> `common`
sys.path.insert(0, str(Path(__file__).resolve().parent))      # dashboard dir -> `queries`
import queries as q  # noqa: E402

from common import config as _cfg  # noqa: E402
from common import weather_rules as _wr  # noqa: E402

# ------------------------------------------------------------------ page setup
st.set_page_config(page_title="Weather Lakehouse", page_icon="🌦️", layout="wide")

ACCENT = "#38bdf8"
PALETTE = px.colors.qualitative.Vivid
TEMPLATE = "plotly_dark"

# Emoji per condition (used in cards / table).
EMOJI = {
    "Clear": "☀️", "Cloudy": "☁️", "Rain": "🌧️", "Rain showers": "🌧️",
    "Drizzle": "🌦️", "Thunderstorm": "⛈️", "Fog": "🌫️", "Snow": "❄️",
    "Snow showers": "🌨️", "Unknown": "❓",
}

st.markdown(
    """
    <style>
      .block-container {padding-top: 1.5rem; padding-bottom: 2rem; max-width: 1400px;}
      #hero {background: linear-gradient(110deg,#0ea5e9 0%,#6366f1 55%,#a855f7 100%);
             padding: 18px 24px; border-radius: 18px; margin-bottom: 14px;}
      #hero h1 {color:#fff; margin:0; font-size:1.9rem; letter-spacing:.4px;}
      #hero p {color:#e0f2fe; margin:.25rem 0 0; font-size:.92rem;}
      [data-testid="stMetric"] {
        background: linear-gradient(145deg,#1e2a44,#16203a);
        border:1px solid #2b3a5e; border-radius:16px; padding:14px 18px;}
      [data-testid="stMetricLabel"] {color:#9fb3d1;}
      .alert {background:linear-gradient(90deg,#7f1d1d,#b91c1c);color:#fff;
              padding:12px 18px;border-radius:12px;font-weight:600;margin-bottom:10px;}
      .citycard {background:linear-gradient(145deg,#172033,#10182b);border:1px solid #263350;
                 border-radius:14px;padding:12px 14px;text-align:center;}
      .citycard .c {font-size:.85rem;color:#9fb3d1;} .citycard .t {font-size:1.5rem;font-weight:700;color:#fff;}
      .citycard .e {font-size:1.6rem;}
    </style>
    """,
    unsafe_allow_html=True,
)


# ----------------------------------------------------------- cached data loads
@st.cache_data(ttl=10)
def d_latest():
    return q.city_latest()


@st.cache_data(ttl=10)
def d_full():
    return q.latest_full()


@st.cache_data(ttl=10)
def d_daily():
    return q.daily_stats()


@st.cache_data(ttl=10)
def d_hourly():
    return q.hourly_stats()


@st.cache_data(ttl=10)
def d_recent(h):
    return q.recent_readings(h)


# ------------------------------------------------------------------- sidebar
st.sidebar.header("⚙️ Controls")
auto = st.sidebar.toggle("🔴 Live (auto-refresh 10s)", value=True)
hours_back = st.sidebar.slider("Trend window (hours)", 1, 72, 12)
if st.sidebar.button("🔄 Refresh now"):
    st.cache_data.clear()
    st.rerun()
st.sidebar.caption(f"Source: **{_cfg.SOURCE_MODE} / {_cfg.SCENARIO}**\n\n"
                   f"bronze {_cfg.BRONZE_EVERY_SECONDS}s · silver "
                   f"{_cfg.SILVER_EVERY_SECONDS}s · gold {_cfg.GOLD_EVERY_SECONDS}s")


def render():
    latest = d_latest()
    if latest.empty:
        st.info("No data yet. Start **python realtime/orchestrator.py**, wait ~30s, "
                "then this fills in automatically.")
        return

    all_cities = sorted(latest["city"].unique())
    chosen = st.session_state.get("cities", all_cities)
    chosen = [c for c in chosen if c in all_cities] or all_cities
    latest = latest[latest["city"].isin(chosen)]

    # hero header
    st.markdown(
        f"<div id='hero'><h1>🌦️ Real-Time Weather Lakehouse</h1>"
        f"<p>Live medallion pipeline · {_cfg.SOURCE_MODE}/{_cfg.SCENARIO} · "
        f"updated {datetime.now():%H:%M:%S}</p></div>",
        unsafe_allow_html=True,
    )

    # heatwave alert banner
    hot = latest[latest["temperature_c"] > 40]
    if not hot.empty:
        names = ", ".join(f"{r.city} ({r.temperature_c:.0f}°)" for r in hot.itertuples())
        st.markdown(f"<div class='alert'>🔥 Heatwave alert — {names}</div>", unsafe_allow_html=True)

    # KPI row
    hottest = latest.loc[latest["temperature_c"].idxmax()]
    coolest = latest.loc[latest["temperature_c"].idxmin()]
    daily = d_daily()
    hw = 0
    if not daily.empty:
        today = daily["day"].max()
        hw = int(daily[(daily["day"] == today) & daily["city"].isin(chosen)]["heatwave"].sum())
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("🏙️ Cities live", len(latest))
    k2.metric("🔥 Hottest", hottest["city"], f"{hottest['temperature_c']:.1f} °C")
    k3.metric("❄️ Coolest", coolest["city"], f"{coolest['temperature_c']:.1f} °C")
    k4.metric("🌡️ Heatwave cities", hw)

    # city cards
    st.write("")
    cols = st.columns(len(latest))
    for col, r in zip(cols, latest.sort_values("temperature_c", ascending=False).itertuples(), strict=False):
        col.markdown(
            f"<div class='citycard'><div class='c'>{r.city}</div>"
            f"<div class='e'>{EMOJI.get(r.condition, '🌡️')}</div>"
            f"<div class='t'>{r.temperature_c:.1f}°</div>"
            f"<div class='c'>{r.condition}</div></div>",
            unsafe_allow_html=True,
        )

    st.divider()

    # Row: map + current temp bar
    m1, m2 = st.columns([1, 1])
    with m1:
        st.subheader("🗺️ City map")
        coords = latest.copy()
        coords["lat"] = coords["city"].map(lambda c: _wr.CITIES.get(c, (None, None))[0])
        coords["lon"] = coords["city"].map(lambda c: _wr.CITIES.get(c, (None, None))[1])
        coords = coords.dropna(subset=["lat", "lon"])
        if coords.empty:
            st.caption("No coordinates available.")
        else:
            fig = px.scatter_map(
                coords, lat="lat", lon="lon", color="temperature_c", size=[18] * len(coords),
                hover_name="city", color_continuous_scale="turbo", zoom=3.2,
                hover_data={"temperature_c": ":.1f", "lat": False, "lon": False},
            )
            fig.update_layout(map_style="carto-darkmatter", template=TEMPLATE, height=380,
                              margin=dict(l=0, r=0, t=0, b=0))
            st.plotly_chart(fig, use_container_width=True)

    with m2:
        st.subheader("🌡️ Current temperature by city")
        cur = latest.sort_values("temperature_c")
        fig = px.bar(cur, x="temperature_c", y="city", orientation="h",
                     color="temperature_c", color_continuous_scale="turbo", text="temperature_c")
        fig.update_traces(texttemplate="%{text:.1f}°", textposition="outside")
        fig.update_layout(template=TEMPLATE, height=380, coloraxis_showscale=False,
                          xaxis_title="°C", yaxis_title="", margin=dict(l=10, r=20, t=10, b=10))
        st.plotly_chart(fig, use_container_width=True)

    # Row: spotlight gauges for one city
    st.subheader("🎯 City spotlight")
    spot = st.selectbox("Pick a city", sorted(latest["city"].unique()), key="spot")
    full = d_full()
    row = full[full["city"] == spot]
    if not row.empty:
        r = row.iloc[0]
        g1, g2, g3 = st.columns(3)
        g1.plotly_chart(_gauge("Temperature °C", r["temperature_c"], 0, 50, ACCENT), use_container_width=True)
        g2.plotly_chart(_gauge("Humidity %", r["humidity_pct"], 0, 100, "#22d3ee"), use_container_width=True)
        g3.plotly_chart(_gauge("Wind km/h", r["wind_speed_kmh"], 0, 70, "#a78bfa"), use_container_width=True)

    st.divider()

    # Row: trend line
    st.subheader(f"📈 Temperature trend — last {hours_back}h")
    recent = d_recent(hours_back)
    recent = recent[recent["city"].isin(chosen)] if not recent.empty else recent
    if recent.empty:
        st.caption("Not enough history yet — let it run a while.")
    else:
        fig = px.line(recent, x="event_time", y="temperature_c", color="city",
                      color_discrete_sequence=PALETTE, markers=True)
        fig.update_layout(template=TEMPLATE, height=380, xaxis_title="", yaxis_title="°C",
                          legend_title="", margin=dict(l=10, r=10, t=10, b=10))
        st.plotly_chart(fig, use_container_width=True)

    # Row: daily max + hourly heatmap
    c3, c4 = st.columns(2)
    with c3:
        st.subheader("📊 Daily max temperature")
        if daily.empty:
            st.caption("No daily stats yet.")
        else:
            d = daily[daily["city"].isin(chosen)].copy()
            d["day"] = d["day"].dt.strftime("%Y-%m-%d")
            d["flag"] = d["heatwave"].map({True: "Heatwave >40°", False: "Normal"})
            fig = px.bar(d, x="city", y="max_temp", color="flag",
                         color_discrete_map={"Heatwave >40°": "#ef4444", "Normal": "#3b82f6"},
                         barmode="group", text="max_temp")
            fig.update_traces(texttemplate="%{text:.0f}°", textposition="outside")
            fig.update_layout(template=TEMPLATE, height=360, xaxis_title="", yaxis_title="°C",
                              legend_title="", margin=dict(l=10, r=10, t=10, b=10))
            st.plotly_chart(fig, use_container_width=True)
    with c4:
        st.subheader("🔥 Avg temperature by hour")
        hourly = d_hourly()
        if hourly.empty:
            st.caption("No hourly stats yet.")
        else:
            h = hourly[hourly["city"].isin(chosen)].copy()
            h["hour"] = h["hour_start"].dt.strftime("%m-%d %H:00")
            pivot = h.pivot_table(index="city", columns="hour", values="avg_temp")
            fig = go.Figure(go.Heatmap(z=pivot.values, x=list(pivot.columns), y=list(pivot.index),
                                       colorscale="turbo", colorbar=dict(title="°C")))
            fig.update_layout(template=TEMPLATE, height=360, margin=dict(l=10, r=10, t=10, b=10))
            st.plotly_chart(fig, use_container_width=True)

    st.caption("Bronze (raw) → Silver (clean, date-partitioned) → Gold (current + aggregates) "
               "• temperature shown as current / avg / max (never summed).")


def _gauge(title, value, lo, hi, color):
    """A premium plotly gauge indicator."""
    return go.Figure(go.Indicator(
        mode="gauge+number", value=float(value), title={"text": title},
        gauge={"axis": {"range": [lo, hi]}, "bar": {"color": color},
               "bgcolor": "#0f172a", "borderwidth": 0},
    )).update_layout(template=TEMPLATE, height=240, margin=dict(l=20, r=20, t=50, b=10))


# city filter in sidebar (after we can read cities)
_lat = d_latest()
if not _lat.empty:
    st.session_state["cities"] = st.sidebar.multiselect(
        "Cities", sorted(_lat["city"].unique()),
        default=st.session_state.get("cities", sorted(_lat["city"].unique())),
    )

# Live auto-refresh: re-run the body every 10s when enabled (Streamlit fragment).
if auto:
    try:
        render = st.fragment(run_every=10)(render)
    except TypeError:
        pass  # older Streamlit without run_every -> manual refresh still works

render()
