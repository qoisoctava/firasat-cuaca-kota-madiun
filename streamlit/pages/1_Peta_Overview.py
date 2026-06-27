# streamlit/pages/1_Peta_Overview.py
import streamlit as st
import plotly.express as px
import sys

sys.path.insert(0, "/opt/streamlit/app")
from utils.db import get_latest_forecast, get_locations

st.set_page_config(page_title="Peta & Overview", page_icon="📍", layout="wide")

st.title("📍 Peta & Overview Cuaca Kota Madiun")
st.markdown("Prakiraan cuaca terkini per kelurahan.")

# ── Load data ────────────────────────────────────────────────────────────────
@st.cache_data(ttl=1800)   # cache 30 menit, refresh setiap pipeline jalan
def load_data():
    return get_latest_forecast()

df = load_data()

if df.empty:
    st.warning("Belum ada data. Pastikan pipeline sudah dijalankan.")
    st.stop()

# ── Metric cards ─────────────────────────────────────────────────────────────
st.subheader("Ringkasan")

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric(
        label = "Suhu Rata-rata",
        value = f"{df['temperature'].mean():.1f} °C",
        delta = f"max {df['temperature'].max():.1f} °C",
    )
with col2:
    st.metric(
        label = "Kelembapan Rata-rata",
        value = f"{df['humidity'].mean():.1f} %",
    )
with col3:
    st.metric(
        label = "Heat Index Rata-rata",
        value = f"{df['heat_index'].mean():.1f} °C",
    )
with col4:
    most_common = df['comfort_label'].value_counts().index[0]
    st.metric(
        label = "Kondisi Dominan",
        value = most_common,
    )

st.divider()

# ── Peta ─────────────────────────────────────────────────────────────────────
st.subheader("Peta Suhu per Kelurahan")

col_map, col_info = st.columns([2, 1])

with col_map:
    fig_map = px.scatter_mapbox(
        df,
        lat           = "latitude",
        lon           = "longitude",
        color         = "temperature",
        size          = "heat_index",
        hover_name    = "location_name",
        hover_data    = {
            "kecamatan":     True,
            "temperature":   True,
            "humidity":      True,
            "heat_index":    True,
            "comfort_label": True,
            "weather_desc_en": True,
            "latitude":      False,
            "longitude":     False,
        },
        color_continuous_scale = "RdYlGn_r",   # merah = panas, hijau = sejuk
        size_max      = 20,
        zoom          = 12,
        center        = {"lat": -7.63, "lon": 111.52},
        mapbox_style  = "open-street-map",
        title         = "Suhu Udara (°C)",
    )
    fig_map.update_layout(
        height          = 500,
        margin          = {"r": 0, "t": 40, "l": 0, "b": 0},
        coloraxis_colorbar = dict(title="°C"),
    )
    st.plotly_chart(fig_map, use_container_width=True)

with col_info:
    st.markdown("**Detail per Kelurahan**")
    st.dataframe(
        df[["location_name", "kecamatan", "temperature", "humidity",
            "heat_index", "comfort_label", "weather_desc_en"]]
          .rename(columns={
              "location_name":  "Kelurahan",
              "kecamatan":      "Kecamatan",
              "temperature":    "Suhu (°C)",
              "humidity":       "Kelembapan (%)",
              "heat_index":     "Heat Index",
              "comfort_label":  "Kondisi",
              "weather_desc_en":"Cuaca",
          })
          .sort_values("Suhu (°C)", ascending=False),
        use_container_width = True,
        hide_index          = True,
        height              = 460,
    )

st.divider()

# ── Distribusi comfort label ──────────────────────────────────────────────────
st.subheader("Distribusi Kondisi Kenyamanan")

col_pie, col_bar = st.columns(2)

with col_pie:
    comfort_counts = df['comfort_label'].value_counts().reset_index()
    comfort_counts.columns = ['Kondisi', 'Jumlah Kelurahan']

    fig_pie = px.pie(
        comfort_counts,
        names  = "Kondisi",
        values = "Jumlah Kelurahan",
        color  = "Kondisi",
        color_discrete_map = {
            "Nyaman":      "#2ecc71",
            "Panas":       "#f39c12",
            "Sangat Panas":"#e67e22",
            "Berbahaya":   "#e74c3c",
            "Ekstrem":     "#8e44ad",
        },
        title  = "Proporsi Kondisi per Kelurahan",
    )
    st.plotly_chart(fig_pie, use_container_width=True)

with col_bar:
    kec_temp = df.groupby("kecamatan")["temperature"].mean().reset_index()
    kec_temp.columns = ["Kecamatan", "Suhu Rata-rata (°C)"]

    fig_bar = px.bar(
        kec_temp,
        x     = "Kecamatan",
        y     = "Suhu Rata-rata (°C)",
        color = "Suhu Rata-rata (°C)",
        color_continuous_scale = "RdYlGn_r",
        title = "Suhu Rata-rata per Kecamatan",
        text_auto = ".1f",
    )
    fig_bar.update_layout(coloraxis_showscale=False)
    st.plotly_chart(fig_bar, use_container_width=True)
    