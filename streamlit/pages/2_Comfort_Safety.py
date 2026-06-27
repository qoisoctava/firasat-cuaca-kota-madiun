# streamlit/pages/2_Comfort_Safety.py
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import sys

sys.path.insert(0, "/opt/streamlit/app")
from utils.db import get_locations, get_comfort_timeseries

st.set_page_config(page_title="Comfort & Safety", page_icon="🌡️", layout="wide")

st.title("🌡️ Comfort & Safety")
st.markdown("Analisis kenyamanan cuaca berdasarkan heat index per lokasi dan waktu.")

# ── Load data ────────────────────────────────────────────────────────────────
@st.cache_data(ttl=1800)
def load_locations():
    return get_locations()

locations = load_locations()

if locations.empty:
    st.warning("Belum ada data. Pastikan pipeline sudah dijalankan.")
    st.stop()

# ── Filter ───────────────────────────────────────────────────────────────────
col_filter1, col_filter2 = st.columns([1, 3])

with col_filter1:
    kecamatan_list = ["Semua"] + sorted(locations["kecamatan"].unique().tolist())
    selected_kec   = st.selectbox("Filter Kecamatan", kecamatan_list)

if selected_kec == "Semua":
    filtered_locations = locations
else:
    filtered_locations = locations[locations["kecamatan"] == selected_kec]

with col_filter2:
    location_options = {
        f"{row['location_name']} ({row['kecamatan']})": row["location_id"]
        for _, row in filtered_locations.iterrows()
    }
    selected_label    = st.selectbox("Pilih Kelurahan", list(location_options.keys()))
    selected_location = location_options[selected_label]

# ── Load timeseries ──────────────────────────────────────────────────────────
@st.cache_data(ttl=1800)
def load_timeseries(location_id):
    return get_comfort_timeseries(location_id)

df = load_timeseries(selected_location)

if df.empty:
    st.warning("Tidak ada data untuk kelurahan ini.")
    st.stop()

# Buat kolom datetime untuk sumbu X
df["datetime"] = df["date"].astype(str) + " " + df["hour"].astype(str).str.zfill(2) + ":00"

st.divider()

# ── Heat Index timeseries ─────────────────────────────────────────────────────
st.subheader(f"Heat Index — {selected_label}")

COLOR_MAP = {
    "Nyaman":      "#2ecc71",
    "Panas":       "#f39c12",
    "Sangat Panas":"#e67e22",
    "Berbahaya":   "#e74c3c",
    "Ekstrem":     "#8e44ad",
}

fig_heat = go.Figure()

# Plot heat index sebagai line
fig_heat.add_trace(go.Scatter(
    x          = df["datetime"],
    y          = df["heat_index"],
    mode       = "lines+markers",
    name       = "Heat Index",
    line       = dict(color="#e74c3c", width=2),
    marker     = dict(
        color  = [COLOR_MAP.get(c, "#95a5a6") for c in df["comfort_label"]],
        size   = 10,
    ),
    hovertemplate = (
        "<b>%{x}</b><br>"
        "Heat Index: %{y:.1f} °C<br>"
        "<extra></extra>"
    ),
))

# Plot suhu sebagai line terpisah
fig_heat.add_trace(go.Scatter(
    x          = df["datetime"],
    y          = df["temperature"],
    mode       = "lines",
    name       = "Suhu Udara",
    line       = dict(color="#3498db", width=1.5, dash="dot"),
    hovertemplate = (
        "<b>%{x}</b><br>"
        "Suhu: %{y:.1f} °C<br>"
        "<extra></extra>"
    ),
))

# Tambah threshold lines
fig_heat.add_hline(y=27, line_dash="dash", line_color="#f39c12",
                   annotation_text="Panas (27°C)")
fig_heat.add_hline(y=32, line_dash="dash", line_color="#e67e22",
                   annotation_text="Sangat Panas (32°C)")
fig_heat.add_hline(y=41, line_dash="dash", line_color="#e74c3c",
                   annotation_text="Berbahaya (41°C)")

fig_heat.update_layout(
    height      = 400,
    xaxis_title = "Waktu",
    yaxis_title = "Temperatur (°C)",
    legend      = dict(orientation="h", y=1.1),
    hovermode   = "x unified",
)
st.plotly_chart(fig_heat, use_container_width=True)

st.divider()

# ── Dua kolom: kelembapan & comfort label ────────────────────────────────────
col_left, col_right = st.columns(2)

with col_left:
    st.subheader("Kelembapan & Kecepatan Angin")

    fig_hum = go.Figure()
    fig_hum.add_trace(go.Bar(
        x          = df["datetime"],
        y          = df["humidity"],
        name       = "Kelembapan (%)",
        marker_color = "#3498db",
        opacity    = 0.7,
    ))
    fig_hum.add_trace(go.Scatter(
        x          = df["datetime"],
        y          = df["wind_speed"],
        name       = "Kecepatan Angin (km/jam)",
        mode       = "lines+markers",
        line       = dict(color="#e67e22", width=2),
        yaxis      = "y2",
    ))
    fig_hum.update_layout(
        height  = 350,
        yaxis   = dict(title="Kelembapan (%)"),
        yaxis2  = dict(
            title    = "Kecepatan Angin (km/jam)",
            overlaying = "y",
            side     = "right",
        ),
        legend  = dict(orientation="h", y=1.1),
        hovermode = "x unified",
    )
    st.plotly_chart(fig_hum, use_container_width=True)

with col_right:
    st.subheader("Distribusi Comfort Label")

    comfort_counts = df["comfort_label"].value_counts().reset_index()
    comfort_counts.columns = ["Kondisi", "Jumlah Jam"]

    fig_comfort = px.bar(
        comfort_counts,
        x     = "Kondisi",
        y     = "Jumlah Jam",
        color = "Kondisi",
        color_discrete_map = COLOR_MAP,
        title = "Frekuensi Kondisi Kenyamanan",
        text_auto = True,
    )
    fig_comfort.update_layout(
        height          = 350,
        showlegend      = False,
        xaxis_title     = "",
    )
    st.plotly_chart(fig_comfort, use_container_width=True)

st.divider()

# ── Tabel detail ─────────────────────────────────────────────────────────────
with st.expander("Lihat Data Detail"):
    st.dataframe(
        df[["datetime", "temperature", "humidity",
            "heat_index", "comfort_label", "wind_speed", "time_of_day"]]
          .rename(columns={
              "datetime":     "Waktu",
              "temperature":  "Suhu (°C)",
              "humidity":     "Kelembapan (%)",
              "heat_index":   "Heat Index (°C)",
              "comfort_label":"Kondisi",
              "wind_speed":   "Angin (km/jam)",
              "time_of_day":  "Waktu Hari",
          }),
        use_container_width = True,
        hide_index          = True,
    )