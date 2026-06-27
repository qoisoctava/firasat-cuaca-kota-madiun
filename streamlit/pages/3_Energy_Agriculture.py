# streamlit/pages/3_Energy_Agriculture.py
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import sys

sys.path.insert(0, "/opt/streamlit/app")
from utils.db import get_energy_summary, get_agriculture_summary

st.set_page_config(page_title="Energy & Agriculture", page_icon="⚡", layout="wide")

st.title("⚡ Energy & Agriculture Proxy")
st.markdown("Estimasi potensi energi terbarukan dan kondisi pertanian per kelurahan.")

# ── Load data ────────────────────────────────────────────────────────────────
@st.cache_data(ttl=1800)
def load_energy():
    return get_energy_summary()

@st.cache_data(ttl=1800)
def load_agriculture():
    return get_agriculture_summary()

df_energy = load_energy()
df_agri   = load_agriculture()

if df_energy.empty or df_agri.empty:
    st.warning("Belum ada data. Pastikan pipeline sudah dijalankan.")
    st.stop()

# ── Tab layout ───────────────────────────────────────────────────────────────
tab_energy, tab_agri = st.tabs(["☀️ Energy Proxy", "🌾 Agriculture Proxy"])


# ════════════════════════════════════════════════════════════════════════════
# TAB 1 — Energy Proxy
# ════════════════════════════════════════════════════════════════════════════
with tab_energy:

    st.subheader("Potensi Energi Terbarukan per Kelurahan")

    # ── Metric cards ─────────────────────────────────────────────────────
    col1, col2, col3 = st.columns(3)

    total = len(df_energy)
    solar_tinggi = df_energy["solar_tinggi"].sum()
    wind_kencang = df_energy["wind_kencang"].sum()

    with col1:
        st.metric(
            label = "Rata-rata Tutupan Awan",
            value = f"{df_energy['avg_cloud_cover'].mean():.1f} %",
        )
    with col2:
        st.metric(
            label = "Rata-rata Kecepatan Angin",
            value = f"{df_energy['avg_wind_speed'].mean():.1f} km/jam",
        )
    with col3:
        st.metric(
            label = "Rata-rata Jarak Pandang",
            value = f"{df_energy['avg_visibility_km'].mean():.1f} km",
        )

    st.divider()

    col_map, col_chart = st.columns([2, 1])

    # ── Peta solar proxy ─────────────────────────────────────────────────
    with col_map:
        st.markdown("**Peta Potensi Solar (rata-rata tutupan awan)**")

        fig_solar_map = px.scatter_mapbox(
            df_energy,
            lat           = "latitude",
            lon           = "longitude",
            color         = "avg_cloud_cover",
            size          = "avg_wind_speed",
            hover_name    = "location_name",
            hover_data    = {
                "kecamatan":        True,
                "avg_cloud_cover":  ":.1f",
                "avg_wind_speed":   ":.1f",
                "solar_tinggi":     True,
                "wind_kencang":     True,
                "latitude":         False,
                "longitude":        False,
            },
            color_continuous_scale = "RdYlGn",   # hijau = sedikit awan = potensi solar tinggi
            size_max      = 20,
            zoom          = 12,
            center        = {"lat": -7.63, "lon": 111.52},
            mapbox_style  = "open-street-map",
        )
        fig_solar_map.update_layout(
            height = 420,
            margin = {"r": 0, "t": 0, "l": 0, "b": 0},
            coloraxis_colorbar = dict(title="Awan (%)"),
        )
        st.plotly_chart(fig_solar_map, use_container_width=True)

    # ── Solar vs Wind proxy bar ──────────────────────────────────────────
    with col_chart:
        st.markdown("**Solar Proxy per Kecamatan**")

        kec_solar = df_energy.groupby("kecamatan")[
            ["solar_tinggi", "solar_sedang", "solar_rendah"]
        ].sum().reset_index()

        fig_solar = go.Figure()
        fig_solar.add_trace(go.Bar(
            name         = "Tinggi",
            x            = kec_solar["kecamatan"],
            y            = kec_solar["solar_tinggi"],
            marker_color = "#f1c40f",
        ))
        fig_solar.add_trace(go.Bar(
            name         = "Sedang",
            x            = kec_solar["kecamatan"],
            y            = kec_solar["solar_sedang"],
            marker_color = "#f39c12",
        ))
        fig_solar.add_trace(go.Bar(
            name         = "Rendah",
            x            = kec_solar["kecamatan"],
            y            = kec_solar["solar_rendah"],
            marker_color = "#7f8c8d",
        ))
        fig_solar.update_layout(
            barmode     = "stack",
            height      = 200,
            margin      = {"t": 10, "b": 10},
            legend      = dict(orientation="h", y=-0.3),
            xaxis_title = "",
            yaxis_title = "Jumlah jam",
        )
        st.plotly_chart(fig_solar, use_container_width=True)

        st.markdown("**Wind Proxy per Kecamatan**")

        kec_wind = df_energy.groupby("kecamatan")[
            ["wind_kencang", "wind_sedang", "wind_lemah"]
        ].sum().reset_index()

        fig_wind = go.Figure()
        fig_wind.add_trace(go.Bar(
            name         = "Kencang",
            x            = kec_wind["kecamatan"],
            y            = kec_wind["wind_kencang"],
            marker_color = "#2980b9",
        ))
        fig_wind.add_trace(go.Bar(
            name         = "Sedang",
            x            = kec_wind["kecamatan"],
            y            = kec_wind["wind_sedang"],
            marker_color = "#85c1e9",
        ))
        fig_wind.add_trace(go.Bar(
            name         = "Lemah",
            x            = kec_wind["kecamatan"],
            y            = kec_wind["wind_lemah"],
            marker_color = "#d6eaf8",
        ))
        fig_wind.update_layout(
            barmode     = "stack",
            height      = 200,
            margin      = {"t": 10, "b": 10},
            legend      = dict(orientation="h", y=-0.3),
            xaxis_title = "",
            yaxis_title = "Jumlah jam",
        )
        st.plotly_chart(fig_wind, use_container_width=True)


# ════════════════════════════════════════════════════════════════════════════
# TAB 2 — Agriculture Proxy
# ════════════════════════════════════════════════════════════════════════════
with tab_agri:

    st.subheader("Kondisi Pertanian per Kelurahan")

    # ── Metric cards ─────────────────────────────────────────────────────
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric(
            label = "Rata-rata Suhu",
            value = f"{df_agri['avg_temperature'].mean():.1f} °C",
        )
    with col2:
        st.metric(
            label = "Rata-rata Kelembapan",
            value = f"{df_agri['avg_humidity'].mean():.1f} %",
        )
    with col3:
        rain_pct = (df_agri["rain_hours"].sum() / df_agri["total_hours"].sum() * 100)
        st.metric(
            label = "Persentase Jam Hujan",
            value = f"{rain_pct:.1f} %",
        )
    with col4:
        irr_pct = (df_agri["irrigation_hours"].sum() / df_agri["total_hours"].sum() * 100)
        st.metric(
            label = "Persentase Butuh Irigasi",
            value = f"{irr_pct:.1f} %",
        )

    st.divider()

    col_map2, col_chart2 = st.columns([2, 1])

    # ── Peta irrigation flag ──────────────────────────────────────────────
    with col_map2:
        st.markdown("**Peta Kebutuhan Irigasi per Kelurahan**")

        df_agri["irrigation_pct"] = (
            df_agri["irrigation_hours"] / df_agri["total_hours"] * 100
        ).round(1)

        fig_agri_map = px.scatter_mapbox(
            df_agri,
            lat           = "latitude",
            lon           = "longitude",
            color         = "irrigation_pct",
            size          = "irrigation_pct",
            hover_name    = "location_name",
            hover_data    = {
                "kecamatan":       True,
                "avg_temperature": ":.1f",
                "avg_humidity":    ":.1f",
                "rain_hours":      True,
                "irrigation_pct":  ":.1f",
                "latitude":        False,
                "longitude":       False,
            },
            color_continuous_scale = "YlOrRd",   # kuning = rendah, merah = butuh irigasi
            size_max      = 25,
            zoom          = 12,
            center        = {"lat": -7.63, "lon": 111.52},
            mapbox_style  = "open-street-map",
            labels        = {"irrigation_pct": "Irigasi (%)"},
        )
        fig_agri_map.update_layout(
            height = 420,
            margin = {"r": 0, "t": 0, "l": 0, "b": 0},
            coloraxis_colorbar = dict(title="Irigasi (%)"),
        )
        st.plotly_chart(fig_agri_map, use_container_width=True)

    # ── Rain vs Irrigation bar ────────────────────────────────────────────
    with col_chart2:
        st.markdown("**Hujan vs Irigasi per Kecamatan**")

        kec_agri = df_agri.groupby("kecamatan")[
            ["rain_hours", "irrigation_hours", "total_hours"]
        ].sum().reset_index()

        fig_rain = go.Figure()
        fig_rain.add_trace(go.Bar(
            name         = "Jam Hujan",
            x            = kec_agri["kecamatan"],
            y            = kec_agri["rain_hours"],
            marker_color = "#3498db",
        ))
        fig_rain.add_trace(go.Bar(
            name         = "Butuh Irigasi",
            x            = kec_agri["kecamatan"],
            y            = kec_agri["irrigation_hours"],
            marker_color = "#e67e22",
        ))
        fig_rain.update_layout(
            barmode     = "group",
            height      = 280,
            margin      = {"t": 10, "b": 10},
            legend      = dict(orientation="h", y=-0.3),
            xaxis_title = "",
            yaxis_title = "Jumlah jam",
        )
        st.plotly_chart(fig_rain, use_container_width=True)

        st.markdown("**Kelembapan & Curah Hujan**")
        fig_hum = px.scatter(
            df_agri,
            x          = "avg_humidity",
            y          = "avg_precipitation",
            color      = "kecamatan",
            hover_name = "location_name",
            size       = "rain_hours",
            labels     = {
                "avg_humidity":      "Kelembapan Rata-rata (%)",
                "avg_precipitation": "Curah Hujan Rata-rata (mm)",
                "kecamatan":         "Kecamatan",
            },
        )
        fig_hum.update_layout(
            height = 280,
            margin = {"t": 10, "b": 10},
        )
        st.plotly_chart(fig_hum, use_container_width=True)

    # ── Tabel detail ──────────────────────────────────────────────────────
    with st.expander("Lihat Data Detail"):
        st.dataframe(
            df_agri[[
                "location_name", "kecamatan", "avg_temperature",
                "avg_humidity", "avg_precipitation",
                "rain_hours", "irrigation_hours", "total_hours", "irrigation_pct"
            ]].rename(columns={
                "location_name":     "Kelurahan",
                "kecamatan":         "Kecamatan",
                "avg_temperature":   "Suhu (°C)",
                "avg_humidity":      "Kelembapan (%)",
                "avg_precipitation": "Hujan (mm)",
                "rain_hours":        "Jam Hujan",
                "irrigation_hours":  "Jam Irigasi",
                "total_hours":       "Total Jam",
                "irrigation_pct":    "% Irigasi",
            }),
            use_container_width = True,
            hide_index          = True,
        )