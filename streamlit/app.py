# streamlit/app.py
import streamlit as st

st.set_page_config(
    page_title = "Weather Madiun Dashboard",
    page_icon  = "🌤️",
    layout     = "wide",
)

st.title("🌤️ Weather Madiun Dashboard")
st.markdown("""
Dashboard prakiraan cuaca **Kota Madiun** berbasis data BMKG.
Data diperbarui otomatis 2× sehari melalui pipeline Airflow.
""")

st.divider()

col1, col2, col3 = st.columns(3)

with col1:
    st.page_link("pages/1_Peta_Overview.py",      label="📍 Peta & Overview",        icon="📍")
with col2:
    st.page_link("pages/2_Comfort_Safety.py",     label="🌡️ Comfort & Safety",       icon="🌡️")
with col3:
    st.page_link("pages/3_Energy_Agriculture.py", label="⚡ Energy & Agriculture",   icon="⚡")

st.divider()

st.markdown("""
#### Tentang Project

Pipeline ini membangun data warehouse cuaca Kota Madiun menggunakan:
- **Apache Airflow** — orchestration & scheduling
- **RustFS** — Bronze layer (Parquet)
- **DuckDB** — Silver & Gold layer
- **dbt** — transformasi data
- **Streamlit + Plotly** — visualisasi

**Sumber data:** API BMKG (`api.bmkg.go.id`)  
**Scope:** 27 kelurahan, 3 kecamatan (Kartoharjo, Manguharjo, Taman)  
**Frekuensi update:** 06:00 WIB & 18:00 WIB
""")