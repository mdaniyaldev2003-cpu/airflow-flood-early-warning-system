import streamlit as st
import pandas as pd
import mysql.connector
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import requests
import time
from requests.auth import HTTPBasicAuth

# ---------- PAGE CONFIG ----------
st.set_page_config(
    page_title="Flood Risk Monitor - Pakistan",
    page_icon="🌊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ---------- CUSTOM CSS FOR BETTER UI ----------
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        color: #1e3c72;
        text-align: center;
        margin-bottom: 1rem;
    }
    .sub-header {
        font-size: 1.2rem;
        color: #2a5298;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        border-radius: 10px;
        padding: 1rem;
        text-align: center;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    .risk-high {
        color: #ff0000;
        font-weight: bold;
    }
    .risk-medium {
        color: #ffa500;
        font-weight: bold;
    }
    .risk-low {
        color: #008000;
        font-weight: bold;
    }
</style>
""", unsafe_allow_html=True)

# ---------- HEADER ----------
st.markdown('<div class="main-header">🌊 Pakistan Flood Risk Monitoring Dashboard</div>', unsafe_allow_html=True)
st.markdown('<div class="sub-header">Real-time analysis for major cities | Data updates daily at 9 AM</div>', unsafe_allow_html=True)

# ---------- AIRFLOW TRIGGER FUNCTION ----------
def run_airflow_dag():
    with st.spinner("🔄 Running Airflow pipeline, fetching new flood data..."):
        try:
            # Airflow API endpoint for triggering the DAG
            dag_id = "flood_risk_monitor_pakistan"
            trigger_url = f"http://localhost:8080/api/v1/dags/{dag_id}/dagRuns"

            # Use your Airflow login credentials
            response = requests.post(
                trigger_url,
                auth=HTTPBasicAuth('api_user', 'api_pass'),
                json={},
                timeout=30
            )

            if response.status_code == 200:
                st.success("✅ DAG triggered successfully! New data is being fetched...")
            else:
                st.error(f"❌ Failed to trigger DAG: {response.status_code}")
                return None

        except Exception as e:
            st.error(f"⚠️ Could not reach Airflow API: {e}")
            st.info("Please ensure Airflow is running (docker-compose up -d)")
            return None

    # Wait for data to be processed, then refresh
    with st.spinner("⏳ Processing new data, please wait..."):
        time.sleep(15)   # Wait for DAG to run
    st.rerun()

# ---------- SIDEBAR FILTERS + DEPLOY BUTTON ----------
st.sidebar.markdown("## 🔍 Filters & Controls")
st.sidebar.markdown("Use below filters to narrow down results:")

# Deploy / Refresh button at the top of sidebar
if st.sidebar.button("🚀 Deploy / Refresh Data", use_container_width=True):
    run_airflow_dag()

st.sidebar.markdown("---")

# ---------- DATABASE CONNECTION ----------
@st.cache_resource
def init_connection():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="D@ni2003",
        database="flood_db",
        port=3307
    )

@st.cache_data(ttl=600)
def load_data():
    conn = init_connection()
    query = "SELECT * FROM flood_risk ORDER BY timestamp DESC"
    df = pd.read_sql(query, conn)
    conn.close()
    
    # Convert timestamp to datetime
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    # Add a "risk level description" column for tooltips
    risk_desc = {
        'HIGH': '⚠️ Immediate action required. Possible flooding expected.',
        'MEDIUM': '🟠 Stay alert. Monitor weather updates.',
        'LOW': '✅ No immediate threat. Routine monitoring.'
    }
    df['risk_description'] = df['risk_level'].map(risk_desc)
    
    return df

# ---------- LOAD DATA WITH ERROR HANDLING ----------
try:
    df = load_data()
    if df.empty:
        st.warning("⚠️ Database mein abhi koi data nahi hai. Pehle Airflow DAG trigger karo (Manual trigger from UI).")
        st.info("**How to trigger?** Go to Airflow UI → find 'flood_risk_monitor_pakistan' → click ▶️ trigger DAG.")
        st.stop()
except Exception as e:
    st.error(f"❌ Database connection failed: {e}")
    st.info("**Solution:** Ensure MySQL container is running: `docker-compose up -d`")
    st.stop()

# City filter
city_options = ['All'] + sorted(df['city'].unique())
selected_city = st.sidebar.selectbox(
    "🏙️ Select City",
    city_options,
    help="Choose a specific city or keep 'All' to see whole Pakistan data"
)

# Risk level filter (with color coding in UI)
risk_options = ['LOW', 'MEDIUM', 'HIGH']
risk_labels = {
    'LOW': '🟢 LOW - No immediate threat',
    'MEDIUM': '🟠 MEDIUM - Stay alert',
    'HIGH': '🔴 HIGH - Immediate action required'
}
selected_risks = st.sidebar.multiselect(
    "⚠️ Risk Level",
    options=risk_options,
    default=risk_options,
    format_func=lambda x: risk_labels[x],
    help="Filter by risk severity. HIGH risk cities need urgent attention."
)

# Date range
min_date = df['timestamp'].min().date()
max_date = df['timestamp'].max().date()
date_range = st.sidebar.date_input(
    "📅 Date Range",
    value=[min_date, max_date],
    min_value=min_date,
    max_value=max_date,
    help="Select start and end date to see historical trends"
)

# ---------- APPLY FILTERS ----------
filtered_df = df.copy()
if selected_city != 'All':
    filtered_df = filtered_df[filtered_df['city'] == selected_city]
filtered_df = filtered_df[filtered_df['risk_level'].isin(selected_risks)]
if len(date_range) == 2:
    filtered_df = filtered_df[
        (filtered_df['timestamp'].dt.date >= date_range[0]) & 
        (filtered_df['timestamp'].dt.date <= date_range[1])
    ]

# Show filter summary
st.sidebar.markdown("---")
st.sidebar.markdown(f"**Showing:** {len(filtered_df)} records")

# ---------- MAIN METRICS (KPI Cards) ----------
st.markdown("## 📈 Key Performance Indicators")
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.markdown('<div class="metric-card">', unsafe_allow_html=True)
    st.metric("Total Records", len(filtered_df))
    st.caption("Number of daily risk assessments")
    st.markdown('</div>', unsafe_allow_html=True)

with col2:
    st.markdown('<div class="metric-card">', unsafe_allow_html=True)
    st.metric("Cities Covered", filtered_df['city'].nunique())
    st.caption("Major Pakistani cities monitored")
    st.markdown('</div>', unsafe_allow_html=True)

with col3:
    st.markdown('<div class="metric-card">', unsafe_allow_html=True)
    avg_risk = round(filtered_df['risk_score'].mean(), 1) if not filtered_df.empty else 0
    st.metric("Average Risk Score", avg_risk, delta=None)
    st.caption("Higher score = higher flood probability")
    st.markdown('</div>', unsafe_allow_html=True)

with col4:
    st.markdown('<div class="metric-card">', unsafe_allow_html=True)
    high_risk_count = filtered_df[filtered_df['risk_level'] == 'HIGH'].shape[0]
    st.metric("High Risk Incidents", high_risk_count, delta=None)
    st.caption("Days when risk was HIGH")
    st.markdown('</div>', unsafe_allow_html=True)

st.markdown("---")

# ---------- CHART 1: Risk Score by City ----------
st.markdown("## 🎯 Risk Score by City")
st.markdown("Higher bars indicate greater flood risk. RED = HIGH, ORANGE = MEDIUM, GREEN = LOW")
if not filtered_df.empty:
    city_risk = filtered_df.groupby('city')['risk_score'].mean().sort_values(ascending=False).reset_index()
    def get_color(score):
        if score >= 7: return 'red'
        elif score >= 4: return 'orange'
        else: return 'green'
    city_risk['color'] = city_risk['risk_score'].apply(get_color)
    
    fig1 = px.bar(
        city_risk, 
        x='city', 
        y='risk_score',
        color='color',
        color_discrete_map={'red':'red', 'orange':'orange', 'green':'green'},
        title="Average Flood Risk Score",
        labels={'risk_score': 'Risk Score (0-10)', 'city': 'City'},
        text='risk_score'
    )
    fig1.update_traces(textposition='outside')
    fig1.update_layout(showlegend=False, height=500)
    st.plotly_chart(fig1, use_container_width=True)
else:
    st.info("No data after applying filters. Adjust filters to see results.")

# ---------- CHART 2: Temperature vs Risk ----------
st.markdown("## 🌡️ Temperature & Humidity Impact on Risk")
st.markdown("Hover over points to see city and risk level. Bigger dots = higher humidity.")
col1, col2 = st.columns([2, 1])
with col1:
    fig2 = px.scatter(
        filtered_df, 
        x='temperature', 
        y='risk_score',
        size='humidity',
        color='risk_level',
        hover_name='city',
        title="Risk Score vs Temperature",
        labels={'temperature': 'Temperature (°C)', 'risk_score': 'Risk Score'},
        color_discrete_map={'HIGH':'red', 'MEDIUM':'orange', 'LOW':'green'}
        # trendline removed because statsmodels may not be installed; can be added later
    )
    fig2.update_layout(height=500)
    st.plotly_chart(fig2, use_container_width=True)

with col2:
    st.markdown("### 🔍 Insight")
    st.markdown("""
    - **High Temperature (>35°C)** + **High Humidity (>80%)** = HIGH risk
    - **Rainfall >5mm** adds +4 to risk score
    """)

# ---------- CHART 3: Risk Level Distribution ----------
st.markdown("## 🥧 Risk Level Distribution")
col1, col2 = st.columns(2)
with col1:
    risk_counts = filtered_df['risk_level'].value_counts().reset_index()
    risk_counts.columns = ['Risk Level', 'Count']
    fig3 = px.pie(
        risk_counts, 
        values='Count', 
        names='Risk Level',
        title="Proportion of Assessments by Risk",
        color='Risk Level',
        color_discrete_map={'HIGH':'red', 'MEDIUM':'orange', 'LOW':'green'},
        hole=0.4
    )
    fig3.update_traces(textposition='inside', textinfo='percent+label')
    st.plotly_chart(fig3, use_container_width=True)

with col2:
    st.markdown("### 📌 What do these mean?")
    st.markdown("""
    - 🟢 **LOW** : Risk score <4 → Routine monitoring only  
    - 🟠 **MEDIUM** : Score 4-6 → Stay alert, watch weather  
    - 🔴 **HIGH** : Score ≥7 → Immediate action recommended  
    """)

# ---------- CHART 4: Time Series (if multiple days) ----------
if filtered_df['timestamp'].dt.date.nunique() > 1:
    st.markdown("## 📅 Risk Trend Over Time")
    st.markdown("How risk score has changed day by day (averaged across cities)")
    daily_risk = filtered_df.groupby(filtered_df['timestamp'].dt.date)['risk_score'].mean().reset_index()
    daily_risk.columns = ['Date', 'Avg Risk Score']
    fig4 = px.line(
        daily_risk, 
        x='Date', 
        y='Avg Risk Score',
        markers=True,
        title="Daily Average Risk Score",
        labels={'Avg Risk Score': 'Average Risk Score (0-10)'}
    )
    fig4.add_hline(y=7, line_dash="dash", line_color="red", annotation_text="HIGH risk threshold")
    fig4.add_hline(y=4, line_dash="dash", line_color="orange", annotation_text="MEDIUM threshold")
    st.plotly_chart(fig4, use_container_width=True)

# ---------- TABLE: Latest Data ----------
st.markdown("## 📋 Detailed Data Table")
st.markdown("Click column headers to sort. Use search box to find specific city or date.")
st.dataframe(
    filtered_df[['city', 'temperature', 'humidity', 'rainfall', 'risk_score', 'risk_level', 'risk_description', 'timestamp']],
    use_container_width=True,
    hide_index=True,
    column_config={
        'risk_level': st.column_config.TextColumn('Risk Level', help='HIGH/MEDIUM/LOW'),
        'risk_description': st.column_config.TextColumn('Explanation', help='What this risk level means'),
        'temperature': st.column_config.NumberColumn('Temp (°C)', format="%.1f"),
        'humidity': st.column_config.NumberColumn('Humidity (%)', format="%.0f"),
        'rainfall': st.column_config.NumberColumn('Rainfall (mm)', format="%.1f"),
        'risk_score': st.column_config.NumberColumn('Risk Score', format="%.0f"),
        'timestamp': st.column_config.DatetimeColumn('Date & Time')
    }
)

# ---------- DOWNLOAD BUTTON ----------
st.markdown("## 💾 Export Data")
csv = filtered_df.to_csv(index=False)
st.download_button(
    label="📥 Download as CSV",
    data=csv,
    file_name=f"flood_risk_report_{datetime.now().strftime('%Y%m%d')}.csv",
    mime="text/csv",
    help="Download filtered data for offline analysis"
)

# ---------- FOOTER ----------
st.markdown("---")
st.markdown(f"✅ Dashboard last refreshed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
st.caption("Data source: OpenWeatherMap API + FAO historical flood data | Powered by Apache Airflow & MySQL")