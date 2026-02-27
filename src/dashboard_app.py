import streamlit as st
import pandas as pd
import plotly.express as px
import os
import glob

# Configure layout
st.set_page_config(page_title="Disney AdOps Command Center", layout="wide", page_icon="🐭")

# Custom CSS for dark mode and Disney-like styling
st.markdown("""
<style>
    .reportview-container {
        background-color: #040714;
        color: white;
    }
    .sidebar .sidebar-content {
        background-color: #1a1d29;
    }
    h1, h2, h3 {
        color: #f9f9f9;
        font-family: 'Avenir', sans-serif;
    }
    .metric-card {
        background-color: #1e2133;
        border-radius: 8px;
        padding: 20px;
        box-shadow: 0 4px 6px rgba(0,0,0,0.3);
        text-align: center;
        border-bottom: 3px solid #0063e5;
    }
    .stDataFrame {
        border-radius: 8px;
    }
    hr {
        border-color: #333;
    }
</style>
""", unsafe_allow_html=True)

st.title("Disney AdOps Command Center 📊")
st.markdown("Automated Media Delivery & Campaign Health Monitoring (Powered by EVE)")
st.markdown("---")

# Data Loader
@st.cache_data
def load_data():
    data_dir = os.path.join(os.path.dirname(__file__), "..", "data")
    
    # Check if data exists
    if not os.path.exists(data_dir):
        return None, None
        
    perf_files = glob.glob(os.path.join(data_dir, "campaign_performance_*.csv"))
    ticket_files = glob.glob(os.path.join(data_dir, "tickets_*.csv"))
    
    df_perf = None
    df_tickets = None
    
    # Load most recent performance data
    if perf_files:
        latest_perf = max(perf_files, key=os.path.getctime)
        df_perf = pd.read_csv(latest_perf)
        df_perf['date'] = pd.to_datetime(df_perf['date'])
        
    # Load most recent ticket data
    if ticket_files:
        latest_ticket = max(ticket_files, key=os.path.getctime)
        df_tickets = pd.read_csv(latest_ticket)
        df_tickets['created_at'] = pd.to_datetime(df_tickets['created_at'])
        
    return df_perf, df_tickets

df_perf, df_tickets = load_data()

if df_perf is None or df_tickets is None:
    st.error("No data found! Please run the data generator first.")
    st.stop()

# ----- SIDEBAR FILTERS -----
st.sidebar.header("Command Filters")
st.sidebar.markdown("Filter live telemetry:")

# Get unique brands
brands = ["All"] + list(df_perf["brand_code"].unique())
selected_brand = st.sidebar.selectbox("Brand", brands)

# Get unique channels
channels = ["All"] + list(df_perf["channel_mapped"].unique())
selected_channel = st.sidebar.selectbox("Channel", channels)

# Filter Data
filtered_perf = df_perf.copy()
filtered_tickets = df_tickets.copy()

if selected_brand != "All":
    filtered_perf = filtered_perf[filtered_perf["brand_code"] == selected_brand]
    filtered_tickets = filtered_tickets[filtered_tickets["brand"] == selected_brand]

if selected_channel != "All":
    filtered_perf = filtered_perf[filtered_perf["channel_mapped"] == selected_channel]
    
# Calculate metrics safely
def safe_sum(series):
    return float(series.sum()) if not series.empty else 0.0

total_spend = safe_sum(filtered_perf['spend_usd'])
total_impr = safe_sum(filtered_perf['impressions'])
total_clicks = safe_sum(filtered_perf['clicks'])
ctr = (total_clicks / total_impr * 100) if total_impr > 0 else 0
cpc = (total_spend / total_clicks) if total_clicks > 0 else 0

# Convert clicks to an estimated number of CAPI conversions for demonstration
# Our mock proxy intercepts clicks and fires them to CAPI
total_capi = int(total_clicks * 0.15) 

# ----- TOP METRICS ROW -----
col1, col2, col3, col4, col5 = st.columns(5)
with col1:
    st.markdown(f'<div class="metric-card"><h4>Spend</h4><h2>${total_spend:,.0f}</h2></div>', unsafe_allow_html=True)
with col2:
    st.markdown(f'<div class="metric-card"><h4>Impressions</h4><h2>{total_impr:,.0f}</h2></div>', unsafe_allow_html=True)
with col3:
    st.markdown(f'<div class="metric-card"><h4>CTR</h4><h2>{ctr:.2f}%</h2></div>', unsafe_allow_html=True)
with col4:
    st.markdown(f'<div class="metric-card"><h4>CPC</h4><h2>${cpc:.2f}</h2></div>', unsafe_allow_html=True)
with col5:
    # Adding a special color for CAPI conversions to highlight the new feature
    st.markdown(f'<div class="metric-card" style="border-bottom: 3px solid #4CAF50;"><h4>CAPI Events</h4><h2 style="color: #4CAF50;">{total_capi:,.0f}</h2></div>', unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# ----- CHARTS -----
col_left, col_right = st.columns(2)

with col_left:
    st.subheader("Spend by Channel")
    if not filtered_perf.empty:
        spend_by_channel = filtered_perf.groupby("channel_mapped")["spend_usd"].sum().reset_index()
        fig_spend = px.pie(spend_by_channel, values="spend_usd", names="channel_mapped", 
                          color_discrete_sequence=px.colors.sequential.Teal)
        fig_spend.update_layout(plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)", font_color="white")
        st.plotly_chart(fig_spend, use_container_width=True)
    else:
        st.info("No data available for this filter.")

with col_right:
    st.subheader("EVE API Operations (Tickets)")
    if not filtered_tickets.empty:
        ticket_status = filtered_tickets.groupby("status")["id"].count().reset_index()
        fig_tickets = px.bar(ticket_status, x="status", y="id", text="id",
                            labels={"id": "Count", "status": "Task State"},
                            color="status",
                            color_discrete_map={
                                "Live": "#00C853", 
                                "Ready to Launch": "#00B0FF",
                                "QA": "#FF5252",
                                "Trafficking": "#FFD600",
                                "Pending Asset": "#9E9E9E"
                            })
        fig_tickets.update_traces(textposition='outside')
        fig_tickets.update_layout(plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)", font_color="white", showlegend=False)
        st.plotly_chart(fig_tickets, use_container_width=True)
    else:
        st.info("No data available for this filter.")

st.markdown("---")
st.subheader("Recent API Trafficking Jobs")

if not filtered_tickets.empty:
    display_cols = ["id", "campaign_id", "request_type", "platform", "status"]
    available_cols = [c for c in display_cols if c in filtered_tickets.columns]
    recent_jobs = filtered_tickets[available_cols].head(10)
    
    st.dataframe(
        recent_jobs,
        use_container_width=True,
        hide_index=True
    )

st.caption("Disney AdOps EVE Analytics | Authorized Personnel Only")
