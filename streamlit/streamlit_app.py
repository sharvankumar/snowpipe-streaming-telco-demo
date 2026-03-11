"""
Telco CDR Analytics Dashboard
Real-time analytics for telecom Call Detail Records (CDR) streaming data
"""

import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session

# Try to import pydeck, fall back to st.map if not available
try:
    import pydeck as pdk
    PYDECK_AVAILABLE = True
except ImportError:
    PYDECK_AVAILABLE = False

# Page configuration (note: page_title and page_icon not supported in SiS)

# Get Snowflake session
session = get_active_session()

# Custom CSS for better styling
st.markdown("""
<style>
    .metric-card {
        background-color: #f0f2f6;
        border-radius: 10px;
        padding: 15px;
        margin: 5px 0;
    }
    .stMetric {
        background-color: #ffffff;
        padding: 15px;
        border-radius: 10px;
        border: 1px solid #e0e0e0;
    }
</style>
""", unsafe_allow_html=True)

# Title
st.title("📡 Telco CDR Analytics Dashboard")
st.markdown("Real-time analytics for Call Detail Records streaming pipeline")

# Disclaimer
st.info(
    "**Note:** This dashboard shows data from the last 30 days. "
    "If you're not seeing data, run the streaming producer script to populate records. "
    "Setup: [snowpipe-streaming-telco-demo](https://github.com/sharvankumar/snowpipe-streaming-telco-demo)"
)

# Sidebar for navigation
st.sidebar.title("Navigation")
page = st.sidebar.radio(
    "Select View",
    ["Overview", "Streaming Health", "Error Analysis", "Tower Analytics", "Device Analytics"]
)

# Helper function to run queries
@st.cache_data(ttl=60)
def run_query(query):
    return session.sql(query).to_pandas()


# ============== OVERVIEW PAGE ==============
if page == "Overview":
    st.header("📊 Overview")
    
    # Key metrics
    col1, col2, col3, col4 = st.columns(4)
    
    # Total records
    total_records = run_query("SELECT COUNT(*) as cnt FROM TELCO_ANALYTICS.CDR_STREAMING_300.CDR_RECORDS_300")
    col1.metric("Total CDR Records", f"{total_records['CNT'].iloc[0]:,}")
    
    # Records in last hour
    recent_records = run_query("""
        SELECT COUNT(*) as cnt FROM TELCO_ANALYTICS.CDR_STREAMING_300.CDR_RECORDS_300 
        WHERE ingestion_time >= DATEADD('day', -30, CURRENT_TIMESTAMP())
    """)
    col2.metric("Records (Last 30 Days)", f"{recent_records['CNT'].iloc[0]:,}")
    
    # Error count
    error_count = run_query("SELECT COUNT(*) as cnt FROM TELCO_ANALYTICS.CDR_STREAMING_300.CDR_DEAD_LETTER_300")
    col3.metric("Dead Letter Queue", f"{error_count['CNT'].iloc[0]:,}")
    
    # Unique towers
    tower_count = run_query("""
        SELECT COUNT(DISTINCT cell_tower_id) as cnt FROM TELCO_ANALYTICS.CDR_STREAMING_300.CDR_RECORDS_300
        WHERE ingestion_time >= DATEADD('day', -30, CURRENT_TIMESTAMP())
    """)
    col4.metric("Active Towers", f"{tower_count['CNT'].iloc[0]:,}")
    
    st.divider()
    
    # Call type distribution
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Call Type Distribution")
        call_types = run_query("""
            SELECT call_type, COUNT(*) as count
            FROM TELCO_ANALYTICS.CDR_STREAMING_300.CDR_RECORDS_300
            WHERE ingestion_time >= DATEADD('day', -30, CURRENT_TIMESTAMP())
            GROUP BY call_type
            ORDER BY count DESC
        """)
        if not call_types.empty:
            st.bar_chart(call_types.set_index('CALL_TYPE')['COUNT'])
    
    with col2:
        st.subheader("Call Status Distribution")
        call_status = run_query("""
            SELECT call_status, COUNT(*) as count
            FROM TELCO_ANALYTICS.CDR_STREAMING_300.CDR_RECORDS_300
            WHERE ingestion_time >= DATEADD('day', -30, CURRENT_TIMESTAMP())
            GROUP BY call_status
            ORDER BY count DESC
        """)
        if not call_status.empty:
            st.bar_chart(call_status.set_index('CALL_STATUS')['COUNT'])
    
    # Network type distribution
    st.subheader("Network Type Distribution")
    network_types = run_query("""
        SELECT network_type, COUNT(*) as count
        FROM TELCO_ANALYTICS.CDR_STREAMING_300.CDR_RECORDS_300
        WHERE ingestion_time >= DATEADD('day', -30, CURRENT_TIMESTAMP())
        GROUP BY network_type
        ORDER BY count DESC
    """)
    if not network_types.empty:
        st.bar_chart(network_types.set_index('NETWORK_TYPE')['COUNT'])


# ============== STREAMING HEALTH PAGE ==============
elif page == "Streaming Health":
    st.header("🔄 Streaming Health")
    
    # Streaming latency
    st.subheader("Streaming Latency (Last 30 Days)")
    with st.expander("ℹ️ What is Streaming Latency?"):
        st.markdown("""
    **Streaming Latency** measures the delay between:
    - **Event Time**: When the CDR record was generated (`call_start_time`)
    - **Ingestion Time**: When it arrived in Snowflake (`ingestion_time`)
    
    | Latency | Status |
    |---------|--------|
    | < 5s | Excellent |
    | 5-30s | Good |
    | > 30s | Review pipeline |
    """)
    latency_df = run_query("SELECT * FROM TELCO_ANALYTICS.CDR_STREAMING_300.V_STREAMING_LATENCY_300 LIMIT 60")
    if not latency_df.empty:
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Avg Latency (seconds)", f"{latency_df['APPROX_LATENCY_S'].mean():.1f}")
        with col2:
            st.metric("Total Rows Ingested", f"{latency_df['ROWS_INGESTED'].sum():,}")
        
        st.line_chart(latency_df.set_index('MINUTE')[['ROWS_INGESTED', 'APPROX_LATENCY_S']])
    else:
        st.info("No latency data available for the last 30 days")
    
    st.divider()
    
    # Error rate
    st.subheader("Error Rate Over Time")
    error_rate_df = run_query("SELECT * FROM TELCO_ANALYTICS.CDR_STREAMING_300.V_ERROR_RATE_300")
    if not error_rate_df.empty:
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Good Rows", f"{error_rate_df['GOOD_ROWS'].sum():,}")
        with col2:
            st.metric("Bad Rows", f"{error_rate_df['BAD_ROWS'].sum():,}")
        with col3:
            avg_error_rate = error_rate_df['ERROR_RATE_PCT'].mean()
            st.metric("Avg Error Rate", f"{avg_error_rate:.2f}%")
        
        st.line_chart(error_rate_df.set_index('MINUTE')['ERROR_RATE_PCT'])
    else:
        st.info("No error rate data available")
    
    st.divider()
    
    # Offset gaps detection
    st.subheader("Offset Gaps (Data Integrity)")
    offset_gaps_df = run_query("SELECT * FROM TELCO_ANALYTICS.CDR_STREAMING_300.V_OFFSET_GAPS_300 LIMIT 100")
    if not offset_gaps_df.empty:
        st.warning(f"Found {len(offset_gaps_df)} offset gaps - potential data loss detected")
        st.dataframe(offset_gaps_df, use_container_width=True)
    else:
        st.success("No offset gaps detected - data integrity OK")


# ============== ERROR ANALYSIS PAGE ==============
elif page == "Error Analysis":
    st.header("⚠️ Error Analysis")
    
    # DLQ Summary
    st.subheader("Dead Letter Queue Summary")
    dlq_summary = run_query("SELECT * FROM TELCO_ANALYTICS.CDR_STREAMING_300.V_DLQ_SUMMARY_300")
    if not dlq_summary.empty:
        st.dataframe(dlq_summary, use_container_width=True)
        
        # Error category chart
        st.bar_chart(dlq_summary.set_index('ERROR_CATEGORY')['ERROR_COUNT'])
    else:
        st.success("No errors in dead letter queue")
    
    st.divider()
    
    # Error by channel
    st.subheader("Errors by Channel")
    error_by_channel = run_query("SELECT * FROM TELCO_ANALYTICS.CDR_STREAMING_300.V_ERROR_BY_CHANNEL_300")
    if not error_by_channel.empty:
        st.dataframe(error_by_channel, use_container_width=True)
    else:
        st.info("No channel-specific errors found")
    
    st.divider()
    
    # Schema evolution tracking
    st.subheader("Schema Evolution Tracking")
    schema_evolution = run_query("SELECT * FROM TELCO_ANALYTICS.CDR_STREAMING_300.V_SCHEMA_EVOLUTION_300")
    if not schema_evolution.empty:
        st.dataframe(schema_evolution, use_container_width=True)
    else:
        st.info("No schema evolution data available")


# ============== TOWER ANALYTICS PAGE ==============
elif page == "Tower Analytics":
    st.header("🗼 Tower Analytics")
    
    # Tower heatmap data
    tower_data = run_query("""
        SELECT 
            cell_tower_id,
            ST_X(tower_location)::FLOAT as longitude,
            ST_Y(tower_location)::FLOAT as latitude,
            call_count,
            avg_duration,
            total_data_mb,
            dropped_calls
        FROM TELCO_ANALYTICS.CDR_STREAMING_300.V_TOWER_HEATMAP_300
        WHERE tower_location IS NOT NULL
    """)
    
    if not tower_data.empty:
        # Summary metrics
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total Towers", len(tower_data))
        col2.metric("Total Calls", f"{tower_data['CALL_COUNT'].sum():,}")
        col3.metric("Dropped Calls", f"{tower_data['DROPPED_CALLS'].sum():,}")
        col4.metric("Total Data (MB)", f"{tower_data['TOTAL_DATA_MB'].sum():,.1f}")
        
        st.divider()
        
        # Interactive map
        st.subheader("Tower Heatmap")
        
        # Prepare data for map
        map_data = tower_data.rename(columns={
            'LATITUDE': 'lat',
            'LONGITUDE': 'lon',
            'CALL_COUNT': 'call_count',
            'DROPPED_CALLS': 'dropped_calls'
        })
        
        if PYDECK_AVAILABLE:
            # Calculate center
            center_lat = map_data['lat'].mean()
            center_lon = map_data['lon'].mean()
            
            # Create layers
            scatter_layer = pdk.Layer(
                "ScatterplotLayer",
                data=map_data,
                get_position=["lon", "lat"],
                get_radius="call_count * 10",
                get_fill_color=[255, 140, 0, 180],
                pickable=True,
                auto_highlight=True,
            )
            
            heatmap_layer = pdk.Layer(
                "HeatmapLayer",
                data=map_data,
                get_position=["lon", "lat"],
                get_weight="call_count",
                aggregation="SUM",
                threshold=0.1,
            )
            
            # View state
            view_state = pdk.ViewState(
                latitude=center_lat,
                longitude=center_lon,
                zoom=4,
                pitch=45,
            )
            
            # Create deck
            deck = pdk.Deck(
                layers=[heatmap_layer, scatter_layer],
                initial_view_state=view_state,
                tooltip={"text": "{CELL_TOWER_ID}\nCalls: {call_count}\nDropped: {dropped_calls}"}
            )
            
            st.pydeck_chart(deck)
        else:
            # Fallback to st.map
            st.map(map_data, latitude='lat', longitude='lon', size='call_count')
        
        st.divider()
        
        # Tower details table
        st.subheader("Tower Details")
        st.dataframe(
            tower_data.sort_values('CALL_COUNT', ascending=False),
            use_container_width=True
        )
    else:
        st.info("No tower data available for the last 30 days")


# ============== DEVICE ANALYTICS PAGE ==============
elif page == "Device Analytics":
    st.header("📱 Device Analytics")
    
    # Device analytics
    device_df = run_query("SELECT * FROM TELCO_ANALYTICS.CDR_STREAMING_300.V_DEVICE_ANALYTICS_300")
    
    if not device_df.empty:
        # Summary metrics
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Unique Device Makes", device_df['DEVICE_MAKE'].nunique())
        with col2:
            st.metric("Unique Device Models", device_df['DEVICE_MODEL'].nunique())
        
        st.divider()
        
        # Device make distribution
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Calls by Device Make")
            make_summary = device_df.groupby('DEVICE_MAKE')['CALL_COUNT'].sum().sort_values(ascending=False)
            st.bar_chart(make_summary)
        
        with col2:
            st.subheader("Calls by OS")
            os_summary = device_df.groupby('DEVICE_OS')['CALL_COUNT'].sum().sort_values(ascending=False)
            st.bar_chart(os_summary)
        
        st.divider()
        
        # Signal strength by device
        st.subheader("Average Signal Strength by Device")
        signal_df = device_df[['DEVICE_MAKE', 'DEVICE_MODEL', 'AVG_SIGNAL_STRENGTH', 'CALL_COUNT']].sort_values('CALL_COUNT', ascending=False)
        st.dataframe(signal_df, use_container_width=True)
        
        st.divider()
        
        # Full device details
        st.subheader("Device Details")
        st.dataframe(device_df, use_container_width=True)
    else:
        st.info("No device data available for the last 30 days")
    
    st.divider()
    
    # Service tag distribution
    st.subheader("Service Tag Distribution")
    service_tags = run_query("SELECT * FROM TELCO_ANALYTICS.CDR_STREAMING_300.V_SERVICE_TAG_DIST_300")
    if not service_tags.empty:
        st.bar_chart(service_tags.set_index('SERVICE_TAG')['USAGE_COUNT'])
        st.dataframe(service_tags, use_container_width=True)
    else:
        st.info("No service tag data available")


# Footer
st.divider()
st.caption("Data refreshes every 60 seconds | Built with Streamlit in Snowflake | [Setup Guide](https://github.com/sharvankumar/snowpipe-streaming-telco-demo)")
