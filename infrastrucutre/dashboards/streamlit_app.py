import pandas as pd
import numpy as np
import pathlib
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import sys
from datetime import datetime, timedelta
from streamlit_folium import st_folium
import folium
from folium.plugins import HeatMap, MarkerCluster

# Add core package to path
root = pathlib.Path(__file__).resolve().parents[1]
sys.path.append(str(root))

# Initialize paths
data_dir = root / 'data'
processed_dir = data_dir / 'processed'
raw_dir = data_dir / 'raw'

# Page config
st.set_page_config(
    page_title='Donor Intelligence Platform',
    layout='wide',
    initial_sidebar_state='expanded'
)

# Check data directories
if not raw_dir.exists():
    st.error("Raw data directory not found. Please run scripts/copy_data.py first.")
    st.stop()

if not processed_dir.exists():
    st.error("Processed data directory not found. Please run the data pipeline first.")
    st.stop()

try:
    # Import visualization after directory checks
    from core.advanced_visualization import DonorVisualization
except ImportError as e:
    st.error(f"Could not import visualization module: {str(e)}")
    st.stop()

try:
    # Load data with more specific error messages
    try:
        donors = pd.read_csv(processed_dir/'scored_donors.csv')
    except FileNotFoundError:
        st.error("Scored donors file not found. Please run the ML pipeline first.")
        st.stop()
        
    try:
        campaigns = pd.read_csv(raw_dir/'campaigns.csv')
    except FileNotFoundError:
        st.error("Campaigns data not found. Please run scripts/copy_data.py first.")
        st.stop()
        
    try:
        donations = pd.read_csv(raw_dir/'donations.csv', parse_dates=['donation_date'])
    except FileNotFoundError:
        st.error("Donations data not found. Please run scripts/copy_data.py first.")
        st.stop()
        
    try:
        events = pd.read_csv(raw_dir/'engagement_events.csv', parse_dates=['event_date'])
    except FileNotFoundError:
        st.error("Events data not found. Please run scripts/copy_data.py first.")
        st.stop()
except Exception as e:
    st.error(f"Error loading data: {str(e)}")
    st.stop()

# Initialize visualizer
viz = DonorVisualization()

# Sidebar filters
st.sidebar.title('Filters')

# Date range filter
date_range = st.sidebar.date_input(
    'Date Range',
    value=(donations['donation_date'].min(), donations['donation_date'].max())
)

# Campaign filter
selected_campaigns = st.sidebar.multiselect(
    'Campaigns',
    campaigns['name'].unique(),
    default=[]
)

# State filter
selected_states = st.sidebar.multiselect(
    'States',
    donors['state'].unique(),
    default=[]
)

# Apply filters
filtered_donors = donors.copy()
if selected_states:
    filtered_donors = filtered_donors[filtered_donors['state'].isin(selected_states)]

filtered_donations = donations[
    (donations['donation_date'] >= pd.Timestamp(date_range[0])) &
    (donations['donation_date'] <= pd.Timestamp(date_range[1]))
]

if selected_campaigns:
    filtered_donations = filtered_donations[
        filtered_donations['campaign_id'].isin(
            campaigns[campaigns['name'].isin(selected_campaigns)]['campaign_id']
        )
    ]

# Calculate KPIs
total_donations = filtered_donations['amount'].sum()
top_decile_donors = filtered_donors[filtered_donors['decile'] == 10]

# Display KPIs
st.header('Key Performance Indicators')

kpi1, kpi2, kpi3, kpi4 = st.columns(4)

kpi1.metric(
    'Total Donations',
    f'${total_donations:,.0f}',
    f'Number of Gifts: {len(filtered_donations):,}'
)

kpi2.metric(
    'High Propensity Donors',
    f'{len(top_decile_donors):,}',
    f'Top Decile: {(len(top_decile_donors)/len(filtered_donors)*100):.1f}%'
)

kpi3.metric(
    'Average Gift Size',
    f'${filtered_donations["amount"].mean():,.0f}',
    f'Median: ${filtered_donations["amount"].median():,.0f}'
)

kpi4.metric(
    'Total Donors',
    f'{len(filtered_donors):,}',
    f'Avg Score: {(filtered_donors["propensity"].mean()*100):.1f}%'
)

# Main title without emoji
st.title('Donor Intelligence Platform')

# Tabs for different visualizations
tab1, tab2, tab3, tab4 = st.tabs([
    "Giving Patterns",
    "Donor Journey",
    "Geographic Analysis",
    "Segment Analysis"
])

with tab1:
    st.header('Giving Patterns Analysis')
    
    # Create subplots for the giving patterns
    fig = go.Figure()
    
    try:
        # Prepare time-based data
        filtered_donations['date'] = pd.to_datetime(filtered_donations['donation_date'])
        filtered_donations['month'] = filtered_donations['date'].dt.month
        filtered_donations['month_name'] = filtered_donations['date'].dt.strftime('%B')
        filtered_donations['day_of_week'] = filtered_donations['date'].dt.dayofweek
        filtered_donations['day_name'] = filtered_donations['date'].dt.strftime('%A')
        
        # Create 2x2 subplots
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=('Monthly Giving Patterns', 'Daily Giving Patterns',
                          'Gift Size Distribution', 'Cumulative Giving'),
            vertical_spacing=0.15,
            horizontal_spacing=0.1
        )
        
        # Monthly patterns
        monthly_stats = filtered_donations.groupby('month_name').agg({
            'amount': ['count', 'mean', 'sum']
        }).reset_index()
        monthly_stats.columns = ['month', 'count', 'average', 'total']
        
        # Sort by month order
        month_order = ['January', 'February', 'March', 'April', 'May', 'June',
                      'July', 'August', 'September', 'October', 'November', 'December']
        monthly_stats['month'] = pd.Categorical(monthly_stats['month'], categories=month_order)
        monthly_stats = monthly_stats.sort_values('month')
        
        fig.add_trace(
            go.Bar(x=monthly_stats['month'], y=monthly_stats['total'],
                  name='Monthly Total'),
            row=1, col=1
        )
        
        # Daily patterns
        day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        daily_stats = filtered_donations.groupby('day_name').agg({
            'amount': ['count', 'mean', 'sum']
        }).reset_index()
        daily_stats.columns = ['day', 'count', 'average', 'total']
        daily_stats['day'] = pd.Categorical(daily_stats['day'], categories=day_order)
        daily_stats = daily_stats.sort_values('day')
        
        fig.add_trace(
            go.Bar(x=daily_stats['day'], y=daily_stats['count'],
                  name='Daily Count'),
            row=1, col=2
        )
        
        # Distribution
        fig.add_trace(
            go.Histogram(x=filtered_donations['amount'], nbinsx=50,
                        name='Gift Distribution'),
            row=2, col=1
        )
        
        # Cumulative
        sorted_donations = filtered_donations.sort_values('date')
        sorted_donations['cumulative'] = sorted_donations['amount'].cumsum()
        
        fig.add_trace(
            go.Scatter(x=sorted_donations['date'], y=sorted_donations['cumulative'],
                      name='Cumulative Giving'),
            row=2, col=2
        )
        
        # Update layout
        fig.update_layout(
            height=800,
            showlegend=True,
            title_text='Giving Patterns Analysis'
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
    except Exception as e:
        st.error(f"Error generating giving patterns: {str(e)}")
        st.write("Debug info:", str(e))
    
    # Add summary metrics
    st.subheader('Key Giving Metrics')
    m1, m2, m3, m4 = st.columns(4)
    
    with m1:
        st.metric('Peak Giving Month', 
                  filtered_donations.groupby(pd.to_datetime(filtered_donations['donation_date']).dt.strftime('%B'))['amount'].sum().idxmax())
    
    with m2:
        st.metric('Most Active Day', 
                  filtered_donations.groupby(pd.to_datetime(filtered_donations['donation_date']).dt.strftime('%A'))['amount'].count().idxmax())
    
    with m3:
        st.metric('Average Gift Size', 
                  f'${filtered_donations["amount"].mean():,.2f}')
    
    with m4:
        st.metric('Giving Consistency', 
                  f'{len(filtered_donations["donation_date"].dt.strftime("%Y-%m").unique())} months')

with tab2:
    # Donor selector
    selected_donor = st.selectbox(
        "Select Donor to View Journey",
        options=filtered_donors['donor_id'].unique(),
        format_func=lambda x: f"Donor {x}"
    )
    
    if selected_donor:
        st.plotly_chart(
            viz.plot_donor_journey(
                selected_donor,
                filtered_donations,
                events[events['donor_id'] == selected_donor]
            ),
            use_container_width=True
        )

with tab3:
    st.header("Geographic Distribution of Donors")
    
    # State-level analysis
    state_summary = filtered_donors.groupby('state').agg({
        'donor_id': 'count',
        'total_amount': 'sum'
    }).reset_index()
    
    # Create choropleth map
    fig = px.choropleth(
        state_summary,
        locations='state',
        locationmode='USA-states',
        color='total_amount',
        scope='usa',
        title='Total Donations by State',
        color_continuous_scale='Viridis'
    )
    st.plotly_chart(fig, use_container_width=True)
    
    # Top states analysis
    col1, col2 = st.columns(2)
    
    with col1:
        top_states = state_summary.nlargest(10, 'total_amount')
        st.plotly_chart(
            px.bar(top_states, x='state', y='total_amount',
                   title='Top 10 States by Donation Volume'),
            use_container_width=True
        )
    
    with col2:
        state_summary['donor_pct'] = state_summary['donor_id'] / state_summary['donor_id'].sum() * 100
        st.plotly_chart(
            px.pie(state_summary.nlargest(5, 'donor_id'),
                   values='donor_pct', names='state',
                   title='Top 5 States by Donor Count'),
            use_container_width=True
        )

with tab4:
    st.header("Donor Segment Analysis")
    
    # Create RFM segments
    filtered_donors['rfm_score'] = (
        filtered_donors['recency_days'].rank(ascending=False) +
        filtered_donors['frequency'].rank(ascending=True) +
        filtered_donors['total_amount'].rank(ascending=True)
    ) / 3
    
    filtered_donors['segment'] = pd.qcut(
        filtered_donors['rfm_score'],
        q=5,
        labels=['Bronze', 'Silver', 'Gold', 'Platinum', 'Diamond']
    )
    
    # Segment Overview
    segment_summary = filtered_donors.groupby('segment').agg({
        'donor_id': 'count',
        'total_amount': ['sum', 'mean'],
        'frequency': 'mean',
        'recency_days': 'mean'
    }).round(2)
    
    segment_summary.columns = ['Count', 'Total Giving', 'Avg Giving', 'Avg Frequency', 'Avg Recency']
    st.dataframe(segment_summary, use_container_width=True)
    
    # Segment Visualizations
    col1, col2 = st.columns(2)
    
    with col1:
        # Segment size
        fig_size = px.pie(
            segment_summary.reset_index(),
            values='Count',
            names='segment',
            title='Donor Distribution by Segment'
        )
        st.plotly_chart(fig_size, use_container_width=True)
    
    with col2:
        # Segment value
        fig_value = px.bar(
            segment_summary.reset_index(),
            x='segment',
            y='Total Giving',
            title='Total Giving by Segment'
        )
        st.plotly_chart(fig_value, use_container_width=True)
    
    # Segment Characteristics
    st.subheader('Segment Characteristics')
    
    fig_chars = px.scatter(
        filtered_donors,
        x='frequency',
        y='total_amount',
        color='segment',
        size='recency_days',
        title='Segment Distribution: Frequency vs Total Giving',
        labels={
            'frequency': 'Number of Donations',
            'total_amount': 'Total Giving ($)',
            'recency_days': 'Days Since Last Gift'
        }
    )
    st.plotly_chart(fig_chars, use_container_width=True)

# Temporarily removed retention analysis while we fix the cohort calculation

# Footer
st.markdown("---")
st.markdown(
    "💡 *This dashboard provides comprehensive donor analytics for strategic "
    "decision making in fundraising and donor engagement.*")
