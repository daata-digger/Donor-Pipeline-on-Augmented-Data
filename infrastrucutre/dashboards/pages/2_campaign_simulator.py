"""
Campaign Simulator Dashboard
Provides sophisticated campaign simulation and donor targeting capabilities
"""
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import pathlib
import sys

# Add core package to path
root = pathlib.Path(__file__).resolve().parents[2]
sys.path.append(str(root))

from core.campaign_simulator import CampaignSimulator

st.set_page_config(page_title="Campaign Simulator", page_icon="🎯", layout="wide")

# Load and prepare data
@st.cache_data
def load_data():
    root = pathlib.Path(__file__).resolve().parents[2]
    donors = pd.read_csv(root/'data/processed/scored_donors.csv')
    donations = pd.read_csv(root/'data/raw/donations.csv', parse_dates=['donation_date'])
    
    # Calculate donor segments based on giving patterns
    donor_stats = donations.groupby('donor_id').agg({
        'amount': ['sum', 'mean', 'count'],
        'donation_date': 'max'
    }).reset_index()
    
    donor_stats.columns = ['donor_id', 'total_giving', 'avg_gift', 'frequency', 'last_gift']
    donor_stats['recency_days'] = (pd.Timestamp('2025-10-03') - donor_stats['last_gift']).dt.days
    
    # Create RFM segments
    donor_stats['segment'] = pd.qcut(donor_stats['total_giving'], q=5, labels=[
        'Bronze', 'Silver', 'Gold', 'Platinum', 'Diamond'
    ]).fillna('Bronze')
    
    # Merge segments back to donors
    donors = donors.merge(donor_stats[['donor_id', 'segment']], on='donor_id', how='left')
    donors['segment'] = donors['segment'].fillna('New')
    
    return donors, donations

donors, donations = load_data()

# Initialize simulator
simulator = CampaignSimulator(donors, donations)

st.title("Campaign Simulator")
st.write("Plan and optimize fundraising campaigns based on donor segments and historical patterns")

# Campaign configuration
st.subheader("Campaign Configuration")

with st.form('campaign_setup', clear_on_submit=False):
    # Basic Info
    campaign_name = st.text_input(
        'Campaign Name',
        help="Enter a descriptive name for your campaign"
    )
    
    # Campaign Details
    col1, col2 = st.columns(2)
    
    with col1:
        campaign_type = st.selectbox(
            'Campaign Type',
            options=['Annual Fund', 'Capital Campaign', 'Emergency Relief', 'Endowment'],
            help="Select the type of fundraising campaign"
        )
        
        goal_amount = st.number_input(
            'Campaign Goal',
            min_value=10000,
            max_value=10000000,
            value=100000,
            step=10000,
            help="Set your fundraising target amount"
        )
        
        duration_months = st.slider(
            'Campaign Duration (months)',
            min_value=1,
            max_value=24,
            value=3,
            help="Select the planned duration of your campaign"
        )
        
    with col2:
        target_segments = st.multiselect(
            'Target Donor Segments',
            options=sorted(donors['segment'].unique()),
            default=sorted(donors['segment'].unique()),
            help="Choose which donor segments to target"
        )
        
        min_gift_size = st.number_input(
            'Minimum Gift Size',
            min_value=100,
            max_value=10000,
            value=1000,
            step=100,
            help="Set the minimum gift amount to consider"
        )
    
    # Advanced Options (collapsible)
    with st.expander("Advanced Options"):
        col1, col2 = st.columns(2)
        
        with col1:
            contact_strategy = st.selectbox(
                'Contact Strategy',
                options=['Personalized', 'Multi-Channel', 'Digital Only'],
                help="Choose how to engage with donors"
            )
            
        with col2:
            urgency_level = st.select_slider(
                'Urgency Level',
                options=['Low', 'Medium', 'High', 'Critical'],
                value='Medium',
                help="Set the campaign urgency level"
            )
    
    # Submit button with clear styling
    submitted = st.form_submit_button(
        'Run Campaign Simulation',
        help="Click to simulate campaign outcomes"
    )

# Process simulation when form is submitted
if submitted:
    if not campaign_name:
        st.error('Please enter a campaign name')
    elif not target_segments:
        st.error('Please select at least one target segment')
    else:
        with st.spinner('Analyzing campaign potential...'):
            # Run simulation with expanded parameters
            results = simulator.simulate_campaign(
                target_segments=target_segments,
                campaign_type=campaign_type.lower().replace(' ', '_'),
                goal_amount=goal_amount,
                min_gift=min_gift_size,
                duration_months=duration_months,
                contact_strategy=contact_strategy,
                urgency=urgency_level
            )
            
            st.subheader("Campaign Simulation Results")
            
            # Key Metrics
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric(
                    "Projected Revenue",
                    f"${results['total_potential']:,.0f}",
                    f"{(results['total_potential']/goal_amount - 1)*100:+.1f}% vs Goal",
                    help="Total expected donations based on simulation"
                )
            
            with col2:
                st.metric(
                    "Target Donors",
                    f"{results['donors_needed']:,}",
                    f"{len(target_segments)} segments",
                    help="Number of donors needed to reach goal"
                )
            
            with col3:
                st.metric(
                    "Average Gift",
                    f"${results['avg_gift']:,.0f}",
                    help="Expected average donation amount"
                )
            
            with col4:
                st.metric(
                    "Response Rate",
                    f"{results['response_rate']*100:.1f}%",
                    help="Expected donor participation rate"
                )
            
            # Campaign Plan Details
            st.subheader("Campaign Strategy")
            
            tab1, tab2, tab3 = st.tabs([
                "Donor Targeting", 
                "Timeline Analysis", 
                "Resource Planning"
            ])
            
            # Tab 1: Donor Targeting
            with tab1:
                col1, col2 = st.columns([2, 1])
                
                with col1:
                    # Donor Distribution Plot
                    fig_dist = px.scatter(
                        results['target_donors'],
                        x='expected_gift',
                        y='response_prob',
                        size='expected_value',
                        color='tier',
                        custom_data=['first_name', 'last_name', 'strategy'],
                        title='Donor Distribution by Expected Gift and Response Probability',
                        labels={
                            'expected_gift': 'Expected Gift Amount ($)',
                            'response_prob': 'Response Probability',
                            'tier': 'Donor Tier'
                        }
                    )
                    
                    fig_dist.update_traces(
                        hovertemplate="<br>".join([
                            "Donor: %{customdata[0]} %{customdata[1]}",
                            "Expected Gift: $%{x:,.0f}",
                            "Response Probability: %{y:.1%}",
                            "Strategy: %{customdata[2]}"
                        ])
                    )
                    
                    fig_dist.update_layout(
                        plot_bgcolor='white',
                        height=500,
                        showlegend=True,
                        legend_title_text='Donor Tier'
                    )
                    
                    st.plotly_chart(fig_dist, use_container_width=True)
                
                with col2:
                    st.subheader("Segment Breakdown")
                    segment_stats = results['target_donors'].groupby('tier').agg({
                        'expected_value': 'sum',
                        'donor_id': 'count'
                    }).reset_index()
                    
                    fig_segments = go.Figure(data=[
                        go.Pie(
                            labels=segment_stats['tier'],
                            values=segment_stats['expected_value'],
                            hole=0.4,
                            textinfo='label+percent',
                            hovertemplate="<br>".join([
                                "Tier: %{label}",
                                "Expected Value: $%{value:,.0f}",
                                "Percentage: %{percent}"
                            ])
                        )
                    ])
                    
                    fig_segments.update_layout(
                        title="Expected Value by Donor Tier",
                        showlegend=False,
                        height=400
                    )
                    
                    st.plotly_chart(fig_segments, use_container_width=True)
            
            # Tab 2: Timeline Analysis
            with tab2:
                # Generate monthly projections
                months = list(range(1, duration_months + 1))
                cumulative_target = [goal_amount * (1 - np.exp(-i/duration_months)) for i in months]
                projected_revenue = [results['total_potential'] * (1 - np.exp(-i/(duration_months*0.8))) for i in months]
                
                fig_timeline = go.Figure()
                
                # Add target line
                fig_timeline.add_trace(go.Scatter(
                    x=months,
                    y=cumulative_target,
                    name='Target',
                    line=dict(color='gray', dash='dash'),
                    hovertemplate="Month %{x}<br>Target: $%{y:,.0f}"
                ))
                
                # Add projected revenue
                fig_timeline.add_trace(go.Scatter(
                    x=months,
                    y=projected_revenue,
                    name='Projected',
                    line=dict(color='blue'),
                    hovertemplate="Month %{x}<br>Projected: $%{y:,.0f}"
                ))
                
                fig_timeline.update_layout(
                    title="Campaign Timeline Projection",
                    xaxis_title="Month",
                    yaxis_title="Cumulative Revenue ($)",
                    plot_bgcolor='white',
                    height=500,
                    showlegend=True
                )
                
                st.plotly_chart(fig_timeline, use_container_width=True)
            
            # Tab 3: Resource Planning
            with tab3:
                col1, col2 = st.columns([1, 1])
                
                with col1:
                    # Contact Strategy Breakdown
                    strategies = results['target_donors']['strategy'].value_counts()
                    
                    fig_strategies = go.Figure(data=[
                        go.Bar(
                            x=strategies.index,
                            y=strategies.values,
                            text=strategies.values,
                            textposition='auto',
                        )
                    ])
                    
                    fig_strategies.update_layout(
                        title="Contact Strategy Distribution",
                        xaxis_title="Strategy",
                        yaxis_title="Number of Donors",
                        plot_bgcolor='white',
                        height=400,
                        showlegend=False
                    )
                    
                    st.plotly_chart(fig_strategies, use_container_width=True)
                
                with col2:
                    # Resource Requirements
                    st.subheader("Required Resources")
                    
                    resources = pd.DataFrame([
                        {
                            'Resource': 'Personal Visits',
                            'Required': len(results['target_donors'][results['target_donors']['strategy'].str.contains('Personal visit')]),
                            'Time': '2-3 hours each'
                        },
                        {
                            'Resource': 'Phone Calls',
                            'Required': len(results['target_donors'][results['target_donors']['strategy'].str.contains('Phone call')]),
                            'Time': '30 mins each'
                        },
                        {
                            'Resource': 'Custom Proposals',
                            'Required': len(results['target_donors'][results['target_donors']['strategy'].str.contains('proposal')]),
                            'Time': '4-5 hours each'
                        }
                    ])
                    
                    st.dataframe(
                        resources,
                        hide_index=True,
                        column_config={
                            'Resource': 'Activity',
                            'Required': st.column_config.NumberColumn(
                                'Count',
                                help='Number of activities required'
                            ),
                            'Time': 'Estimated Time'
                        }
                    )
            
            # Download Campaign Plan
            st.subheader("Campaign Plan")
            plan = results['target_donors'].copy()
            plan['expected_gift'] = plan['expected_gift'].round(2)
            plan['response_prob'] = plan['response_prob'].round(3)
            plan['expected_value'] = plan['expected_value'].round(2)
            
            st.dataframe(
                plan,
                hide_index=True,
                column_config={
                    'donor_id': 'Donor ID',
                    'first_name': 'First Name',
                    'last_name': 'Last Name',
                    'expected_gift': st.column_config.NumberColumn(
                        'Expected Gift',
                        format="$%.2f"
                    ),
                    'response_prob': st.column_config.NumberColumn(
                        'Response Probability',
                        format="%.1%"
                    ),
                    'expected_value': st.column_config.NumberColumn(
                        'Expected Value',
                        format="$%.2f"
                    ),
                    'tier': 'Donor Tier',
                    'strategy': 'Contact Strategy'
                }
            )
            
            csv = plan.to_csv(index=False)
            st.download_button(
                "Download Campaign Plan",
                csv,
                f"{campaign_name.lower().replace(' ', '_')}_campaign_plan.csv",
                "text/csv",
                key='download-plan'
            )
            
            # Generate campaign plan
            st.subheader('Campaign Plan')
            plan = simulator.create_campaign_plan(results)
            
            # Display donor targeting strategy
            fig = px.scatter(
                plan,
                x='expected_gift',
                y='response_prob',
                size='expected_value',
                color='tier',
                hover_data=['first_name', 'last_name', 'strategy'],
                title='Donor Targeting Strategy'
            )
            st.plotly_chart(fig, use_container_width=True)
            
            # Display campaign plan table
            st.dataframe(
                plan.style.format({
                    'expected_gift': '${:,.0f}',
                    'response_prob': '{:.1%}',
                    'expected_value': '${:,.0f}'
                })
            )
            
            # Download button for campaign plan
            csv = plan.to_csv(index=False)
            st.download_button(
                'Download Campaign Plan',
                csv,
                f'{campaign_name}_plan.csv',
                'text/csv'
            )

# Action Buttons
st.header("📋 Additional Actions")

col1, col2 = st.columns(2)

with col1:
    if st.button("📧 Schedule Email Campaign"):
        st.info("Email campaign scheduler coming soon...")

with col2:
    if st.button("� Generate Campaign Brief"):
        st.info("Campaign brief generator coming soon...")