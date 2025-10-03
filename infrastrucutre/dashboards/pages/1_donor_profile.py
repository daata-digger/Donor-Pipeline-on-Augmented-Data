"""
Donor 360° Profile View
Shows comprehensive donor insights including:
- Lifetime giving history
- Engagement patterns
- Wealth indicators
- Propensity predictions with SHAP explanations
"""
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import pathlib

st.set_page_config(page_title="Donor Profile", layout="wide")

# Load data
@st.cache_data
def load_data():
    root = pathlib.Path(__file__).resolve().parents[2]
    donors = pd.read_csv(root/'data/processed/scored_donors.csv')
    donations = pd.read_csv(root/'data/raw/donations.csv', parse_dates=['donation_date'])
    events = pd.read_csv(root/'data/raw/engagement_events.csv', parse_dates=['event_date'])
    wealth = pd.read_csv(root/'data/raw/wealth_external.csv')
    return donors, donations, events, wealth

donors, donations, events, wealth = load_data()

# Sidebar - Donor Selection
st.sidebar.title("Donor Lookup")
search_term = st.sidebar.text_input("Search by Name or ID")

if search_term:
    filtered_donors = donors[
        (donors['first_name'].str.contains(search_term, case=False, na=False)) |
        (donors['last_name'].str.contains(search_term, case=False, na=False)) |
        (donors['donor_id'].astype(str).str.contains(search_term))
    ]
    
    if len(filtered_donors) > 0:
        selected_donor_id = st.sidebar.selectbox(
            "Select Donor",
            filtered_donors['donor_id'].tolist(),
            format_func=lambda x: f"{filtered_donors[filtered_donors['donor_id']==x].iloc[0]['first_name']} {filtered_donors[filtered_donors['donor_id']==x].iloc[0]['last_name']} (ID: {x})"
        )
        
        # Get donor details
        donor = donors[donors['donor_id'] == selected_donor_id].iloc[0]
        donor_donations = donations[donations['donor_id'] == selected_donor_id]
        donor_events = events[events['donor_id'] == selected_donor_id]
        donor_wealth = wealth[wealth['donor_id'] == selected_donor_id].iloc[0]
        
        # Header with key metrics
        st.title(f"Donor Profile: {donor['first_name']} {donor['last_name']}")
        
        # Key Metrics
        col1, col2, col3, col4 = st.columns(4)
        
        col1.metric(
            "Lifetime Giving",
            f"${donor['total_amount']:,.2f}"
        )
        col2.metric(
            "Propensity Score",
            f"{donor['propensity']:.1%}"
        )
        col3.metric(
            "Events Attended",
            str(donor['events_attended'])
        )
        col4.metric(
            "Wealth Score",
            f"{donor_wealth['wealth_score_ext']:.1%}"
        )
        
        # Giving History
        st.header("Giving History")
        # Summary statistics
        stats_col1, stats_col2, stats_col3 = st.columns(3)
        with stats_col1:
            st.metric(
                "Average Gift",
                f"${donor_donations['amount'].mean():,.2f}",
                f"Total: {len(donor_donations)} gifts"
            )
        with stats_col2:
            if len(donor_donations) > 0:
                st.metric(
                    "Largest Gift",
                    f"${donor_donations['amount'].max():,.2f}",
                    f"Date: {donor_donations.loc[donor_donations['amount'].idxmax(), 'donation_date'].strftime('%Y-%m-%d')}"
                )
            else:
                st.metric("Largest Gift", "$0", "No donations yet")
        with stats_col3:
            if len(donor_donations) > 0:
                last_gift = donor_donations.nlargest(1, 'donation_date').iloc[0]
                st.metric(
                    "Most Recent Gift",
                    f"${last_gift['amount']:,.2f}",
                    f"Date: {last_gift['donation_date'].strftime('%Y-%m-%d')}"
                )
            else:
                st.metric("Most Recent Gift", "$0", "No donations yet")
        # Enhanced donation timeline
        fig_timeline = px.scatter(
            donor_donations,
            x='donation_date',
            y='amount',
            size='amount',
            title='Donation Timeline',
            labels={'amount': 'Gift Amount ($)', 'donation_date': 'Date'},
            hover_data=['amount', 'donation_date'],
            color='amount',
            color_continuous_scale=px.colors.sequential.Blues
        )
        fig_timeline.update_traces(marker=dict(line=dict(width=1, color='DarkSlateGrey')))
        fig_timeline.update_layout(
            height=400,
            xaxis=dict(showgrid=True, zeroline=False),
            yaxis=dict(showgrid=True, zeroline=False),
            hoverlabel=dict(bgcolor="white"),
            hovermode='closest',
            plot_bgcolor='#111217',
            paper_bgcolor='#111217',
        )
        st.plotly_chart(fig_timeline, use_container_width=True)
        
        # Split detailed analysis into columns
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("External Wealth Score")
            wealth_data = pd.DataFrame({
                'Indicator': [
                    'Wealth Score',
                    'Relative Ranking'
                ],
                'Value': [
                    f"{donor_wealth['wealth_score_ext']:.1%}",
                    f"Top {((1 - donor_wealth['wealth_score_ext']) * 100):.0f}%"
                ]
            })
            st.dataframe(wealth_data, hide_index=True)
        
        with col2:
            st.subheader("Engagement History")
            if len(donor_events) > 0:
                fig_events = px.timeline(
                    donor_events,
                    x_start='event_date',
                    x_end='event_date',
                    y='event_type',
                    title='Event Timeline',
                    labels={'event_type': 'Type', 'event_date': 'Date'}
                )
                st.plotly_chart(fig_events, use_container_width=True)
            else:
                st.write("No event history recorded")
        
        # Predictions and Recommendations
        st.header("Predictions & Recommendations")
        
        rec_col1, rec_col2 = st.columns(2)
        
        with rec_col1:
            st.subheader("Next Best Action")
            if donor['propensity'] > 0.7:
                st.markdown("<div style='background-color:#e6f7ff;padding:8px;border-radius:6px;color:#005073;'>High-priority prospect for major gift cultivation</div>", unsafe_allow_html=True)
                st.markdown("- Schedule personal meeting\n- Share impact report\n- Invite to leadership council")
            elif donor['propensity'] > 0.4:
                st.markdown("<div style='background-color:#fffbe6;padding:8px;border-radius:6px;color:#ad8b00;'>Good candidate for upgrade campaign</div>", unsafe_allow_html=True)
                st.markdown("- Send personalized appeal\n- Invite to donor events")
            else:
                st.markdown("<div style='background-color:#f6ffed;padding:8px;border-radius:6px;color:#237804;'>Focus on engagement</div>", unsafe_allow_html=True)
                st.markdown("- Send newsletter\n- Invite to community events")
        
        with rec_col2:
            st.subheader("Gift Capacity Analysis")
            # Calculate suggested ask amount based on past giving and wealth score
            recent_gifts = donor_donations.nlargest(3, 'amount')['amount']
            base_amount = recent_gifts.mean() if len(recent_gifts) > 0 else 0
            wealth_factor = 1 + (donor_wealth['wealth_score_ext'] * 2)  # Up to 3x for highest wealth score
            frequency_bonus = min(donor['frequency'] / 10, 1)  # Bonus for frequent donors
            suggested_amount = base_amount * wealth_factor * (1 + frequency_bonus)
            
            st.metric(
                "Suggested Ask Amount",
                f"${suggested_amount:,.2f}",
                f"Based on giving history and wealth score"
            )
            
        # Action Buttons
        st.header("Actions")
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if st.button("Send Email"):
                st.info(f"Sending email to {donor['first_name']}...")
        
        with col2:
            if st.button("Add to Campaign"):
                st.info("Adding donor to campaign list...")
        
        with col3:
            if st.button("Export Profile"):
                st.info("Preparing export...")
        
    else:
        st.warning("No donors found matching your search.")
else:
    st.info("Search for a donor using the sidebar to view their complete profile.")