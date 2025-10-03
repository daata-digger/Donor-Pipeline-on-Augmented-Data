"""
Enhanced Streamlit Dashboard with Advanced Features
"""
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import geopandas as gpd
from pathlib import Path
import json
import xgboost as xgb
from datetime import datetime, timedelta

# Import dashboard components
from donor_analytics_enterprise.core.visualization import DonorVisualization
from donor_analytics_enterprise.core.analytics import DonorAnalytics

class EnhancedDonorDashboard:
    def __init__(self):
        self.data_dir = Path("data")
        self.load_data()
        self.viz = DonorVisualization()
        self.analytics = DonorAnalytics()
        
    def load_data(self):
        """Load and prepare all necessary data"""
        self.donors = pd.read_csv(self.data_dir / "processed/donor_features.csv")
        self.model = xgb.XGBClassifier()
        self.model.load_model(self.data_dir / "ml/model_xgb.pkl")
        
    def run_dashboard(self):
        """Main dashboard entry point"""
        st.set_page_config(
            page_title="Donor Analytics Enterprise",
            page_icon="🎯",
            layout="wide"
        )
        
        # Sidebar navigation
        st.sidebar.title("Donor Analytics")
        page = st.sidebar.selectbox(
            "Navigation",
            ["Overview", "Donor Search", "Geographic Insights", 
             "RFM Analysis", "Campaign Simulator", "ML Insights"]
        )
        
        if page == "Overview":
            self.overview_page()
        elif page == "Donor Search":
            self.donor_search_page()
        elif page == "Geographic Insights":
            self.geographic_page()
        elif page == "RFM Analysis":
            self.rfm_page()
        elif page == "Campaign Simulator":
            self.campaign_page()
        elif page == "ML Insights":
            self.ml_insights_page()
            
    def overview_page(self):
        """Main overview dashboard"""
        st.title("🎯 Donor Analytics Overview")
        
        # Key Metrics
        metrics = self.analytics.compute_giving_metrics(
            self.donors,
            pd.read_csv(self.data_dir / "raw/donations.csv"),
            pd.read_csv(self.data_dir / "raw/campaigns.csv")
        )
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric(
                "Total Donors",
                f"{metrics['total_donors']:,}",
                f"{metrics['donor_activation_rate']:.1%} Active"
            )
        
        with col2:
            st.metric(
                "Total Donations",
                f"${metrics['total_amount']:,.2f}",
                f"${metrics['avg_donation']:,.2f} Avg"
            )
            
        with col3:
            st.metric(
                "Campaign Success",
                f"{metrics['campaign_success_rate']:.1%}",
                "Goal Achievement"
            )
            
        with col4:
            st.metric(
                "Active Donors",
                f"{metrics['active_donors']:,}",
                f"{metrics['total_donations']:,} Gifts"
            )
            
        # Trend Analysis
        st.header("📈 Giving Trends")
        fig = self.viz.plot_giving_trends(
            pd.read_csv(self.data_dir / "raw/donations.csv")
        )
        st.plotly_chart(fig, use_container_width=True)
        
        # Donor Segments
        st.header("👥 Donor Segments")
        fig = self.viz.plot_donor_segments(self.donors)
        st.plotly_chart(fig, use_container_width=True)
        
    def donor_search_page(self):
        """Enhanced donor search and profile view"""
        st.title("🔍 Donor Search")
        
        # Search functionality
        search_col1, search_col2 = st.columns([3, 1])
        
        with search_col1:
            search_term = st.text_input("Search by name, email, or ID")
            
        with search_col2:
            search_type = st.selectbox(
                "Search Type",
                ["Contains", "Exact Match", "Starts With"]
            )
            
        if search_term:
            results = self.search_donors(search_term, search_type)
            
            if len(results) > 0:
                selected_donor = st.selectbox(
                    "Select Donor",
                    results.apply(lambda x: f"{x['first_name']} {x['last_name']} (ID: {x['donor_id']})", axis=1)
                )
                
                if selected_donor:
                    donor_id = int(selected_donor.split("ID: ")[-1].strip(")"))
                    self.show_donor_profile(donor_id)
            else:
                st.warning("No donors found matching your search criteria.")
                
    def geographic_page(self):
        """Enhanced geographic analysis"""
        st.title("📍 Geographic Analysis")
        
        # Map view selector
        view_type = st.radio(
            "Select View",
            ["State Overview", "Donor Clusters", "Giving Density"]
        )
        
        if view_type == "State Overview":
            fig = self.viz.plot_geographic_distribution(self.donors)
            st.plotly_chart(fig, use_container_width=True)
            
        elif view_type == "Donor Clusters":
            donor_map = self.create_donor_cluster_map()
            st.components.v1.html(donor_map._repr_html_(), height=600)
            
        else:  # Giving Density
            fig = self.create_giving_density_map()
            st.plotly_chart(fig, use_container_width=True)
            
    def rfm_page(self):
        """Enhanced RFM analysis"""
        st.title("📊 RFM Analysis")
        
        # RFM Score Distribution
        st.header("RFM Score Distribution")
        fig = self.viz.plot_rfm_distribution(self.donors)
        st.plotly_chart(fig, use_container_width=True)
        
        # Segment Analysis
        st.header("Segment Analysis")
        segments = self.analyze_segments()
        st.plotly_chart(segments, use_container_width=True)
        
        # Detailed Segment Stats
        st.header("Segment Statistics")
        stats = self.get_segment_stats()
        st.dataframe(stats)
        
    def campaign_page(self):
        """Enhanced campaign simulator"""
        st.title("🎯 Campaign Simulator")
        
        # Campaign Configuration
        st.header("Campaign Setup")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            campaign_type = st.selectbox(
                "Campaign Type",
                ["Annual Fund", "Capital Campaign", "Emergency Relief", "Planned Giving"]
            )
            
        with col2:
            campaign_goal = st.number_input(
                "Campaign Goal ($)",
                min_value=10000,
                max_value=10000000,
                value=100000,
                step=10000
            )
            
        with col3:
            duration_months = st.slider(
                "Campaign Duration (months)",
                min_value=1,
                max_value=24,
                value=3
            )
            
        # Target Parameters
        st.subheader("Targeting Parameters")
        
        col1, col2 = st.columns(2)
        
        with col1:
            min_propensity = st.slider(
                "Minimum Propensity Score",
                min_value=0.0,
                max_value=1.0,
                value=0.3,
                format="%.2f"
            )
            
        with col2:
            min_capacity = st.slider(
                "Minimum Gift Capacity",
                min_value=100,
                max_value=100000,
                value=1000,
                step=100,
                format="$%d"
            )
            
        # Run simulation
        if st.button("Run Simulation"):
            self.run_campaign_simulation(
                campaign_type,
                campaign_goal,
                duration_months,
                min_propensity,
                min_capacity
            )
            
    def ml_insights_page(self):
        """ML model insights and explanations"""
        st.title("🤖 Machine Learning Insights")
        
        # Feature Importance
        st.header("Feature Importance")
        importance_fig = self.plot_feature_importance()
        st.plotly_chart(importance_fig, use_container_width=True)
        
        # Model Performance
        st.header("Model Performance")
        performance_fig = self.plot_model_performance()
        st.plotly_chart(performance_fig, use_container_width=True)
        
        # SHAP Values
        st.header("SHAP Analysis")
        shap_fig = self.plot_shap_values()
        st.plotly_chart(shap_fig, use_container_width=True)
        
    def search_donors(self, term, search_type):
        """Enhanced donor search"""
        if search_type == "Contains":
            mask = (
                self.donors['first_name'].str.contains(term, case=False) |
                self.donors['last_name'].str.contains(term, case=False) |
                self.donors['email'].str.contains(term, case=False) |
                self.donors['donor_id'].astype(str).str.contains(term)
            )
        elif search_type == "Exact Match":
            mask = (
                (self.donors['first_name'].str.lower() == term.lower()) |
                (self.donors['last_name'].str.lower() == term.lower()) |
                (self.donors['email'].str.lower() == term.lower()) |
                (self.donors['donor_id'].astype(str) == term)
            )
        else:  # Starts With
            mask = (
                self.donors['first_name'].str.startswith(term, case=False) |
                self.donors['last_name'].str.startswith(term, case=False) |
                self.donors['email'].str.startswith(term, case=False) |
                self.donors['donor_id'].astype(str).str.startswith(term)
            )
        
        return self.donors[mask]
        
    def show_donor_profile(self, donor_id):
        """Enhanced donor profile display"""
        donor = self.donors[self.donors['donor_id'] == donor_id].iloc[0]
        
        st.header(f"Donor Profile: {donor['first_name']} {donor['last_name']}")
        
        # Contact Information
        st.subheader("📋 Contact Information")
        contact_col1, contact_col2 = st.columns(2)
        
        with contact_col1:
            st.write(f"**Email:** {donor['email']}")
            st.write(f"**Phone:** {donor.get('phone', 'N/A')}")
            
        with contact_col2:
            st.write(f"**Address:** {donor['city']}, {donor['state']} {donor.get('postal_code', '')}")
            st.write(f"**Join Date:** {donor.get('join_date', 'N/A')}")
            
        # Giving History
        st.subheader("💰 Giving History")
        metrics_col1, metrics_col2, metrics_col3, metrics_col4 = st.columns(4)
        
        with metrics_col1:
            st.metric("Total Giving", f"${donor['monetary']:,.2f}")
            
        with metrics_col2:
            st.metric("Gift Frequency", f"{donor['frequency']} gifts")
            
        with metrics_col3:
            st.metric("Last Gift", f"{donor['recency_days']} days ago")
            
        with metrics_col4:
            propensity = self.model.predict_proba(donor[self.model.feature_names_in_].to_frame().T)[0][1]
            st.metric("Propensity Score", f"{propensity:.1%}")
            
        # Giving Trend
        st.subheader("📈 Giving Trend")
        trend_fig = self.plot_donor_trend(donor_id)
        st.plotly_chart(trend_fig, use_container_width=True)
        
        # Engagement
        st.subheader("🤝 Engagement")
        engagement_col1, engagement_col2 = st.columns(2)
        
        with engagement_col1:
            st.metric("Events Attended", int(donor.get('events_attended', 0)))
            
        with engagement_col2:
            st.metric("Volunteer Hours", donor.get('volunteer_hours', 0))
            
        # Actions
        st.subheader("📱 Actions")
        action_col1, action_col2, action_col3 = st.columns(3)
        
        with action_col1:
            if st.button("📧 Send Email"):
                st.info("Email functionality would be integrated here")
                
        with action_col2:
            if st.button("📞 Log Call"):
                st.info("Call logging would be integrated here")
                
        with action_col3:
            if st.button("📊 Download Report"):
                st.info("Report download would be integrated here")

if __name__ == "__main__":
    dashboard = EnhancedDonorDashboard()
    dashboard.run_dashboard()