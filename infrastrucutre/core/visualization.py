"""
Visualization utilities for donor analytics
Supports both Streamlit and Tableau integrations
"""
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import numpy as np
from typing import Dict, List, Optional
from pathlib import Path

class DonorVisualization:
    def __init__(self, data_dir: Optional[Path] = None):
        self.data_dir = data_dir or Path("data")
        
    def load_data(self) -> Dict[str, pd.DataFrame]:
        """Load processed data for visualization"""
        donor_features = pd.read_csv(self.data_dir / "processed" / "donor_features.csv")
        scored_donors = pd.read_csv(self.data_dir / "processed" / "scored_donors.csv")
        return {
            'donor_features': donor_features,
            'scored_donors': scored_donors
        }
        
    def plot_rfm_distribution(self, donor_features: pd.DataFrame) -> go.Figure:
        """Create RFM distribution plots"""
        # Create subplots for R, F, M distributions
        fig = go.Figure()
        
        # Recency distribution
        fig.add_trace(go.Histogram(
            x=donor_features['recency_days'],
            name='Recency',
            nbinsx=50,
            opacity=0.75
        ))
        
        fig.update_layout(
            title="Donor Recency Distribution",
            xaxis_title="Days Since Last Donation",
            yaxis_title="Count of Donors",
            showlegend=True
        )
        
        return fig
        
    def plot_giving_trends(self, donations: pd.DataFrame) -> go.Figure:
        """Plot giving trends over time"""
        daily_totals = donations.groupby('donation_date')['amount'].sum().reset_index()
        
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=daily_totals['donation_date'],
            y=daily_totals['amount'],
            mode='lines+markers',
            name='Daily Donations'
        ))
        
        fig.update_layout(
            title="Daily Donation Trends",
            xaxis_title="Date",
            yaxis_title="Total Donations ($)",
            showlegend=True
        )
        
        return fig
        
    def plot_donor_segments(self, donor_features: pd.DataFrame) -> go.Figure:
        """Create donor segment visualization"""
        fig = px.scatter(
            donor_features,
            x='total_amount',
            y='frequency',
            size='recency_days',
            color='engagement_score',
            hover_data=['donor_id', 'city', 'state'],
            title="Donor Segments by RFM"
        )
        
        return fig
        
    def create_kpi_cards(self, metrics: Dict[str, float]) -> None:
        """Display KPI metrics in Streamlit"""
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
            
    def plot_geographic_distribution(self, donor_features: pd.DataFrame) -> go.Figure:
        """Create geographic distribution visualization"""
        state_summary = donor_features.groupby('state').agg({
            'donor_id': 'count',
            'total_amount': 'sum',
            'frequency': 'mean'
        }).reset_index()
        
        fig = go.Figure(data=go.Choropleth(
            locations=state_summary['state'],
            z=state_summary['total_amount'],
            locationmode='USA-states',
            colorscale='Viridis',
            colorbar_title="Total Donations"
        ))
        
        fig.update_layout(
            title_text='Donation Distribution by State',
            geo_scope='usa',
        )
        
        return fig
        
    def create_tableau_dashboard(self, donor_features: pd.DataFrame, tableau_path: Path):
        """Generate Tableau dashboard template"""
        # This is a placeholder for Tableau integration
        # In a real implementation, this would:
        # 1. Generate a Tableau workbook
        # 2. Add worksheets for each visualization
        # 3. Create a dashboard layout
        # 4. Save the workbook
        pass
        
    def run_streamlit_dashboard(self):
        """Run the main Streamlit dashboard"""
        st.title("🎯 Donor Analytics Dashboard")
        
        # Load data
        data = self.load_data()
        donor_features = data['donor_features']
        scored_donors = data['scored_donors']
        
        # Display KPIs
        metrics = {
            'total_donors': len(donor_features),
            'active_donors': len(scored_donors),
            'total_donations': donor_features['total_amount'].sum(),
            'avg_donation': donor_features['total_amount'].mean(),
            'donor_activation_rate': len(scored_donors) / len(donor_features),
            'campaign_success_rate': 0.85  # This would come from actual data
        }
        self.create_kpi_cards(metrics)
        
        # RFM Distribution
        st.header("📊 Donor RFM Analysis")
        rfm_fig = self.plot_rfm_distribution(donor_features)
        st.plotly_chart(rfm_fig, use_container_width=True)
        
        # Geographic Distribution
        st.header("🗺️ Geographic Distribution")
        geo_fig = self.plot_geographic_distribution(donor_features)
        st.plotly_chart(geo_fig, use_container_width=True)
        
        # Donor Segments
        st.header("👥 Donor Segments")
        segments_fig = self.plot_donor_segments(donor_features)
        st.plotly_chart(segments_fig, use_container_width=True)
        
        # Export Options
        st.header("📤 Export Options")
        if st.button("Download Full Report"):
            # Generate report
            pass
        
        if st.button("Create Tableau Dashboard"):
            # Generate Tableau workbook
            pass