"""
Advanced visualization components for donor analytics
"""
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import numpy as np
from typing import Dict, List, Optional
import folium
from folium.plugins import HeatMap, MarkerCluster

class DonorVisualization:
    def __init__(self):
        self.color_palette = px.colors.qualitative.Set3
        
    def plot_donor_journey(self, 
                          donor_id: int,
                          donations: pd.DataFrame,
                          events: pd.DataFrame) -> go.Figure:
        """Create donor journey visualization"""
        # Filter data for donor
        donor_donations = donations[donations['donor_id'] == donor_id]
        donor_events = events[events['donor_id'] == donor_id]
        
        # Create timeline
        fig = go.Figure()
        
        # Add donations
        fig.add_trace(go.Scatter(
            x=donor_donations['donation_date'],
            y=donor_donations['amount'],
            mode='markers+lines',
            name='Donations',
            marker=dict(size=10, symbol='circle'),
            line=dict(color='blue')
        ))
        
        # Add events
        if len(donor_events) > 0:
            fig.add_trace(go.Scatter(
                x=donor_events['event_date'],
                y=[0] * len(donor_events),
                mode='markers',
                name='Events',
                marker=dict(size=8, symbol='star')
            ))
        
        fig.update_layout(
            title=f"Donor Journey (ID: {donor_id})",
            xaxis_title="Date",
            yaxis_title="Donation Amount ($)",
            showlegend=True
        )
        
        return fig
        
    def plot_giving_patterns(self,
                           donations: pd.DataFrame) -> Dict[str, go.Figure]:
        """Analyze and visualize giving patterns"""
        figures = {}
        
        # Prepare time-based data
        donations['date'] = pd.to_datetime(donations['donation_date'])
        donations['month'] = donations['date'].dt.month
        donations['month_name'] = donations['date'].dt.strftime('%B')
        donations['day_of_week'] = donations['date'].dt.dayofweek
        donations['day_name'] = donations['date'].dt.strftime('%A')
        
        # Monthly patterns
        monthly_stats = donations.groupby('month_name').agg({
            'amount': ['count', 'mean', 'sum']
        }).reset_index()
        monthly_stats.columns = ['month', 'count', 'average', 'total']
        
        # Sort by month order
        month_order = ['January', 'February', 'March', 'April', 'May', 'June',
                      'July', 'August', 'September', 'October', 'November', 'December']
        monthly_stats['month'] = pd.Categorical(monthly_stats['month'], categories=month_order)
        monthly_stats = monthly_stats.sort_values('month')
        
        figures['monthly'] = go.Figure()
        figures['monthly'].add_trace(go.Bar(
            x=monthly_stats['month'],
            y=monthly_stats['total'],
            name='Total Amount',
            marker_color='lightblue'
        ))
        figures['monthly'].add_trace(go.Scatter(
            x=monthly_stats['month'],
            y=monthly_stats['average'],
            name='Average Gift',
            yaxis='y2',
            line=dict(color='red', width=2)
        ))
        figures['monthly'].update_layout(
            title='Monthly Giving Patterns',
            yaxis=dict(title='Total Donations ($)', side='left'),
            yaxis2=dict(title='Average Gift Size ($)', side='right', overlaying='y'),
            showlegend=True
        )
        
        # Daily patterns
        daily_stats = donations.groupby('day_name').agg({
            'amount': ['count', 'mean', 'sum']
        }).reset_index()
        daily_stats.columns = ['day', 'count', 'average', 'total']
        
        # Sort by day order
        day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        daily_stats['day'] = pd.Categorical(daily_stats['day'], categories=day_order)
        daily_stats = daily_stats.sort_values('day')
        
        figures['daily'] = go.Figure()
        figures['daily'].add_trace(go.Bar(
            x=daily_stats['day'],
            y=daily_stats['count'],
            name='Number of Gifts',
            marker_color='lightblue'
        ))
        figures['daily'].add_trace(go.Scatter(
            x=daily_stats['day'],
            y=daily_stats['average'],
            name='Average Gift',
            yaxis='y2',
            line=dict(color='red', width=2)
        ))
        figures['daily'].update_layout(
            title='Day of Week Patterns',
            yaxis=dict(title='Number of Gifts', side='left'),
            yaxis2=dict(title='Average Gift Size ($)', side='right', overlaying='y'),
            showlegend=True
        )
        
        # Amount distribution
        figures['distribution'] = go.Figure()
        figures['distribution'].add_trace(go.Histogram(
            x=donations['amount'],
            nbinsx=50,
            name='Gift Distribution',
            marker_color='lightblue'
        ))
        figures['distribution'].update_layout(
            title='Gift Size Distribution',
            xaxis_title='Gift Amount ($)',
            yaxis_title='Number of Gifts',
            bargap=0.1
        )
        
        # Cumulative giving
        sorted_donations = donations.sort_values('date')
        sorted_donations['cumulative'] = sorted_donations['amount'].cumsum()
        
        figures['cumulative'] = go.Figure()
        figures['cumulative'].add_trace(go.Scatter(
            x=sorted_donations['date'],
            y=sorted_donations['cumulative'],
            mode='lines',
            name='Cumulative Giving',
            line=dict(color='blue', width=2)
        ))
        figures['cumulative'].update_layout(
            title='Cumulative Giving Over Time',
            xaxis_title='Date',
            yaxis_title='Cumulative Amount ($)',
            showlegend=True
        )
        
        # Update the layouts with consistent height
        for fig in figures.values():
            fig.update_layout(height=600)
            
        return figures
        
    def create_donor_heatmap(self,
                           donors: pd.DataFrame,
                           zoom_start: int = 4) -> folium.Map:
        """Create interactive donor heatmap"""
        # Initialize map
        donor_map = folium.Map(
            location=[39.8283, -98.5795],  # Center of US
            zoom_start=zoom_start
        )
        
        # Add heatmap layer
        locations = donors[['latitude', 'longitude']].values.tolist()
        weights = donors['total_amount'].values.tolist()
        
        HeatMap(
            locations,
            weights,
            min_opacity=0.2,
            radius=15,
            blur=10,
            max_zoom=1,
        ).add_to(donor_map)
        
        # Add marker clusters
        marker_cluster = MarkerCluster().add_to(donor_map)
        
        for idx, row in donors.iterrows():
            folium.Marker(
                location=[row['latitude'], row['longitude']],
                popup=f"Donor ID: {row['donor_id']}<br>"
                      f"Total Giving: ${row['total_amount']:,.2f}<br>"
                      f"Frequency: {row['frequency']}"
            ).add_to(marker_cluster)
            
        return donor_map
        
    def plot_segment_analysis(self,
                            donors: pd.DataFrame,
                            segment_col: str = 'decile') -> Dict[str, go.Figure]:
        """Create comprehensive segment analysis visualizations"""
        figures = {}
        
        # Ensure required columns exist
        required_columns = {
            'total_amount': 0.0,
            'frequency': 0,
            'recency_days': 0,
            'events_attended': 0,
            'volunteer_hours': 0.0
        }
        
        for col, default in required_columns.items():
            if col not in donors.columns:
                donors[col] = default
        
        # Segment size and value
        segment_summary = donors.groupby(segment_col).agg({
            'donor_id': 'count',
            'total_amount': 'sum'
        }).reset_index()
        
        figures['size_value'] = px.scatter(
            segment_summary,
            x='donor_id',
            y='total_amount',
            size='donor_id',
            color=segment_col,
            title='Donor Segments: Size vs Total Giving',
            labels={
                'donor_id': 'Number of Donors',
                'total_amount': 'Total Donations ($)',
                segment_col: 'Segment'
            }
        )
        
        # Segment characteristics
        characteristics = donors.groupby(segment_col).agg({
            'frequency': 'mean',
            'recency_days': 'mean',
            'total_amount': 'mean',
            'events_attended': 'mean',
            'volunteer_hours': 'mean'
        }).reset_index()
        
        figures['characteristics'] = px.parallel_coordinates(
            characteristics,
            color=segment_col,
            title='Segment Characteristics'
        )
        
        # Geographic distribution
        figures['geography'] = px.scatter_mapbox(
            donors,
            lat='latitude',
            lon='longitude',
            color=segment_col,
            size='total_amount',
            title='Geographic Distribution by Segment'
        )
        
        return figures
        
    def plot_retention_analysis(self,
                              donors: pd.DataFrame,
                              donations: pd.DataFrame) -> go.Figure:
        """Analyze and visualize donor retention"""
        # Calculate first donation date for each donor
        first_donations = donations.groupby('donor_id')['donation_date'].min().reset_index()
        first_donations.columns = ['donor_id', 'first_donation_date']
        
        # Merge first donation date with donors
        donors = donors.merge(first_donations, on='donor_id', how='left')
        donors['cohort'] = pd.to_datetime(donors['first_donation_date']).dt.to_period('Y')
        donations['year'] = pd.to_datetime(donations['donation_date']).dt.to_period('Y')
        
        cohort_data = []
        for cohort in donors['cohort'].unique():
            cohort_donors = donors[donors['cohort'] == cohort]['donor_id'].unique()
            for year in donations['year'].unique():
                if year >= cohort:
                    active_donors = donations[
                        (donations['year'] == year) &
                        (donations['donor_id'].isin(cohort_donors))
                    ]['donor_id'].nunique()
                    
                    retention = active_donors / len(cohort_donors)
                    cohort_data.append({
                        'cohort': cohort,
                        'year': year,
                        'retention': retention
                    })
        
        cohort_df = pd.DataFrame(cohort_data)
        
        # Create heatmap
        fig = px.imshow(
            cohort_df.pivot(
                index='cohort',
                columns='year',
                values='retention'
            ),
            title='Donor Retention by Cohort',
            labels={'x': 'Year', 'y': 'Cohort', 'color': 'Retention Rate'}
        )
        
        return fig