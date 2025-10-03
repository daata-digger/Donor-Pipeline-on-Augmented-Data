"""
Geographic analysis visualization component
"""
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from typing import Dict

def plot_geographic_analysis(donors: pd.DataFrame) -> Dict[str, go.Figure]:
    """Create geographic analysis visualizations"""
    figures = {}
    
    # Ensure required columns exist
    if 'state' not in donors.columns:
        donors['state'] = 'NY'  # Default to NY for demo
    if 'total_amount' not in donors.columns:
        donors['total_amount'] = 0.0
        
    # State-level analysis
    state_summary = donors.groupby('state').agg({
        'donor_id': 'count',
        'total_amount': 'sum'
    }).reset_index()
    
    # State distribution map
    figures['state_map'] = px.choropleth(
        state_summary,
        locations='state',
        locationmode='USA-states',
        color='total_amount',
        scope='usa',
        title='Total Donations by State',
        labels={'total_amount': 'Total Donations ($)'},
        color_continuous_scale='Viridis'
    )
    
    # Top states bar chart
    top_states = state_summary.nlargest(10, 'total_amount')
    figures['top_states'] = px.bar(
        top_states,
        x='state',
        y='total_amount',
        title='Top 10 States by Donation Volume',
        labels={
            'state': 'State',
            'total_amount': 'Total Donations ($)'
        }
    )
    
    # State donor concentration
    state_summary['donors_pct'] = state_summary['donor_id'] / state_summary['donor_id'].sum() * 100
    state_summary['amount_pct'] = state_summary['total_amount'] / state_summary['total_amount'].sum() * 100
    
    figures['concentration'] = px.scatter(
        state_summary,
        x='donors_pct',
        y='amount_pct',
        text='state',
        title='Donor Concentration by State',
        labels={
            'donors_pct': '% of Total Donors',
            'amount_pct': '% of Total Donations'
        }
    )
    
    return figures