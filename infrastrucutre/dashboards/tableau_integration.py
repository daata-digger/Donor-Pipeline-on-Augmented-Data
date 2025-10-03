"""
Tableau Integration Module for Enterprise Donor Analytics
Handles automatic publishing and updating of Tableau workbooks.
"""
import os
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
from tableauserverclient as TSC
from dotenv import load_dotenv

load_dotenv()

class TableauPublisher:
    def __init__(self):
        self.server = os.getenv('TABLEAU_SERVER')
        self.username = os.getenv('TABLEAU_USERNAME')
        self.password = os.getenv('TABLEAU_PASSWORD')
        self.site = os.getenv('TABLEAU_SITE')
        
        self.auth = TSC.TableauAuth(
            username=self.username,
            password=self.password,
            site=self.site
        )
        self.server = TSC.Server(self.server)

    def publish_workbook(
        self,
        workbook_path: str,
        project_name: str,
        name: Optional[str] = None
    ) -> str:
        """Publish a Tableau workbook to server."""
        with self.server.auth.sign_in(self.auth):
            # Find project
            all_projects, _ = self.server.projects.get()
            project = next((p for p in all_projects if p.name == project_name), None)
            
            if not project:
                raise ValueError(f"Project {project_name} not found")

            # Publish workbook
            workbook = Path(workbook_path)
            new_workbook = TSC.WorkbookItem(project_id=project.id, name=name or workbook.stem)
            
            with open(workbook_path, 'rb') as wb:
                published_workbook = self.server.workbooks.publish(
                    new_workbook,
                    wb,
                    'Overwrite'
                )
            
            return published_workbook.id

    def refresh_datasource(self, datasource_name: str) -> None:
        """Trigger a refresh of a Tableau datasource."""
        with self.server.auth.sign_in(self.auth):
            all_datasources, _ = self.server.datasources.get()
            datasource = next(
                (ds for ds in all_datasources if ds.name == datasource_name),
                None
            )
            
            if not datasource:
                raise ValueError(f"Datasource {datasource_name} not found")
            
            self.server.datasources.refresh(datasource)

    def create_donor_dashboard(
        self,
        donor_data: pd.DataFrame,
        campaign_data: pd.DataFrame
    ) -> str:
        """Create and publish donor analytics dashboard."""
        # Template path
        template_path = Path(__file__).parent / "templates" / "donor_analytics.twb"
        
        # Create temporary directory for data
        temp_dir = Path("temp_tableau")
        temp_dir.mkdir(exist_ok=True)
        
        # Save data as extracts
        donor_data.to_csv(temp_dir / "donors.csv", index=False)
        campaign_data.to_csv(temp_dir / "campaigns.csv", index=False)
        
        # Publish workbook
        workbook_id = self.publish_workbook(
            str(template_path),
            "Donor Analytics",
            "Donor Performance Dashboard"
        )
        
        return workbook_id

    def schedule_refresh(
        self,
        workbook_id: str,
        schedule: Dict[str, str]
    ) -> None:
        """Schedule automatic refresh for a workbook."""
        with self.server.auth.sign_in(self.auth):
            workbook = self.server.workbooks.get_by_id(workbook_id)
            
            # Create schedule
            schedule_item = TSC.ScheduleItem(
                name=f"Refresh {workbook.name}",
                priority=50,
                schedule_type=TSC.ScheduleItem.Type.Daily,
                **schedule
            )
            
            # Add workbook to schedule
            self.server.schedules.create(schedule_item)
            self.server.schedules.add_to_schedule(schedule_item, workbook)

    def get_dashboard_metrics(self) -> Dict[str, float]:
        """Retrieve key metrics from Tableau dashboards."""
        metrics = {
            'total_donations': 0,
            'donor_retention_rate': 0,
            'campaign_success_rate': 0,
            'avg_donation_amount': 0
        }
        
        with self.server.auth.sign_in(self.auth):
            views, _ = self.server.views.get()
            for view in views:
                if view.name == "Donor Metrics":
                    # Get view data
                    view_data = self.server.views.query_data(view)
                    metrics.update(self._parse_metrics(view_data))
        
        return metrics

    def _parse_metrics(self, data: List[List[str]]) -> Dict[str, float]:
        """Parse raw data from Tableau into metrics dictionary."""
        metrics = {}
        for row in data:
            try:
                metric_name = row[0]
                metric_value = float(row[1])
                metrics[metric_name] = metric_value
            except (IndexError, ValueError):
                continue
        return metrics