"""
Script to copy raw data files to enterprise project
"""
import shutil
import pathlib

src_dir = pathlib.Path(__file__).resolve().parents[2] / 'data/raw'
dest_dir = pathlib.Path(__file__).resolve().parent / 'data/raw'

if not dest_dir.exists():
    dest_dir.mkdir(parents=True)

files_to_copy = [
    'campaigns.csv',
    'donations.csv',
    'donors.csv',
    'engagement_events.csv',
    'wealth_external.csv'
]

for file in files_to_copy:
    shutil.copy(src_dir / file, dest_dir / file)
    print(f"Copied {file}")