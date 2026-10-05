import os
import requests
from datetime import datetime, timezone
from dotenv import load_dotenv
load_dotenv()

API_URL = "https://api.opentransportdata.swiss/la/gtfs-rt"
API_KEY = os.environ.get("GTFS_RT_API_KEY")

def fetch_gtfs_rt(save_dir="data/raw/gtfs_rt"):
    if not API_KEY:
        raise RuntimeError("Set the GTFS_RT_API_KEY environment variable first.")

    os.makedirs(save_dir, exist_ok=True)
    headers = {"Authorization": f"Bearer {API_KEY}"}

    response = requests.get(API_URL, headers=headers, allow_redirects=True)
    response.raise_for_status()

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    filepath = os.path.join(save_dir, f"gtfs_rt_{timestamp}.pb")

    with open(filepath, "wb") as f:
        f.write(response.content)

    print(f"Saved {len(response.content)} bytes to {filepath}")
    return filepath

if __name__ == "__main__":
    fetch_gtfs_rt()