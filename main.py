import os
import csv
import requests
import threading
import time

# Define the API URLs
API_BASE_URL = "https://portal.gorivir.com/api/external/pim/assets/upc/"
DOWNLOAD_BASE_URL = "https://portal.gorivir.com/api/external/dam/asset/download/"
LOGIN_URL = "https://portal.gorivir.com/api/external/login"

# Define the folder to save images
SAVE_FOLDER = "G:\Images"
os.makedirs(SAVE_FOLDER, exist_ok=True)

# Replace with your actual username and password
USERNAME = "stan@coolscience.com"
PASSWORD = "StanfordS392817*"

# Global headers dictionary to store the authorization token
HEADERS = {}
token_acquired = threading.Event()


def update_token():
    """Fetch a new authorization token and refresh it every 30 minutes."""
    global HEADERS
    while True:
        try:
            response = requests.post(LOGIN_URL, json={"userName": USERNAME, "password": PASSWORD})
            response.raise_for_status()
            token_data = response.json().get("data", {})
            access_token = token_data.get("access_token")
            if access_token:
                HEADERS['Authorization'] = f'Bearer {access_token}'
                print("Authorization token updated successfully.")
                token_acquired.set()  # Signal that the token has been acquired
            else:
                print("Failed to retrieve access_token.")
        except requests.RequestException as e:
            print(f"Error updating token: {e}")

        # Wait 30 minutes before updating the token again
        time.sleep(1800)


# Start the token updater thread and wait for the initial token
token_thread = threading.Thread(target=update_token, daemon=True)
token_thread.start()

# Wait for the token to be acquired before proceeding with API calls
token_acquired.wait()


def get_upcs_from_csv(file_path):
    """Reads UPCs from a CSV file and returns them as a list."""
    upcs = []
    with open(file_path, mode='r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            upcs.append(row['UPC#12'])
    return upcs


def fetch_asset_ids(upc):
    """Fetch asset unique IDs for a given UPC, filtering by 'MARKETING' imageType."""
    response = requests.get(f"{API_BASE_URL}{upc}", headers=HEADERS)
    response.raise_for_status()
    data = response.json().get("data", {})

    if not data or "assets" not in data:
        print(f"No assets found for UPC: {upc}")
        return []

    # Filter assets for "imageType": "MARKETING"
    marketing_assets = [asset["uniqueId"] for asset in data["assets"] if asset.get("imageType") == "MARKETING"]
    return marketing_assets


def download_image(upc, unique_id, index):
    """Download an image given a unique ID, naming it as UPC-index.tif."""
    response = requests.get(f"{DOWNLOAD_BASE_URL}{unique_id}", headers=HEADERS, stream=True)
    response.raise_for_status()

    file_path = os.path.join(SAVE_FOLDER, f"{upc}-{index}.tif")
    with open(file_path, 'wb') as file:
        for chunk in response.iter_content(1024):
            file.write(chunk)

    print(f"Downloaded image for UPC {upc} with unique ID {unique_id} to {file_path}")


def process_upcs(file_path):
    """Process each UPC to fetch and download images with specified naming convention."""
    upcs = get_upcs_from_csv(file_path)
    for upc in upcs:
        print(f"Processing UPC: {upc}")
        unique_ids = fetch_asset_ids(upc)

        if not unique_ids:
            print(f"No MARKETING images found for UPC: {upc}")
            continue

        for index, unique_id in enumerate(unique_ids):
            download_image(upc, unique_id, index)


process_upcs('C:/Users/amazi/OneDrive/Desktop/KEHEGoRivir/KEHE.CSV')


