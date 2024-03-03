import requests
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from web_scraping.cassandra_connection import CassandraConnector
import os

import logging
import sys

PAGE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "current_page.txt")
# Set up logging
logging.basicConfig(
    filename="script.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="w",
)


def get_token(max_retries=100, backoff_factor=2):
    """
    Fetches the API token from endpoint with retry mechanism and exponential backoff.

    :param max_retries: Maximum number of retries.
    :param backoff_factor: Factor by which the delay between retries will increase.
    """

    url = "https://mohalocal.loca.lt/token"
    for attempt in range(max_retries):
        response = requests.get(url, verify=False)
        if response.status_code == 200:
            data = response.json()
            return data.get("token", "").strip()
        else:
            print(
                f"Failed to fetch token: {response.status_code}, retrying... {attempt}"
            )
            time.sleep(backoff_factor**attempt)

    print("Exceeded maximum retries, failed to fetch token.")
    return ""


def get_data(page, headers):
    url = "https://api.app.getprog.ai/api/search/"
    payload = {"page": page, "seniority": ["Senior"], "size": 100}

    while True:
        try:
            response = requests.post(url, json=payload, headers=headers)
            if response.status_code == 401:
                return "unauthorized"
            return response.json()
        except Exception as e:
            logging.info("Request failed: retrying ...")
            time.sleep(3)  # Wait a bit before retrying


def get_profile(profile_id, headers):
    profile_url = f"https://api.app.getprog.ai/api/candidates/{profile_id}"

    while True:
        try:
            profile_response = requests.get(profile_url, headers=headers)
            if profile_response.status_code == 401:
                return "unauthorized"
            return profile_response.json()
        except Exception as e:
            logging.info("Profile request failed: retrying ... ")
            time.sleep(3)  # Wait a bit before retrying


def process_profile(profile, headers):
    profile_id = profile["profile"]["id"]
    profile_data = get_profile(profile_id, headers)

    while True:
        try:
            profile_data = profile_data.get("profile")
            profile_data["is_first_name_female"] = profile["profile"][
                "is_first_name_female"
            ]
            profile_data["sub_region"] = profile["profile"].get("sub_region")
            profile_data["region"] = profile["profile"].get("region")
            profile_data["weight"] = profile.get("weight")
            profile_data["match_score"] = profile.get("match_score")
            profile_data = json.dumps(profile_data)
            return profile_data
        except:
            logging.info("Retrying profile request...")
            time.sleep(3)
            headers = {"Authorization": f"{get_token()}"}
            profile_data = get_profile(profile_id, headers)


def read_page_from_file():
    if os.path.exists(PAGE_FILE):
        with open(PAGE_FILE, "r") as file:
            return int(file.read().strip())
    return 3193  # Default starting page


def write_page_to_file(page):
    with open(PAGE_FILE, "w") as file:
        file.write(str(page))


def main():
    cassandra_conn = CassandraConnector(keyspace="getprog_ia", logging=logging)
    headers = {"Authorization": f"{get_token()}"}
    page = read_page_from_file()

    with ThreadPoolExecutor(max_workers=40) as executor:
        while page < 50000:
            response_json = get_data(page, headers)

            if response_json == "unauthorized":
                logging.info("Token expired. Refreshing token...")
                headers = {"Authorization": f"{get_token()}"}
                continue

            if not response_json.get("results"):
                logging.info(f"No results found for page {page}.")
                break

            logging.info(f"Processing page: {page}")
            futures = [
                executor.submit(process_profile, profile, headers)
                for profile in response_json.get("results", [])
            ]
            for future in as_completed(futures):
                profile_data = future.result()
                if profile_data:
                    cassandra_conn.insert_to_cassandra(profile_data, logging)

            page += 1
            write_page_to_file(page)


if __name__ == "__main__":
    unique_id = sys.argv[1]  # Get the unique identifier from the command line arguments
    logging.info(f"Running script with unique ID: {unique_id}")
    main()
