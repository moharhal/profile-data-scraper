import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from getprog.cassandra_connection import CassandraConnector
from getprog.utils import write_page_to_file, read_page_from_file
from getprog.requests_ import *
import logging
import sys
from typing import Dict, Any

# Create a logger instance
logger = logging.getLogger(__name__)
logging.basicConfig(
    filename="script.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="w",
)


def process_profile(profile: Dict[str, Any], headers: Dict[str, Any]) -> str:
    """
    Processes a profile by fetching additional data and formatting it for insertion into Cassandra.

    :param profile: The profile data to process.
    :param headers: The headers to use for the request.
    :return: The formatted profile data as a JSON string.
    """
    profile_id = profile["profile"]["id"]
    profile_data = get_profile(profile_id, headers, logger)

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
            profile_data = get_profile(profile_id, headers, logger)


def main():
    """
    Main function that orchestrates the process of fetching, processing, and inserting profile data into Cassandra.
    """
    cassandra_conn = CassandraConnector(keyspace="getprog_ia", logging=logger)
    headers = {"Authorization": f"{get_token()}"}
    page: int = read_page_from_file()
    counter = 0
    with ThreadPoolExecutor(max_workers=40) as executor:
        page_resolt_check = 0
        while page < 50000:
            print(page)
            response_json = get_data(page, headers, logger)

            if response_json == "unauthorized":
                logging.info("Token expired. Refreshing token...")
                headers = {"Authorization": f"{get_token()}"}
                continue

            if not response_json.get("results"):
                page_resolt_check += 1
                logging.info(f"No results found for page {page}.")
                if page_resolt_check == 10:
                    page += 1
                    continue

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
            counter += 1
            if counter == 5:
                print("======================= stop counter ")
                break
            write_page_to_file(page)


if __name__ == "__main__":
    unique_id = sys.argv[1]  # Get the unique identifier from the command line arguments
    logging.info(f"Running script with unique ID: {unique_id}")
    main()
