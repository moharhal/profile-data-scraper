import requests
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from web_scraping.cassandra_connection import CassandraConnector


def get_token():
    """
    Fetches the API token from endpoint.

    """
    url = "https://b23d-196-115-56-255.ngrok-free.app/token"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        return data.get("token", "").strip()
    else:
        print(f"Failed to fetch token: {response.status_code}")
        return ""


def get_data(page, headers):
    url = "https://api.app.getprog.ai/api/search/"
    payload = {"page": page, "seniority": ["Senior"], "size": 100}

    try:
        response = requests.post(url, json=payload, headers=headers)
        if response.status_code == 401:
            return "unauthorized"
        return response.json()
    except Exception as e:
        print("Request failed:", e)
        return "error"


def get_profile(profile_id, headers):
    profile_url = f"https://api.app.getprog.ai/api/candidates/{profile_id}"
    try:
        profile_response = requests.get(profile_url, headers=headers)
        if profile_response.status_code == 401:
            return "unauthorized"
        return profile_response.json()
    except Exception as e:
        print("Profile request failed:", e)
        return "error"


def process_profile(profile, headers):
    profile_id = profile["profile"]["id"]
    profile_data = get_profile(profile_id, headers)

    if profile_data == "unauthorized":
        print("unauthorized")
        time.sleep(10)
        headers = {"Authorization": f"{get_token()}"}
        profile_data = get_profile(profile_id, headers)

    if profile_data != "error":
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
    return None


def main():
    cassandra_conn = CassandraConnector(keyspace="getprog_ia")
    headers = {"Authorization": f"{get_token()}"}
    page = 336

    with ThreadPoolExecutor(max_workers=50) as executor:
        while page < 203982:
            response_json = get_data(page, headers)
            if response_json == "unauthorized":
                print("data unauthorized")
                time.sleep(10)
                headers = {"Authorization": f"{get_token()}"}
                continue
            elif response_json == "error":
                break

            print("page: ", page)
            futures = [
                executor.submit(process_profile, profile, headers)
                for profile in response_json.get("results", [])
            ]
            for future in as_completed(futures):
                profile_data = future.result()
                if profile_data:
                    cassandra_conn.insert_to_cassandra(profile_data)

            page += 1


if __name__ == "__main__":
    main()
