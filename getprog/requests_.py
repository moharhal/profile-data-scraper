from typing import Dict, Any, Union
import requests
import time
import logging


def get_token(max_retries: int = 100, backoff_factor: int = 2) -> str:
    """
    Fetches the API token from endpoint with retry mechanism and exponential backoff.

    :param max_retries: Maximum number of retries.
    :param backoff_factor: Factor by which the delay between retries will increase.
    :return: The fetched token or an empty string if the request fails.
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


def get_profile(
    profile_id: str, headers: Dict[str, Any], logging: logging.Logger
) -> Union[Dict[str, Any], str]:
    """
    Retrieves the profile information for a given profile ID.

    :param profile_id: The ID of the profile to fetch.
    :param headers: The headers to include in the request.
    :param logging: The logging instance to use for logging messages.
    :return: The profile information as a dictionary or "unauthorized" if access is denied.
    """

    profile_url = f"https://api.app.getprog.ai/api/candidates/{profile_id}"

    while True:
        try:
            profile_response = requests.get(profile_url, headers=headers, timeout=100)
            if profile_response.status_code == 401:
                return "unauthorized"
            return profile_response.json()
        except Exception as e:
            logging.info("Profile request failed: retrying ... ")
            time.sleep(3)


def get_data(
    page: int, headers: Dict[str, Any], logging: logging.Logger
) -> Union[Dict[str, Any], str]:
    """
    Retrieves data for a given page with specified headers.

    :param page: The page number to fetch data from.
    :param headers: The headers to include in the request.
    :param logging: The logging instance to use for logging messages.
    :return: The data as a dictionary or "unauthorized" if access is denied.
    """

    url = "https://api.app.getprog.ai/api/search/"
    payload = {"page": page, "seniority": ["Senior"], "size": 100}

    while True:
        try:
            response = requests.post(url, json=payload, headers=headers, timeout=100)
            if response.status_code == 401:
                return "unauthorized"
            return response.json()
        except Exception as e:
            logging.info("Request failed: retrying ...")
            time.sleep(3)
