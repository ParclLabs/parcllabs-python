import argparse
import base64
import requests


def main():
    """
    Get the latest version of the readme API
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("-k", "--key")
    args = parser.parse_args()

    rdme_key = args.key
    encoded_key = base64.b64encode(f"{rdme_key}:".encode()).decode()

    url = "https://dash.readme.com/api/v1/version"
    headers = {"accept": "application/json", f"authorization": f"Basic {encoded_key}"}

    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        raise Exception("Failed to get previous readme version")
    else:
        versions = response.json()
        times = [version["createdAt"] for version in versions]
        latest_time = max(times)
        latest_version = [
            version for version in versions if version["createdAt"] == latest_time
        ][0]
        print(latest_version["version_clean"])


if __name__ == "__main__":
    main()
