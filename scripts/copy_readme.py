import argparse
import base64
import re
import requests


FRONT_MATTER = (
    "---\ntitle: Python Library\nslug: python-library-1\ncategory: CATEGORY_ID\n---\n\n"
)


def get_category_id(rdme_key: str):
    """
    Get the category ID for the readme doc
    """
    encoded_key = base64.b64encode(f"{rdme_key}:".encode()).decode()

    url = "https://dash.readme.com/api/v1/categories/guides"
    headers = {"accept": "application/json", f"authorization": f"Basic {encoded_key}"}

    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        raise Exception("Failed to get readme category id")
    else:
        resp_json = response.json()
        return resp_json["id"]


def main():
    """
    Create custom README copy that adds front matter and deletes irrelevant fields for readme doc deployment
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("-k", "--key")
    args = parser.parse_args()

    rdme_key = args.key
    cat_id = get_category_id(rdme_key)
    readme_front_matter = FRONT_MATTER.replace("CATEGORY_ID", cat_id)

    input_readme = "README.md"
    output_file = "README_COPY.md"
    with open(input_readme, "r") as file:
        readme_content = file.read()

    # remove irrelavant sections
    readme_content = re.sub(
        "<!-- readme header split -->(.*?)<!-- readme header end -->",
        "",
        readme_content,
        flags=re.DOTALL,
    )

    with open(output_file, "w") as f:
        f.write(readme_front_matter + readme_content)


if __name__ == "__main__":
    main()
