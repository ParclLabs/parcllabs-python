import re


FRONT_MATTER = "---\ntitle: Python Library\nslug: python-library-1\n---\n\n"


def main():
    """
    Create custom README copy that adds front matter and deletes irrelevant fields for readme doc deployment
    """
    input_readme = "README.md"
    output_file = "README_COPY.md"
    with open(input_readme, "r") as file:
        readme_content = file.read()

    # remove links
    readme_content = re.sub("!\[.*]\(.*\)\n", "", readme_content)

    # remove irrelavant sections
    for section in readme_content.split("\n### "):
        if section.startswith("Parcl Labs Data Overview"):
            full_section = "\n### " + section
            readme_content = readme_content.replace(full_section, "")

    with open(output_file, "w") as f:
        f.write(FRONT_MATTER + readme_content)


if __name__ == "__main__":
    main()
