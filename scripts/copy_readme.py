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

    # remove irrelavant sections
    readme_content = re.sub(
        "<!-- readme header split -->(.*?)<!-- readme header end -->",
        "",
        readme_content,
        flags=re.DOTALL,
    )

    with open(output_file, "w") as f:
        f.write(FRONT_MATTER + readme_content)


if __name__ == "__main__":
    main()
