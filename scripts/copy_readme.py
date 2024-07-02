FRONT_MATTER = "---\ntitle: Python Library\nslug: python-library-1\n---\n\n"


def main():
    """
    Create custom README copy that adds front matter and deletes irrelevant fields for readme doc deployment
    """
    input_readme = "README.md"
    output_file = "README_COPY.md"
    with open(input_readme, "r") as file:
        readme_lines = file.readlines()

    # remove links
    for line in readme_lines:
        if line.startswith(("![Logo]", "![GitHub Tag]", "![PyPI - Downloads]")):
            readme_lines.remove(line)

    # remove irrelavant sections
    readme_content = "".join(readme_lines)
    count = 0
    for section in readme_content.split("\n### "):
        if section.startswith("Parcl Labs Data Overview"):
            full_section = "\n### " + section
            readme_content = readme_content.replace(full_section, "")
        count += 1

    with open(output_file, "w") as f:
        f.write(FRONT_MATTER + readme_content)


if __name__ == "__main__":
    main()
