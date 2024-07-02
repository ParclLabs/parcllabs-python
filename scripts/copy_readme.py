import os


def main():
    """
    Copy README.md to README_COPY.md and prepend front matter to the copied file to prepare for readme doc deployment
    """
    input_readme = "README.md"
    output_file = "README_COPY.md"
    os.system(f"cp {input_readme} {output_file}")

    prepend_lines = "---\ntitle: Python Library\nslug: python-library-1\n---\n\n"
    with open(output_file, "r+") as file:
        content = file.read()
        file.seek(0, 0)
        file.write(prepend_lines + content)


if __name__ == "__main__":
    main()
