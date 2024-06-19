import re
import os


def extract_code_blocks(readme_content):
    code_blocks = re.findall(r"```python\n(.*?)```", readme_content, re.DOTALL)
    return code_blocks


def save_code_blocks_to_file(code_blocks, output_file):
    with open(output_file, "w") as f:
        for block in code_blocks:
            f.write(block + "\n\n")


def main():
    input_readme = os.path.join(os.path.dirname(__file__), "..", "README.md")
    output_file = os.path.join(os.path.dirname(__file__), "extracted_readme_code.py")

    with open(input_readme, "r") as file:
        readme_content = file.read()

    code_blocks = extract_code_blocks(readme_content)
    save_code_blocks_to_file(code_blocks, output_file)
    print(f"Extracted code blocks saved to {output_file}")


if __name__ == "__main__":
    main()
