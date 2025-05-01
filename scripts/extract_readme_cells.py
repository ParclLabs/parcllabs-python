import re
from pathlib import Path


def extract_code_blocks(readme_content: str) -> list[str]:
    return re.findall(r"```python\n(.*?)```", readme_content, re.DOTALL)


def save_code_blocks_to_file(code_blocks: list[str], output_file: Path) -> None:
    with Path(output_file).open("w") as f:
        for block in code_blocks:
            f.write(block + "\n\n")


def main() -> None:
    input_readme = Path(__file__).parent.parent / "README.md"
    output_file = Path(__file__).parent / "extracted_readme_code.py"

    with Path(input_readme).open() as file:
        readme_content = file.read()

    code_blocks = extract_code_blocks(readme_content)
    save_code_blocks_to_file(code_blocks, output_file)


if __name__ == "__main__":
    main()
