"""strip_ANSI_codes_from_file.py"""

import re
import sys


def strip_ansi_codes(text):
    """
    Removes ANSI escape codes from a given string.
    """
    ansi_escape = re.compile(r"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])")
    return ansi_escape.sub("", text)


def strip_ansi_codes_from_file(input_filepath, output_filepath):
    """
    Reads a file, strips ANSI codes, and writes the clean text to a new file.
    """
    try:
        with open(input_filepath, "r", encoding="ISO-8859-1") as infile:
            content = infile.read()

        cleaned_content = strip_ansi_codes(content)

        with open(output_filepath, "w", encoding="utf-8") as outfile:
            outfile.write(cleaned_content)
    except FileNotFoundError:
        print(f"Error: Input file '{input_filepath}' not found.")
    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print(
            "Usage: python strip_ANSI_codes_from_file.py <input_file.txt> <output_file.html>"
        )
    else:
        strip_ansi_codes_from_file(sys.argv[1], sys.argv[2])
