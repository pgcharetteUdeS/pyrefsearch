"""ansi_to_html_converter.py"""

from ansi2html import Ansi2HTMLConverter
import sys


def convert_ansi_to_html(input_file, output_file):
    conv = Ansi2HTMLConverter(font_size="large")
    ansi_text = conv.convert(input_file)
    with open(input_file, "r", encoding="ISO-8859-1") as f_in:
        ansi_text = f_in.read()
    html_output = conv.convert(ansi_text)
    with open(output_file, "w", encoding="utf-8") as f_out:
        f_out.write(html_output)


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print(
            "Usage: python ansi_to_html_converter.py <input_file.txt> <output_file.html>"
        )
    else:
        convert_ansi_to_html(sys.argv[1], sys.argv[2])
