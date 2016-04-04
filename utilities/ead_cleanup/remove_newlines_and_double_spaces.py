import os
import re

from tqdm import tqdm

input_directory = r'C:\Users\djpillen\GitHub\vandura\Real_Masters_all'
output_directory = r'C:\Users\djpillen\GitHub\vandura\Real_Masters_all'


# The names of all EADs to which you'd like to apply this script:
#eads = []

# comment the above and uncomment the below if you instead want apply this script to every EAD in the input directory
eads = [ead for ead in os.listdir(input_directory) if ead.endswith(".xml")]


def fix_whitespace(input_dir, output_dir):
    whitespace_regex = r"\s{2,}|\v|\n|\r"
    for ead in tqdm(eads):
        with open(os.path.join(input_dir, ead), mode="r") as f:
            data = f.read()

        data = " ".join(re.split(whitespace_regex, data))

        with open(os.path.join(output_dir, ead), mode="w") as f:
            f.write(data)


if __name__ == "__main__":
    fix_whitespace(input_dir=input_directory, output_dir=input_directory)
