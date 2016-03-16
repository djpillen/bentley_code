import json
import os
import shutil

from lxml import etree
import re
from tqdm import tqdm

from utilities.ead_cleanup.prettifydirectory import prettify_xml


class EAD(object):

    def __init__(self, filepath):
        self.path_to_ead = filepath
        self.filename = os.path.basename(self.path_to_ead)
        self.tree = etree.parse(self.path_to_ead)

        try:
            self.id = self.extract_eadid(self.tree.xpath("//eadid")[0].text)
        except IndexError as e:
            self.id = "[no id found]"

        try:
            self.title = self.tree.xpath("//titleproper")[0].text.replace("Finding Aid for ", "")
        except IndexError as e:
            self.title = "[no collection title found]"

    def prettyprint(self, output_dir):
        if not os.path.exists("tmp"):
            os.makedirs("tmp")

        with open(os.path.join("tmp", self.filename), mode="w") as f:
            f.write(etree.tostring(self.tree, xml_declaration=True, encoding="utf-8"))

        text = prettify_xml(self.filename, input_dir="tmp", output_dir=output_dir)

        with open(os.path.join(output_dir, self.filename), mode="w") as f:
            f.write(text)

        shutil.rmtree("tmp")

    def extract_eadid(self, text):
        id_regex = re.compile(r"\d.*")
        return re.findall(id_regex, text)[0]


class EADDir(object):
    def __init__(self, input_dir=r'C:\Users\wboyle\PycharmProjects\vandura\Real_Masters_all'):
        self.input_dir = input_dir
        self.ead_files = [ead for ead in os.listdir(input_dir) if ead.endswith(".xml")]

    def apply_function_to_dir(self, function, output_dir, var1=""):
        for ead_file in tqdm(self.ead_files):
            ead = EAD(os.path.join(self.input_dir, ead_file))
            if var1:
                function(ead, var1)
            else:
                function(ead)

            with open(os.path.join(output_dir, ead_file),'w') as f:
                f.write(etree.tostring(ead.tree, encoding='utf-8',xml_declaration=True, pretty_print=True))

    def characterize_dir(self, function):
        results = []
        for ead_file in tqdm(self.ead_files):
            ead = EAD(os.path.join(self.input_dir, ead_file))
            result = function(ead)
            results.append(result)

        return results


def dump_json(filename, data, filepath=""):
    if filepath:
        filename = os.path.join(filepath, filename)

    with open(filename, mode="w") as f:
        text = json.dumps(data, sort_keys=True, indent=4, ensure_ascii=False, encoding="utf-8")
        f.write(text.encode("utf-8"))
