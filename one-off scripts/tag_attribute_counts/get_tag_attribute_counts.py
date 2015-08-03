import os
import csv
from pprint import pprint

from lxml import etree
from tqdm import tqdm


def write_attribute_value_counts(ead_input_dir, tag_name, attribute_name):
    '''
    Reads all eads in the input directory to create counts of each unique attribute value
    found for the given attribute in the given tag type

    Use "c0x" as the tag type if you want the attributes of every type of c0-level tag.
    '''

    # create list of valid files to run through
    files = [ead for ead in os.listdir(ead_input_dir) if ead.endswith(".xml")]

    # init values dictionary
    values = {}

    # iterate through all valid eads
    for ead in tqdm(files):
        tree = etree.parse(os.path.join(ead_input_dir, ead))

        ## extract all relevant tags from the tree
        # special case for the "c0x" tag value
        if tag_name == "c0x":
            elements_list = [tree.xpath("//c0{}".format(i)) for i in range(1, 10)]
            elements = []
            for element in elements_list:
                elements += list(element)

        # normal case
        else:
            elements = tree.xpath("//{}".format(tag_name))

        # add attribute value to the values dictionary, and increment its count
        for element in elements:
            value = element.attrib.get(attribute_name, "")
            values[value] = values.get(value, 0) + 1

    # write results to file (filename based off of tag and attribute names)
    with open("{0}_{1}_counts.csv".format(tag_name, attribute_name), mode="wb") as f:
        writer = csv.writer(f)
        header = ["attribute value", "count"]
        value_rows = sorted([[attribute, count] for attribute, count in values.items()])

        writer.writerow(header)
        writer.writerows(value_rows)

    pprint(values)

if __name__ == "__main__":
    # change to your input directory
    input_dir = r'C:\Users\wboyle\PycharmProjects\vandura\Real_Masters_all'

    # change to the tag/attribute combo you're looking to characterize
    tag_name = "container"
    attribute_name = "type"

    write_attribute_value_counts(input_dir, tag_name=tag_name, attribute_name=attribute_name)
