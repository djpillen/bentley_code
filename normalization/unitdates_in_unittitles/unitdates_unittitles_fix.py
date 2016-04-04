from vandura.config import ead_dir

import csv
import os
import re
from copy import deepcopy

from lxml import etree
from tqdm import tqdm


input_dir = os.path.join(ead_dir, "eads")
output_dir = os.path.join(ead_dir,"eads")


def grab_suspects(input_dir):
    eads = [ead for ead in os.listdir(input_dir) if ead.endswith(".xml")]
    tag_regex = r"\<\/?.*?\>"
    clean_total = 0
    calcify_total = 0
    data = []
    for ead in tqdm(eads):
        tree = etree.parse(os.path.join(input_dir, ead))
        unittitles = tree.xpath("//did/unittitle")
        for unittitle in unittitles:
            action, clean, calcify = determine_action(unittitle)
            clean_total += clean
            calcify_total += calcify
            if action:
                text_with_tags = " ".join(etree.tostring(unittitle).split()).strip()
                text_without_tags = " ".join(re.sub(tag_regex, "", text_with_tags).split()).strip()
                data.append([ead, tree.getpath(unittitle), text_with_tags, text_without_tags, action])

    with open('wonky_unitdate_display_candidates.csv', mode="wb") as f:
        writer = csv.writer(f)
        writer.writerows(data)
        print(len(data))

    print "Clean: {0}".format(clean_total)
    print "Calcify: {0}".format(calcify_total)


def fix_suspects(input_dir, output_dir):
    skipped_items = []
    count = 0

    with open("wonky_unitdate_display_candidates.csv", mode="r") as f:
        example_dict = {}
        reader = csv.reader(f)
        items = list(reader)
        items.reverse()  # reverse the input list so that xpaths remain valid as I edit multiple entries in one ead

    for filename, xpath, text_with_tags, text_without_tags, action in tqdm(items):
        example_dict[filename] = example_dict.get(filename, [])
        example_dict[filename].append((xpath, action, text_without_tags))

    for ead, dict_value_list in tqdm(example_dict.items()):
        tree = etree.parse(os.path.join(input_dir, ead))

        for xpath, action, text in dict_value_list:
            unittitle = tree.xpath(xpath)[0]
            
            #disparity = find_date_disparity(unittitle)
            #if disparity > 10 and action == "move_and_calcify" and ead != "geolsurv.xml":
                #skipped_items.append([ead, xpath, text, disparity, action])
            #else:
                #move_unitdates(unittitle, action)
            move_unitdates(unittitle, action)

        with open(os.path.join(output_dir, ead), mode="w") as f:
            f.write(etree.tostring(tree, pretty_print=True, xml_declaration=True, encoding="utf-8"))

    #with open("skipped_items.csv", mode="wb") as f:
        #writer = csv.writer(f)
        #writer.writerows(skipped_items)

    print("Skipped {0} entries".format(len(skipped_items)))


def determine_action(unittitle):
    # characterize the distribution of unitdates in the unittitle, and decide on which course of action to take
    unitdates = unittitle.xpath("unitdate")
    action = ""
    clean = 0
    calcify = 0
    if unitdates:
        for unitdate in unitdates:
            if unitdate.tail:
                if len(unitdate.tail.strip(", 1234567890-")) > 5 and len(unitdates) > 1:
                    action = "move_and_calcify"
                    calcify += 1
                elif any([unitdate.tail.strip() == candidate for candidate in [",", "and", ", and"]]):
                    tails = [unitdate.tail if unitdate.tail else "" for unitdate in unitdates]
                    if all([len(tail.strip(" and,.")) == 0 for tail in tails]):
                        action = "move_and_clean"
                        clean += 1
    if unitdates and not action:
        action = "move_and_clean"
        clean += 1

    return action, clean, calcify


def find_date_disparity(unittitle):
    unitdates = unittitle.xpath("unitdate")

    dates = [unitdate.text for unitdate in unitdates if unitdate.text]
    processed_dates = []
    for date in dates:
        years = re.findall(r"\d{4}", date)
        if len(years) == 2:
            processed_dates.append("{}-{}".format(years[0], years[1]))
        elif len(years) == 1:
            processed_dates.append(years[0])

    single_dates = [int(date) for date in processed_dates if "-" not in date]
    ranges = [date for date in processed_dates if "-" in date]

    # if there are only date ranges, assume it's fine
    if len(single_dates) == 0:
        return 0

    # if there are only single dates, do some simple math on them
    elif len(ranges) == 0:
        single_dates.sort()
        max_dif = 0
        for i, date in enumerate(single_dates):
            if i != len(single_dates) - 1:
                dif = single_dates[i + 1] - date
                max_dif = dif if dif > max_dif else max_dif

        return max_dif

    # otherwise compare each single date against the highest and lowest values among all the ranges
    else:
        int_dates = [int(date) for date in single_dates]

        range_dates = []
        for range in ranges:
            date1, date2 = range.split("-")
            range_dates += [int(date1), int(date2)]

        max_range = max(range_dates)
        min_range = min(range_dates)

        max_dif = 0
        for date in int_dates:
            if date > max_range:
                if date - max_range > max_dif:
                    max_dif = date - max_range
            elif date < min_range:
                if min_range - date > max_dif:
                    max_dif = min_range - date

        return max_dif


def move_unitdates(unittitle_node, action):
    parent = unittitle_node.getparent()
    unitdates = unittitle_node.xpath("unitdate")

    tag_regex = r"<unitdate.*?>(.*?)<\/unitdate>"

    # copy the original unitdates for later use
    copies = []
    for unitdate in unitdates:
        copies.append(deepcopy(unitdate))

    # if the action is to "move and calcify", leave the text but copy the tags
    if action == "move_and_calcify":
        new_unittitle = etree.fromstring(re.sub(tag_regex, '\g<1>', etree.tostring(unittitle_node)))
        new_unittitle.attrib["altrender"] = "calcified"

    # otherwise the unitdates are just at the end of the tag -- remove them entirely and append after unittitle
    # also need to clean the remainder. This is tricky due to weirdness in lxml's ".text" function
    else:
        new_unittitle = etree.fromstring(re.sub(tag_regex, '', etree.tostring(unittitle_node)))
        if len(list(new_unittitle)) > 0:
            last_element = list(new_unittitle)[-1]
            last_element.tail = clean_text(last_element.tail)
        else:
            new_unittitle.text = clean_text(new_unittitle.text)

    parent.insert(parent.index(unittitle_node), new_unittitle)
    parent.remove(unittitle_node)

    # remove duplicate unitdates
    dates = set()
    for copy in copies:
        if copy.text in dates:
            copies.remove(copy)
        dates.add(copy.text)

    # clean the tails of the copied unitdates
    for i, copy in enumerate(copies):
        copy.tail = ""
        parent.insert(parent.index(new_unittitle) + 1 + i, copy)  # adding i to ensure original order is preserved

    # if the new unittitle is empty, remove it entirely
    if not new_unittitle.text and len(list(new_unittitle)) == 0:
        parent.remove(new_unittitle)

    return parent


def clean_text(text):
    try:
        text = text.rstrip(" ,\n")
        if text.endswith("and"):
            text = text[:-4].rstrip(" ,")
        return text
    except AttributeError:
        return text


if __name__ == "__main__":
    grab_suspects(input_dir)
    fix_suspects(input_dir, output_dir)
