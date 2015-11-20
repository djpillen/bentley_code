import os
import re
import csv
import json

from nameparser import HumanName
from lxml import etree
from tqdm import tqdm
from utilities.utilities import dump_json


def grab_persnames(input_dir):
    eads = [ead for ead in os.listdir(input_dir) if ead.endswith(".xml")]
    persnames_dict = {}

    for ead in tqdm(eads):
        try:
            tree = etree.parse(os.path.join(input_dir, ead))
        except etree.XMLSyntaxError as e:
            print("Error in {0}: {1}".format(ead, e))
            continue

        persnames = tree.xpath("//controlaccess/persname") + tree.xpath("//origination/persname")
        for persname in persnames:
            auth = persname.attrib.get("authfilenumber", "")
            source = persname.attrib.get("source")
            attribs = [auth, source]
            name = persname.text.encode("utf-8")
            if name in persnames_dict:
                if auth and not persnames_dict[name]:
                    persnames_dict[name] = attribs
            else:
                persnames_dict[name] = attribs

    return persnames_dict


def parse_persname(persname, source, auth):
    name = persname.replace("---", "- --")
    name = name.split("--")[0]
    name, birth_date, death_date = extract_birth_death_dates(name)
    dates_string = make_date_string(birth_date, death_date)
    name = HumanName(name.decode("utf-8"))

    titles = ["sir", "mr", "mrs", "baron", "dame", "madame", "viscount", "conte"]
    numbers = ["II", "III"]
    title = name.title
    suffix = name.suffix
    number = u""

    # check if the suffix should actually be a title
    if not title and any(suffix.lower().strip(". ") == title for title in titles):
        title = suffix.capitalize()
        if "mr" in title.lower() and not title.endswith("."):
            title += "."
        suffix = u""

    # extract numbers from the suffix
    if suffix in numbers:
        number = suffix
        suffix = u""

    # special cases cleanup
    if name.title == u"Royal":
        name.title = ""
        title = ""
        name.middle = name.first if not name.middle else "{} {}".format(u"Royal", name.middle)
        name.first = u"Royal"

    if name.title == u"Queen of Great":
        title = name.title + u" Britain"
        name.first = u""

    if name.title == u"Lama":
        title = u"Dalai Lama XIV"
        name.first = u""
        name.middle = u""

    if name.title == u"Marquis":
        title = u""
        name.first = u"Marquis"
        name.middle = u"W."

    if suffix == u"1941":
        birth_date = suffix
        suffix = u""

    if suffix in [u"18", u"b."]:
        suffix = u""

    if suffix == u"Jr":
        suffix += u"."

    if ", fl. 17th cent" in suffix:
        suffix = u"sieur de"
        dates_string = u"fl. 17th cent"

    rest_of_name = u"{0} {1}".format(name.first, name.middle).rstrip()
    if rest_of_name == u"Christella D. Personal journey through South Africa. 1991":
        rest_of_name = u"Christella D."

    # create the parsed name dictionary
    name_parsed = {u"title": unicode(title),
                   u"primary_name": unicode(name.last),
                   u"rest_of_name": rest_of_name,
                   u"suffix": unicode(suffix),
                   u"fuller_form": unicode(name.nickname),
                   u"numbers": unicode(number),
                   u"birth_date": unicode(birth_date),
                   u"death_date": unicode(death_date),
                   u"dates": unicode(dates_string),
                   u"auth": unicode(auth),
                   u"source": unicode(source),
                   }

    return name_parsed


def make_date_string(birth, death):
    if birth and death:
        return u"{}-{}".format(birth, death)
    if birth:
        return u"b. {}".format(birth)
    if death:
        return u"d. {}".format(death)
    return u""


def extract_birth_death_dates(string):
    alt_date_regex = r"(\d{4}) or \d{2}"
    date_regex = r"(\d{4})\??\-(?:ca\.)?((?:\d{4})?)\??"
    birth_letter_regex = r"b\. ?(\d{4})()"
    death_letter_regex = r"d\. ?()(\d{4})"
    circa_regex_1 = r"(\d{4}) \(ca\.\)-(\d{4})"
    birth_date = ""
    death_date = ""

    string = re.sub(alt_date_regex, "\g<1>", string)
    string = string.rstrip(".")

    for regex in [date_regex, birth_letter_regex, death_letter_regex, circa_regex_1]:
        dates = re.findall(regex, string)

        if len(dates) == 1:
            string = re.sub(regex, "", string)
            string = string.replace(" ca.", "").rstrip(" ,")
            birth_date, death_date = dates[0]
            break

    return string, birth_date, death_date


if __name__ == "__main__":
    input_dir = r'C:\Users\wboyle\PycharmProjects\vandura\Real_Masters_all'

    # retrieve all persnames from eads
    persnames = grab_persnames(input_dir)

    # # serialize results to a csv
    # with open("persnames.csv", mode="wb") as f:
    #     writer = csv.writer(f)
    #     data = sorted([[name, attrib[1], attrib[0]] for name, attrib in persnames.items()])
    #     writer.writerows(data)

    # parse all names for aspace export
    output = []
    output_dict = {}
    for name, attributes in persnames.items():
        auth, source = attributes
        n = parse_persname(name, source, auth)
        output.append([name,
                       n.get("title", "").encode("utf-8"),
                       n.get("primary_name", "").encode("utf-8"),
                       n.get("rest_of_name", "").encode("utf-8"),
                       n.get("suffix", "").encode("utf-8"),
                       n.get("fuller_form", "").encode("utf-8"),
                       n.get("numbers", ""),
                       n.get("dates", ""),
                       n.get("birth_date", ""),
                       n.get("death_date", ""),
                       n.get("auth", ""),
                       n.get("source", "")
                       ])
        output_dict[name.decode("utf-8")] = n

    # write results to a csv file
    with open("parsed_persnames.csv", mode="wb") as f:
        headers = ["original name", "title", "primary_name", "rest_of_name", "suffix", "fuller_form", "numbers", "dates", "birth date",
                   "death date", "auth link", "source"]
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(output)

    # write results to a json file
    dump_json("persname_output.json", output_dict)
