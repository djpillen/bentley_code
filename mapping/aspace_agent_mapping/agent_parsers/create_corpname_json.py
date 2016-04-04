import re

import nltk.data

keys = [u"primary_name",
        u"subordinate_name_1",
        u"subordinate_name_2",
        u"qualifier",
        u"authority_id",
        u"source",
        u"sort_name_auto_generate",
        u"date_string",
        u"number",
        u"date_start",
        u"date_end"]


def parse_corpname(string, authority_id="", source=""):
    string, qualifier = extract_qualifier(string)

    primary_name, sub_name_1, sub_name_2 = split_into_component_entities(string)

    if not qualifier and len(qualifier.strip()) == 0:
        if sub_name_2 and len(sub_name_2.strip()) > 0 and not sub_name_2.endswith(')') and not sub_name_2.endswith('.'):
            sub_name_2 += '.'
        elif sub_name_1 and len(sub_name_1.strip()) > 0 and not sub_name_2 and not sub_name_1.endswith(')') and not sub_name_1.endswith('.'):
            sub_name_1 += '.'
        elif primary_name and len(primary_name.strip()) > 0 and not sub_name_1 and not sub_name_2 and not primary_name.endswith(')') and not primary_name.endswith('.'):
            primary_name += '.'

    if ' inc ' in primary_name or ' Inc ' in primary_name:
        primary_name = primary_name.replace(' inc ', ' inc. ').replace(' Inc ', ' Inc. ')


    corpname_parsed = {u"primary_name":       unicode(primary_name),
                       u"subordinate_name_1": unicode(sub_name_1),
                       u"subordinate_name_2": unicode(sub_name_2),
                       u"qualifier":          unicode(qualifier.strip(" ()")),
                       u"authority_id":       unicode(authority_id),
                       u"source":             unicode(source),
                       u"date_string":        unicode(""),
                       u"number":             unicode(""),
                       u"date_start":         unicode(""),
                       u"date_end":           unicode(""),
                       u"sort_name_auto_generate": True}

    # remove empty fields
    for key, value in corpname_parsed.items():
        if not value:
            del corpname_parsed[key]

    if corpname_parsed.get(u"primary_name") and not corpname_parsed.get(u"subordinate_name_1") and not corpname_parsed.get(u"subordinate_name_2") and not corpname_parsed.get(u"qualifier"):
        if not corpname_parsed[u"primary_name"].endswith(')') and not corpname_parsed[u"primary_name"].endswith('.'):
            corpname_parsed[u"primary_name"] = corpname_parsed[u"primary_name"] + '.'

    return corpname_parsed


def extract_qualifier(string):
    qualifier_regex = re.compile(r"(\([^\(]*?\))$")  # matches any ending parenthetical statement
    qualifier = ""

    qualifier_match = re.findall(qualifier_regex, string)
    if qualifier_match:
        qualifier = qualifier_match[-1]

    string = string.replace(qualifier, "")

    return string, qualifier


def extract_names_from_split_components(parts):
    # creates primary name and sub names based on a list of split parts
    if len(parts) == 0:
        return "", "", ""
    if len(parts) == 1:
        return parts[0], "", ""
    if len(parts) == 2:
        return parts[0], parts[1], ""
    if len(parts) == 3:
        return parts[0], parts[1], parts[2]
    else:
        return parts[0], parts[1], ". ".join(parts[2:]).replace("..", ".")


def split_into_component_entities(string):
    # uses an nltk sentence detector to split a corpname into its component parts
    # while also not splitting on things like "St." or "Co."

    part_detector = nltk.data.load("tokenizers/punkt/english.pickle")

    # doesn't handle "No." properly, so we handle that manually
    string = re.sub("[Nn]o. ", "$$number$$", string)

    # split
    parts = part_detector.tokenize(string.strip())

    # replace "No."
    parts = [name.replace("$$number$$", "no. ") for name in parts]

    return [name.strip(". ") for name in extract_names_from_split_components(parts)]
