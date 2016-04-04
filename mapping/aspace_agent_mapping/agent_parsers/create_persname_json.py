import re

from nameparser import HumanName


def parse_persname(persname, auth="", source=""):
    name, birth_date, death_date = extract_birth_death_dates(persname)
    birth_date, death_date = validate_dates(birth_date, death_date)
    dates_string = make_date_string(birth_date, death_date)
    name = HumanName(name)

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

    if name.middle and not birth_date and not death_date and not name.middle.endswith('.') and len(name.middle.replace(".","").replace(" ","")) < 2:
        name.middle += "."

    rest_of_name = u"{0} {1}".format(name.first, name.middle).rstrip()
    if rest_of_name == u"Christella D. Personal journey through South Africa. 1991":
        rest_of_name = u"Christella D."

    non_terminating_middle_initials = re.compile(r'\s[\.\s]?[A-Za-z]$')

    if non_terminating_middle_initials.search(rest_of_name) and not rest_of_name.endswith('.'):
        rest_of_name += "."

    # People with single-part names (like Keewaydinoquay) are mis-assigned. Have to fix those
    primary_name = name.last
    if rest_of_name and not primary_name:
        primary_name = rest_of_name
        rest_of_name = ""

    if rest_of_name and not birth_date and not death_date and not name.nickname and not suffix:
        if not rest_of_name.endswith('.'):
            rest_of_name += "."

    # create the parsed name dictionary
    name_parsed = {u"title": unicode(title),
                   u"primary_name": unicode(primary_name),
                   u"rest_of_name": rest_of_name,
                   u"suffix": unicode(suffix),
                   u"fuller_form": unicode(name.nickname),
                   u"numbers": unicode(number),
                   u"birth_date": unicode(birth_date),
                   u"death_date": unicode(death_date),
                   u"date_string": unicode(dates_string),
                   u"authority_id": unicode(auth),
                   u"source": unicode(source),
                   u"name_order": u"inverted",
                   u"sort_name_auto_generate": True}

    # remove empty fields
    for key, value in name_parsed.items():
        if not value:
            del name_parsed[key]

    return name_parsed


def validate_dates(birth_date, death_date):
    if birth_date and death_date:
        if int(birth_date) > int(death_date):
            birth_date = ""
            death_date = ""
    return birth_date, death_date


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
            string = string.replace(" ca.", "").replace(", ,",",").rstrip(" ,")
            birth_date, death_date = dates[0]
            break

    return string, birth_date, death_date