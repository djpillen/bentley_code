
def parse_famname(string, auth_id="", auth_source=""):
    auth_source = auth_source if auth_source else u"local"

    if not string.endswith(')') and not string.endswith('.'):
        string += "."

    fam_dict = {u"family_name": unicode(string),
                u"prefix": u"",
                u"sort_name_auto_generate": True,
                u"authority_id": auth_id,
                u"source": auth_source}

    # remove empty fields
    for key, value in fam_dict.items():
        if not value:
            del fam_dict[key]

    return fam_dict