import csv
import json
import re
from tqdm import tqdm


def main():
    # CHANGE THIS
    input_filepath = "C:/Users/djpillen/GitHub/accessions/accessions_20160226-final.csv"

    donornumberid_to_contactid_filepath = 'C:/Users/djpillen/GitHub/accessions/DonorNumberID_to_ContactID.csv'
    donornumberid_to_contactid_map = make_donornumberid_to_contactid_map(donornumberid_to_contactid_filepath)

    contactid_to_aspace_id_filepath = 'C:/Users/djpillen/GitHub/bentley_code/mapping/aspace_agent_mapping/donor_name_to_aspace_id_map.json'

    donornumberid_to_aspace_id_map = make_donornumberid_to_aspace_id_map(donornumberid_to_contactid_map, contactid_to_aspace_id_filepath)

    clean_filename = convert_input_file_to_utf8(input_filepath)

    json_data = make_accession_json_list(clean_filename, donornumberid_to_aspace_id_map)

    with open("json_data.json", mode="w") as f:
        json.dump(json_data, f, encoding='utf-8', indent=4, sort_keys=True)


def make_accession_json_list(filepath, donornumberid_to_aspace_id_map):
    json_for_each_accession = []

    accessions = load_accession_data_from_beal_export(filepath)
    for accession in tqdm(accessions):
        aspace_json = {}

        aspace_json.update(get_accession_title(accession))
        aspace_json.update(create_simple_json_fields(accession))
        aspace_json.update(create_deaccession_json(accession))
        aspace_json.update(create_user_defined_json_entries(accession))
        aspace_json.update(create_extent_json_entries(accession))
        aspace_json.update(create_collection_management_json_entries(accession))
        aspace_json.update(create_access_restriction_json(accession))
        aspace_json.update(create_external_document_json_entries(accession))
        aspace_json.update(create_classifications_json(accession))
        aspace_json.update(create_donor_json(accession, donornumberid_to_aspace_id_map))

        # we may end up wanting to change the following method if/when ASpace re-implements processing status as a drop-down
        aspace_json.update(add_processing_status_to_general_note(accession, aspace_json))

        json_for_each_accession.append(aspace_json)

    return json_for_each_accession

def make_donornumberid_to_aspace_id_map(donornumberid_to_contactid_map, contactid_to_aspace_id_filepath):
    donornumberid_to_aspace_id_map = {}

    with open(contactid_to_aspace_id_filepath, 'r') as f:
        contactid_to_aspace_id_map = json.load(f)

    for donornumberid in donornumberid_to_contactid_map:
        contactid = donornumberid_to_contactid_map[donornumberid]
        if contactid in contactid_to_aspace_id_map:
            donornumberid_to_aspace_id_map[donornumberid] = contactid_to_aspace_id_map[contactid]

    return donornumberid_to_aspace_id_map


def make_donornumberid_to_contactid_map(filepath):
    donornumberid_to_contactid_map = {}

    with open(filepath,'rb') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            donornumberid = row[0]
            contactid = row[1]
            if donornumberid and contactid:
                donornumberid_to_contactid_map[donornumberid] = contactid

    return donornumberid_to_contactid_map


def add_processing_status_to_general_note(accession, aspace_json):
    note = aspace_json.get("general_note", "") or ""
    processing_status = accession.get("ProcessingStatus", "") or ""
    if not processing_status or processing_status == "RCS":
        return {}

    if note:
        note += "; Processing Status: {}".format(processing_status)
    else:
        note = "Processing Status: {}".format(processing_status)

    return {"general_note": note}

def create_donor_json(accession, donornumberid_to_aspace_id_map):
    aspace_json = {}
    agent_links = []
    donornumberid = accession.get("DonorNumberID","")
    if donornumberid:
        aspace_id = donornumberid_to_aspace_id_map.get(donornumberid,"")
        if aspace_id:
            agent_links.append({"ref":aspace_id,'role':'source'})

    if agent_links:
        aspace_json["linked_agents"] = agent_links

    return aspace_json

def create_classifications_json(accession):
    aspace_json = {}
    classifications = []
    processing_status = accession.get("ProcessingStatus", "") or ""
    if processing_status == "RCS":
        classifications.append({"ref": "/repositories/2/classifications/4"})

    classification_type = normalize_unit(accession["Unit"])

    if classification_type == "mhc":
        classifications.append({"ref": "/repositories/2/classifications/1"})
    if classification_type == "uarp":
        classifications.append({"ref": "/repositories/2/classifications/2"})

    if classifications:
        aspace_json["classifications"] = classifications

    return aspace_json


def normalize_unit(unit):
    if not unit:
        return ""
    if "mhc" in unit.lower():
        return "mhc"
    if "ua" in unit.lower():
        return "uarp"
    else:
        return ""


def create_external_document_json_entries(accession):
    aspace_json = {}
    external_documents_fields = ["FileLink"]
    external_documents = []
    for doc_field in external_documents_fields:
        if accession.get(doc_field, ""):
            external_documents.append(make_external_document_json(accession.get(doc_field), doc_type=doc_field))

    if external_documents:
        aspace_json["external_documents"] = external_documents

    return aspace_json


def create_access_restriction_json(accession):
    aspace_json = {}
    access_restriction_fields = ["RestrictionsType", "Note"]
    if any([accession_has_value_for_key(accession, field) for field in access_restriction_fields]):
        aspace_json["access_restrictions_note"] = "Restriction type: {}\nRestriction note: {}".format(
            accession.get("RestrictionsType", ""), accession.get("Notes", ""))

    return aspace_json


def create_collection_management_json_entries(accession):
    aspace_json = {}
    collection_management_mappings = {"Difficulty": "processing_plan",
                                      "PercentageToRetain": "processing_plan",
                                      "ProcessNote": "processing_plan",
                                      "PriorityLevel": "processing_priority",
                                      "Processor": "processors"}

    if accession_has_any_collection_management_data(accession, collection_management_mappings):
        aspace_json["collection_management"] = make_collection_management_json(accession, collection_management_mappings)

    return aspace_json


def accession_has_any_collection_management_data(accession, collection_management_mappings):
    return any([accession_has_value_for_key(accession, accession_key) for accession_key in collection_management_mappings.keys()])


def accession_has_value_for_key(accession, accession_key):
    value = accession.get(accession_key, "")
    if not value:
        return False

    return len(value.strip()) > 0


def create_extent_json_entries(accession):
    aspace_json = {}
    extent_json_entries = []
    if is_digital_extent(accession):
        extent_type = accession.get("DigitalSizeType", "")

        if extent_type in ["MB", "GB", "KB", "TB"]:
            extent_json_entries.append(make_extent_json(number=accession.get("DigitalSize", ""), type_=accession.get("DigitalSizeType", "(MB?)")))

    if accession_has_value_for_key(accession, "LinearFeet"):
        extent_json_entries.append(make_extent_json(number=accession.get("LinearFeet", "")))

    aspace_json["extents"] = extent_json_entries
    return aspace_json


def is_digital_extent(accession):
    return accession_has_value_for_key(accession, "DigitalSize")


def create_user_defined_json_entries(accession):
    aspace_json = {}
    user_defined_field_mappings = {"Acknowledged": "boolean_1",
                                   "StaffReceived": "string_1",
                                   "ThankYouNote": "text_1",
                                   "GiftAgreementStatus": "enum_1",
                                   "LocationInfo": "text_2"}


    user_defined_data = make_user_defined_json(accession, user_defined_field_mappings)
    if user_defined_data:
        aspace_json["user_defined"] = user_defined_data

    return aspace_json


def create_deaccession_json(accession):
    aspace_json = {}
    deaccessions = []
    if is_disposition(accession):
        aspace_json["disposition"] = make_disposition_text(accession)
    else:
        deaccessions.append(make_deaccession_json(accession))
    # hack of a fix to get rid of empty dispositions
    if "disposition" in aspace_json and not aspace_json.get("disposition", ""):
        del aspace_json["disposition"]
    if len(deaccessions) > 0:
        aspace_json["deaccessions"] = deaccessions

    return aspace_json


def get_accession_title(accession):
    aspace_json = {}
    if accession.get('AccDescription', "").strip():
        aspace_json["title"] = accession.get('AccDescription', "").strip()
    return aspace_json


def create_simple_json_fields(accession):
    simple_field_mappings = {'AccDescription': 'content_description',
                             'AccessionDate': 'accession_date',
                             'AccessionID': 'id_0',
                             'DonorBoxList': 'inventory',
                             'GivenThrough': 'provenance',
                             "DonorType": "acquisition_type",
                             "MHCType": "resource_type",
                             "Notes": "use_restrictions_note",
                             "Note": "general_note"}

    aspace_json = {}
    for accession_key, aspace_key in simple_field_mappings.items():
        text = get_normalized_accession_text(accession, accession_key)
        if text:
            aspace_json[aspace_key] = text

    return aspace_json


def get_normalized_accession_text(accession, accession_key):
    text = ""
    if accession.get(accession_key, ""):
        text = accession.get(accession_key, "").strip()
    text = normalize_accession_text(accession_key, text)
    return text


def normalize_accession_text(accession_key, text):
    if accession_key == "AccessionDate":
        text = normalize_date_text(text)

    elif accession_key == "DonorType":
        text = normalize_donor_type(text)

    elif accession_key == "MHCType":
        text = normalize_mhc_type(text)

    return text


def normalize_mhc_type(text):
    if "published" in text.lower():
        text = "publications"
    else:
        text = "papers"
    return text


def normalize_donor_type(text):
    if "purchase" in text.lower():
        text = "purchase"
    else:
        text = "deposit"
    return text


def load_accession_data_from_beal_export(filepath):
    with open(filepath, mode="r") as f:
        reader = UnicodeDictReader(f)
        accessions = [row for row in reader]

    return accessions

def UnicodeDictReader(utf8_data, **kwargs):
    csv_reader = csv.DictReader(utf8_data, **kwargs)
    for row in csv_reader:
        yield {key: unicode(value, 'utf-8') for key, value in row.iteritems()}


def is_disposition(accession):
    return not accession.get("DestructionDate", "") and not accession.get("ReturnDate", "")


def normalize_date_text(text):
    regex = re.compile(r"/|\.|\-")
    date_components = re.split(regex, text)
    try:
        date_components = [int(part) for part in date_components]
        if len(date_components) == 2:
            month, year = date_components
            day = 1
        elif len(date_components) == 3:
            month, day, year = date_components
        elif len(date_components) == 1:
            year = date_components[0]
            month = 1
            day = 1
        else:
            year, month, day = (1800, 1, 1)
    except ValueError:
        year, month, day = (1800, 1, 1)

    return "{:04d}-{:02d}-{:02d}".format(year, month, day)


def make_disposition_text(accession):
    description = accession.get("Description", "")
    location = accession.get("Location", "")
    type_ = accession.get("SeparationsType", "")
    volume = accession.get("Volume", "")

    text = ""
    if description:
        text += "Description: {}\n".format(description)
    if location:
        text += "Location: {}\n".format(location)
    if type_:
        text += "Type: {}\n".format(type_)
    if volume:
        text += "Volume: {}\n".format(type_)

    return text.strip()


def make_collection_management_json(accession, fields):
    aspace_json = {}

    processing_plan_string = ""
    for accession_key in [key for key, value in fields.items() if value == "processing_plan"]:
        if accession.get(accession_key, ""):
            processing_plan_string += "{}: {}\n".format(accession_key, accession[accession_key])

    if processing_plan_string:
        aspace_json["processing_plan"] = processing_plan_string

    for accession_key, aspace_key in [(key, value) for key, value in fields.items() if value != "processing_plan"]:
        if accession_has_value_for_key(accession, accession_key):
            if aspace_key == "processing_priority":
                value = accession[accession_key].lower()
                if value in ["high", "low", "medium"]:
                    aspace_json[aspace_key] = value
            else:
                aspace_json[aspace_key] = accession[accession_key]

    return aspace_json


def make_deaccession_json(accession):
    aspace_json = {"extents": []}

    destruction_date = accession.get("DestructionDate", "")
    return_date = accession.get("ReturnDate", "")
    volume = accession.get("Volume", "")

    aspace_json["description"] = accession.get("Description", "Deaccession not described")
    if not aspace_json["description"]:
        aspace_json["description"] = "Deaccession not described"

    aspace_json["scope"] = "part"

    # TODO deaccession objects can only take one date, BUT some accession records have both returned and destroyed dates.
    # do something about that.
    if destruction_date.strip():
        aspace_json["date"] = make_date_json(
            type_="single",
            label="deaccession",
            expression="Material destroyed on " + destruction_date,
            begin_date=normalize_date_text(destruction_date)
        )

    if return_date.strip():
        aspace_json["date"] = make_date_json(
            type_="single",
            label="deaccession",
            expression="Returned to donor on " + return_date,
            begin_date=normalize_date_text(return_date)
        )


    if volume and volume.strip():
        aspace_json["extents"].append(make_extent_json(number=volume))

    return aspace_json


def make_date_json(type_, label, expression="", begin_date="", end_date=""):
    return {"date_type": type_, "label": label, 'expression': expression, "begin": begin_date, "end": end_date}


def make_external_document_json(text, doc_type='File Link'):
    return {'location': text, 'title': doc_type}


def make_extent_json(number, type_='linear feet', portion='whole'):
    return {'number': number, 'extent_type': type_, 'portion': portion}


def make_user_defined_json(accession, field_mappings):
    aspace_json = {}
    for accession_key, aspace_key in field_mappings.items():
        text = ""
        if accession.get(accession_key, ""):
            text = accession.get(accession_key, "").strip()

        if aspace_key == "enum_1":
            text = normalize_gift_agreement_status(text)

        if text:
            text = text.replace(";;;", "\n")
            if aspace_key.startswith("boolean"):
                text = coerce_to_boolean(text)

            aspace_json[aspace_key] = text

    return aspace_json


def normalize_gift_agreement_status(text):
    valid_values = ["on file", "pending", "sent", "n/a", "other"]
    text = text.lower()
    if "file" in text.lower() and "on" in text.lower():
        text = "on file"
    if "to be sent" in text.lower():
        text = "pending"

    if text not in valid_values:
        if text:
            print(text)
        text = ""

    return text


def coerce_to_boolean(text):
    try:
        return bool(int(text))
    except:
        return False


def convert_input_file_to_utf8(filename):
    with open(filename, mode="rb") as f:
        data = f.read()

    data = data.decode("latin-1")
    data = data.encode("utf-8")

    name, extension = filename.split(".")
    with open("{}_clean.{}".format(name, extension), mode="wb") as f:
        f.write(data)

    return "{}_clean.{}".format(name, extension)

if __name__ == "__main__":
    main()

