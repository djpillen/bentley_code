import csv
import json
from pprint import pprint
import nameparser
import pickle
from tqdm import tqdm
from mapping.aspace_agent_mapping.agent_parsers.Corpname import Corpname
from mapping.aspace_agent_mapping.agent_parsers.Persname import Persname
from mapping.aspace_agent_mapping.scripts.post_agents import post_donors_and_record_ids
from mapping.aspace_agent_mapping.scripts.post_agents import return_posted_agent
from mapping.aspace_agent_mapping.scripts.post_agents import update_posted_agent


def main():
    print("loading data...")
    convert_to_utf8_and_add_headers("C:/Users/djpillen/GitHub/accessions/donor_records.tab")
    donor_data = load_donor_data("C:/Users/djpillen/GitHub/accessions/donor_records_clean.tab")

    ead_agents_to_aspace_ids_file = 'local_to_aspace_agent_name_map.p'
    ead_agents_to_aspace_ids = pickle.load(open(ead_agents_to_aspace_ids_file))

    print("extracting donors...")
    person_donor_data, corp_donor_data = extract_agents(donor_data)

    agent_dict = {"persname": {}, "corpname": {}}

    updated_agents_dict = {}

    posted_donors_dict = {}

    for person in tqdm(person_donor_data, desc="creating person json data"):
        parsed_name = make_person_name(person)
        name = unicode(parsed_name)
        if not name or not name.strip():
            continue
        full_name = u"{0}, {1} {2}".format(parsed_name.last, parsed_name.first, parsed_name.middle).rstrip()
        if full_name in ead_agents_to_aspace_ids or full_name+'.' in ead_agents_to_aspace_ids:
            try:
                agent_uri = ead_agents_to_aspace_ids[full_name]
            except:
                agent_uri = ead_agents_to_aspace_ids[full_name+"."]
            print agent_uri
            person_json = return_posted_agent(agent_uri, host="http://localhost:8089", username="admin", password="admin")
            donor_detail = get_donor_detail(person)
            beal_contact_id = donor_detail[u"beal_contact_id"]
            person_json["donor_details"].append(donor_detail)
            person_json_data = json.dumps(person_json)
            update_posted_agent(agent_uri, person_json, host="http://localhost:8089", username="admin", password="admin")
            updated_agents_dict[beal_contact_id] = agent_uri
        elif name in agent_dict["persname"]:
            person_json = json.loads(agent_dict["persname"][name])
            donor_detail = get_donor_detail(person)
            person_json["donor_details"].append(donor_detail)
            agent_dict["persname"][name] = json.dumps(person_json)
        else:
            person_json = json.loads(Persname(name, "", "local").get_aspace_json())
            person_json.update(get_donor_details(person))
            agent_dict["persname"][name] = json.dumps(person_json)

    for corp in tqdm(corp_donor_data, desc="creating corp json data"):
        name = make_corporation_name(corp)
        if not name or not name.strip():
            continue
        if name in ead_agents_to_aspace_ids or name+"." in ead_agents_to_aspace_ids:
            try:
                agent_uri = ead_agents_to_aspace_ids[name]
            except:
                agent_uri = ead_agents_to_aspace_ids[name+"."]
            corp_json = return_posted_agent(agent_uri, host="http://localhost:8089", username="admin", password="admin")
            donor_detail = get_donor_detail(corp)
            beal_contact_id = donor_detail[u"beal_contact_id"]
            pprint(corp_json)
            corp_json["donor_details"].append(donor_detail)
            corp_json_data = json.dumps(corp_json)
            update_posted_agent(agent_uri, corp_json, host="http://localhost:8089", username="admin", password="admin")
            updated_agents_dict[beal_contact_id] = agent_uri
        elif name in agent_dict["corpname"]:
            corp_json = json.loads(agent_dict["corpname"][name])
            donor_detail = get_donor_detail(corp)
            corp_json["donor_details"].append(donor_detail)
            agent_dict["corpname"][name] = json.dumps(corp_json)
        else:
            corp_json = json.loads(Corpname(name, "", "local").get_aspace_json())
            corp_json.update(get_donor_details(corp))
            agent_dict["corpname"][name] = json.dumps(corp_json)

    ids = post_donors_and_record_ids(agent_dict, host="http://localhost:8089", username="admin", password="admin")

    for contact_id in updated_agents_dict:
        ids[contact_id] = updated_agents_dict[contact_id]

    with open("donor_name_to_aspace_id_map.json", mode="w") as f:
        json.dump(ids, f, ensure_ascii=False, indent=4, sort_keys=True)

def get_donor_details(donor_data):
    donor_number = donor_data.get("donor number", "")
    donor_part = donor_data.get("donor part", "")
    contact_id = donor_data.get("contact id", "")
    dart_id = donor_data.get("bhl dart id", "")

    #if donor_part and donor_number:
        #donor_number += "-{}".format(donor_part)

    return {u"donor_details": [{u"donor_number": donor_number,
                                u"donor_number_auto_generate": False,
                               u"dart_id": dart_id,
                               u"beal_contact_id": contact_id}]}

def get_donor_detail(donor_data):
    donor_number = donor_data.get("donor number", "")
    donor_part = donor_data.get("donor part", "")
    contact_id = donor_data.get("contact id", "")
    dart_id = donor_data.get("bhl dart id", "")

    return {u'donor_number': donor_number,
            u'donor_number_auto_generate': False,
            u'dart_id': dart_id,
            u'beal_contact_id': contact_id}


def load_donor_data(filepath):
    with open(filepath, mode="r") as f:
        reader = UnicodeDictReader(f, delimiter="\t")
        return list(reader)


def extract_agents(donor_data):
    corporations = []
    people = []
    for donor in donor_data:
        if donor["last name"] and donor["organization"]:
            corporations.append(donor)
            people.append(donor)
        elif donor["last name"]:
            people.append(donor)
        elif donor["organization"]:
            corporations.append(donor)

    return people, corporations


def make_person_name(person):
    name = nameparser.HumanName()
    name.title = person["title"]
    name.first = person["first name"]
    name.middle = person["middle name"]
    name.last = person["last name"]
    name.suffix = person["suffix"]

    return name


def make_corporation_name(corp):
    return unicode(corp['organization'])


def convert_to_utf8_and_add_headers(filename):
    with open(filename, mode="rb") as f:
        data = f.read()

    data = data.decode("latin-1")
    data = data.encode("utf-8")

    name, extension = filename.split(".")
    with open("{}_clean.{}".format(name, extension), mode="wb") as f:
        headers = ["contact id", "bhl dart id", "suffix", "first name", "middle name", "last name", "title",
                   "organization", "note", "status", "donor number", "donor part", "folder status"]
        f.write("\t".join(headers) + "\n")
        f.write(data)


def UnicodeDictReader(utf8_data, **kwargs):
    csv_reader = csv.DictReader(utf8_data, **kwargs)
    for row in csv_reader:
        yield {key: unicode(value, 'utf-8') for key, value in row.iteritems()}


if __name__ == "__main__":
    main()

