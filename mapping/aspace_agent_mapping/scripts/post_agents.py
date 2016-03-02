import csv
import json
from pprint import pprint

from tqdm import tqdm

from utilities.aspace_interface.pyspace import PySpace


def post_agents_and_record_ids(agent_dict, host, username, password):
    name_to_aspace_ids_map = {}
    pyspace = PySpace(host=host, username=username, password=password)

    for agent_type, agent_dct in agent_dict.items():
        for name, json_data in tqdm(agent_dct.items(), desc="posting {}s...".format(agent_type)):
            aspace_agent_type = normalize_agent_type(agent_type)
            response = post_agent(pyspace, json_data, aspace_agent_type)
            name_to_aspace_ids_map[unicode(name)] = unicode(extract_aspace_id(json_data, response, pyspace))

    return name_to_aspace_ids_map

def post_donors_and_record_ids(agent_dict, host, username, password):
    name_to_aspace_ids_map = {}
    pyspace = PySpace(host=host, username=username, password=password)

    for agent_type, agent_dct in agent_dict.items():
        for name, json_data in tqdm(agent_dct.items(), desc="posting {}s...".format(agent_type)):
            aspace_agent_type = normalize_agent_type(agent_type)
            contact_id = json.loads(json_data)["donor_details"][0]["beal_contact_id"]
            response = post_agent(pyspace, json_data, aspace_agent_type)
            name_to_aspace_ids_map[contact_id] = unicode(extract_aspace_id(json_data, response, pyspace))

    return name_to_aspace_ids_map

def extract_aspace_id(original_json, returned_json, pyspace):
    aspace_id = ""
    if not returned_json:
        return ""
    if u"status" in returned_json:
        aspace_id = returned_json[u"uri"]
    if u"error" in returned_json:
        error = returned_json[u'error']
        error = error.values() if type(error) == dict else [[error,],]
        try:
            if [u'Authority ID must be unique'] in error:
                return pyspace.retrieve_agent_uri_by_authority_id(original_json["names"][0]["authority_id"])
        except (AttributeError, TypeError):
            pprint(returned_json)
            exit()
        if u"conflicting_record" in returned_json[u"error"]:
            aspace_id = returned_json[u"error"][u"conflicting_record"][0]
        else:
            print(returned_json)
    return aspace_id


def normalize_agent_type(agent_type):
    if agent_type == "persname":
        return "people"
    if agent_type == "corpname":
        return "corporate_entities"
    if agent_type == "famname":
        return "families"
    else:
        print("??? {}".format(agent_type))


def post_agent(pyspace, json_data, agent_type):
    try:
        return pyspace.add_agent(json_data, agent_type=agent_type)
    except:
        pprint(json_data)
