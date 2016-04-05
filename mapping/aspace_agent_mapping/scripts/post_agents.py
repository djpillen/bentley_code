import csv
import json
from pprint import pprint

from tqdm import tqdm


def post_agents_and_record_ids(session, aspace_url, agent_dict):
    session.headers.update({"Content-type":"application/json"})
    name_to_aspace_ids_map = {}

    for agent_type, agent_dct in agent_dict.items():
        for name, json_data in tqdm(agent_dct.items(), desc="posting {}s...".format(agent_type)):
            aspace_agent_type = normalize_agent_type(agent_type)
            response = session.post("{0}/agents/{1}".format(aspace_url, aspace_agent_type), data=json_data).json()
            name_to_aspace_ids_map[unicode(name)] = unicode(extract_aspace_id(session, json_data, response, aspace_url))

    return name_to_aspace_ids_map

def post_donors_and_record_ids(session, aspace_url, agent_dict):
    session.headers.update({"Content-type":"application/json"})
    name_to_aspace_ids_map = {}

    for agent_type, agent_dct in agent_dict.items():
        for name, json_data in tqdm(agent_dct.items(), desc="posting {}s...".format(agent_type)):
            aspace_agent_type = normalize_agent_type(agent_type)
            contact_ids = []
            agent_json = json.loads(json_data)
            for donor_detail in agent_json["donor_details"]:
                contact_ids.append(donor_detail["beal_contact_id"])
            response = session.post("{0}/agents/{1}".format(aspace_url, aspace_agent_type), data=json_data).json()
            aspace_id = unicode(extract_aspace_id(session, json_data, response, aspace_url))
            for contact_id in contact_ids:
                name_to_aspace_ids_map[contact_id] = aspace_id

    return name_to_aspace_ids_map

def return_posted_agent(session, aspace_url, agent_uri):
    response = session.get("{0}{1}".format(aspace_url, agent_uri)).json()
    #pyspace = PySpace(host=host, username=username, password=password)
    #response = pyspace.get_agent(agent_uri)
    return response

def update_posted_agent(session, aspace_url, agent_uri, agent_json):
    session.headers.update({"Content-type":"application/json"})
    response = session.post("{0}{1}".format(aspace_url, agent_uri), data=json.dumps(agent_json)).json()
    #pyspace = PySpace(host=host, username=username, password=password)
    #response = pyspace.update_agent(agent_uri, agent_json)
    if not u"status" in response:
        print response

def extract_aspace_id(session, original_json, returned_json, aspace_url):
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
                return retrieve_agent_uri_by_authority_id(session, aspace_url, original_json["names"][0]["authority_id"])
        except (AttributeError, TypeError):
            pprint(returned_json)
        if u"conflicting_record" in returned_json[u"error"]:
            aspace_id = returned_json[u"error"][u"conflicting_record"][0]
        else:
            pprint(returned_json)
            pprint(original_json)
            quit()
    return aspace_id

def retrieve_agent_uri_by_authority_id(session, aspace_url, auth_id):
    data = {"q": auth_id}
    result = session.get('''{0}/search?page=1'''.format(asapce_url), data=data).json()

    if not result:
        return ""
    elif not result["results"]:
        return ""

    return result["results"][0]["uri"]


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
