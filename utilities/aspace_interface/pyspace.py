import json
from pprint import pprint

import requests
from tqdm import tqdm


class PySpace (object):
    def __init__(self, host="http://localhost:8089", username="admin", password="admin", repository="2"):
        self.host = host
        self.repository = repository

        auth_url = "{0}/users/{1}/login?password={2}".format(host, username, password)
        auth_json_response = requests.post(auth_url).json()

        self.session_id = auth_json_response["session"]
        self.headers_contentype_json = {'Content-type': 'application/json', 'X-ArchivesSpace-Session': self.session_id}
        self.headers_session_only = {"X-ArchivesSpace-Session": self.session_id}

    def add_ead(self, ead_json):
        return requests.post("{0}/repositories/{1}/batch_imports".format(self.host, self.repository),
                             headers=self.headers_contentype_json,
                             data=json.dumps(ead_json)
                             ).json()

    def add_accession(self, accession_json):
        if type(accession_json) == str:
            accession_json = json.loads(accession_json)

        return requests.post("{0}/repositories/{1}/accessions".format(self.host, self.repository),
                             headers=self.headers_contentype_json,
                             data=json.dumps(accession_json)
                             ).json()

    def add_control_access_values(self, enum_id, value_list):
        pass

    def ead_to_json(self, path_to_ead_file):
        with open(path_to_ead_file, mode="rb") as f:
            headers = {'Content-type': 'text/xml; charset=utf-8', 'X-ArchivesSpace-Session': self.session_id}
            return requests.post("{0}/plugins/jsonmodel_from_format/resource/ead".format(self.host),
                                 headers=headers,
                                 data=f
                                 ).json()

    def add_agent(self, agent_json, agent_type):
        return requests.post('{0}/agents/{1}'.format(self.host, agent_type),
                             headers=self.headers_contentype_json,
                             data=agent_json
                             ).json()

    def add_family(self, family_json):
        return self.add_agent(agent_json=family_json, agent_type="families")

    def add_corporation(self, corp_json):
        return self.add_agent(agent_json=corp_json, agent_type="corporate_entities")

    def add_person(self, person_json):
        return self.add_agent(agent_json=person_json, agent_type="people")

    def get_agent(self, agent_uri):
        url = "{0}{1}".format(self.host, agent_uri)
        return requests.get("{0}{1}".format(self.host, agent_uri), headers=self.headers_session_only).json()

    def update_agent(self, agent_uri, agent_json):
        url = "{0}{1}".format(self.host, agent_uri)
        return requests.post("{0}{1}".format(self.host, agent_uri), headers=self.headers_contentype_json, data=json.dumps(agent_json)).json()

    def get_all_object_ids(self, object_type):
        """
        acceptable object types:
            accessions
            archival_objects
            classification_terms
            classifications
            digital_object_components
            digital_objects
            events
            groups
            resources
        """
        return requests.get('{0}/repositories/{1}/{2}?all_ids=true'.format(self.host, self.repository, object_type),
                            headers=self.headers_session_only
                            ).json()

    def retrieve_agent_uri_by_authority_id(self, auth_id):
        data = {"q": auth_id}
        result = requests.get('''{0}/search?page=1'''.format(self.host), headers=self.headers_session_only, data=data).json()

        if not result:
            return ""
        elif not result["results"]:
            return ""

        return result["results"][0]["uri"]

    def add_values_to_enum(self, enum_id, values_to_add):
        enum_address = "{}/config/enumerations/{}".format(self.host, enum_id)
        existing_enums = requests.get(enum_address, headers=self.headers_session_only).json()
        existing_enums["values"].extend(values_to_add)
        return requests.post(enum_address, headers=self.headers_session_only, data=json.dumps(existing_enums)).json()

    def add_classification(self, classification_title, classification_identifier):
        classification_json = {u'title': classification_title, u'identifier': classification_identifier}
        return requests.post('{}/repositories/{}/classifications'.format(self.host, self.repository),
                             headers=self.headers_session_only,
                             data=json.dumps(classification_json)
                             ).json()

    def get_repository_level_object_json_by_uri(self, uri):
        return requests.get("{}/repositories/{}{}".format(self.host, self.repository, uri), self.headers_session_only).json()

    def get_system_level_object_json_by_uri(self, uri):
        return requests.get("{}{}".format(self.host, uri), headers=self.headers_session_only).json()

    def delete_aspace_object(self, object_type, aspace_id):
        """
        acceptable object types:
            accessions
            archival_objects
            classification_terms
            classifications
            digital_object_components
            digital_objects
            events
            groups
            resources
        """
        return requests.delete('{0}/repositories/{1}/{2}/{3}'.format(self.host, self.repository, object_type, aspace_id),
                               headers=self.headers_session_only
                               ).json()

    def delete_agent_by_uri(self, uri):
        return requests.delete('{0}{1}'.format(self.host, uri), headers=self.headers_session_only).json()

    def delete_all_agents(self):
        agent_types = ["people", "families", "corporate_entities", "software"]
        for agent_type in agent_types:
            ids = requests.get("{0}/agents/{1}?all_ids=true".format(self.host, agent_type), headers=self.headers_session_only).json()
            for id_ in tqdm(ids, desc="deleting all {}".format(agent_type)):
                requests.delete("{0}/agents/{1}/{2}".format(self.host, agent_type, id_), headers=self.headers_session_only)

    def change_repository(self, repository_number):
        self.repository = repository_number
