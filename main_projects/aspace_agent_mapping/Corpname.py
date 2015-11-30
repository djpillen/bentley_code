from main_projects.aspace_agent_mapping.parsers.create_corpname_json import parse_corpname

class Corpname:
    def __init__(self, string, auth_id="", auth_source=""):
        self.data_dict = parse_corpname(string, auth_id, auth_source)

    def get_aspace_json(self):
        return {"publish": True, "agent_type": "agent_corporate_entity", "names": [self.data_dict]}
