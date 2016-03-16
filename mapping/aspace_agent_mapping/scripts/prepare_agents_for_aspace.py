from tqdm import tqdm

from mapping.aspace_agent_mapping.agent_parsers.Corpname import Corpname
from mapping.aspace_agent_mapping.agent_parsers.Famname import Famname
from mapping.aspace_agent_mapping.agent_parsers.Persname import Persname


def prepare_agents(dict_of_agents_by_type):
    # agent dict format: {"corpname": {dict of corporations}, "persname": {dict of persnames}, "famname": {dict of famnames}}
    prepped_data = {}
    for agent_type, dict_of_all_agents_of_type in tqdm(dict_of_agents_by_type.items(), desc="creating aspace json"):
        prepped_data[agent_type] = prepare_json_for_agent_type(agent_type, dict_of_all_agents_of_type)

    return prepped_data


def prepare_json_for_agent_type(agent_type, agent_dict):
    prepped_data = {}
    for name, auth_values in agent_dict.items():
        name = normalize_name(name)
        auth_id, auth_source = auth_values
        auth_source = normalize_source(auth_source)

        if agent_type == "corpname":
            prepped_data[name] = Corpname(name, auth_id, auth_source).get_aspace_json()
        if agent_type == "persname":
            prepped_data[name] = Persname(name, auth_id, auth_source).get_aspace_json()
        if agent_type == "famname":
            prepped_data[name] = Famname(name, auth_id, auth_source).get_aspace_json()

    return prepped_data


def normalize_name(name):
    exceptions = ['University of Michigan--Dearborn',
                  'University of Michigan--Flint',
                  'University of Michigan--Dearborn. Department of History',
                  'University of Wisconsin--Milwaukee',
                  'Lutheran Church--Missouri Synod']

    if any(name.startswith(exception) for exception in exceptions):
        return name

    normalized_name = name.replace("---", "- --")
    normalized_name = normalized_name.split("--")[0].strip()
    if not normalized_name.endswith(".") and not normalized_name.endswith(")") and not normalized_name.endswith('-'):
        normalized_name += "."
    return normalized_name


def normalize_source(source):
    source = source if source else "local"
    valid_sources = ["local", "nad", "naf", "ulan", "lcnaf"]
    if source not in valid_sources:
        source = "local"

    return source