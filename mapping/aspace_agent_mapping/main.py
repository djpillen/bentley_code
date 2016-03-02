import json

from scripts.add_aspace_agent_ids_to_eads import add_aspace_ids_to_all_agents_in_dir
from scripts.get_agents_from_eads import get_all_agents
from scripts.post_agents import post_agents_and_record_ids
from scripts.prepare_agents_for_aspace import prepare_agents

# code to extract agents from EADs, post them to an ASpace instance, then write the new ASpace
# references for each agent back to the EAD files

# third-party libraries required: NLTK, tqdm, requests, nameparser

# presets:
ead_dir = r"C:\Users\djpillen\GitHub\archivesspace_migration\eads"
aspace_address = "http://localhost:8089"
aspace_username = "admin"
aspace_password = "admin"


def main():
    # extract agent data from eads
    agents = get_all_agents(ead_dir)

    # create json-formatted entries for each agent
    prepped_agents = prepare_agents(agents)

    with open("agents_aspace.json", mode="wb") as f:
        string = json.dumps(prepped_agents, ensure_ascii=False, encoding="utf-8", sort_keys=True, indent=4)
        f.write(string.encode("utf-8"))

    # send these formatted entries to aspace and retrieve their new IDs
    local_name_to_aspace_id_map = post_agents_and_record_ids(prepped_agents, host=aspace_address, username=aspace_username, password=aspace_password)

    # add these ids to our EAD files to map properly
    ead_path = ""
    add_aspace_ids_to_all_agents_in_dir(local_name_to_aspace_id_map, path_to_eads=ead_dir)


if __name__ == "__main__":
    main()
