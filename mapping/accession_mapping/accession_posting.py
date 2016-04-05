from vandura.config import accessions_dir
from vandura.shared.scripts.archivesspace_authenticate import authenticate

import getpass
import json
from os.path import join
from pprint import pprint
import requests

from tqdm import tqdm

aspace_url = "http://localhost:8089"
username = "admin"
password = getpass.getpass("Password:")
s = authenticate(aspace_url, username, password)
s.headers.update({"Content-type":"application/json"})

json_data_file = join(accessions_dir, "json", "json_data.json")
with open("json_data.json", mode="r") as f:
    json_data = json.load(f)


for accession_json in tqdm(json_data, desc="posting accessions...", leave=True):
	if type(accession_json) == str:
            accession_json = json.loads(accession_json)

    response = s.post("{0}/repositories/2/accessions".format(aspace_url),
                         data=json.dumps(accession_json)
                         ).json()

    if "invalid_object" in response:
        pprint(json.dumps(response))
        quit()
