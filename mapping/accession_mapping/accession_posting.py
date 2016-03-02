import json
from pprint import pprint

from tqdm import tqdm

from utilities.aspace_interface.pyspace import PySpace

pyspace = PySpace(host="http://localhost:8089", repository="2", username="admin", password="admin")

'''
print("adding required enumerated values...")
pyspace.add_values_to_enum(55, ["on file", "pending", "sent", "n/a", "other"])
pyspace.add_values_to_enum(14, ["linear feet", "MB", "GB", "KB", "TB"])


print("adding classifications for MHC, UARP, and RCS...")
pyspace.add_classification('Michigan Historical Collections', 'MHC')
pyspace.add_classification('University Archives and Records Program', 'UARP')
pyspace.add_classification('Records Center Storage', 'RCS')
'''

with open("json_data.json", mode="r") as f:
    json_data = json.load(f)


for accession_json in tqdm(json_data, desc="posting accessions...", leave=True):
    response = pyspace.add_accession(accession_json)
    if "invalid_object" in response:
        pprint(json.dumps(response))
