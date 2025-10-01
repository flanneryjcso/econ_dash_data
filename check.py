
import json

with open('/home/flanneryj/econ_dash/econ_dash_dict.json', 'r') as json_file:
    old_dict = json.load(json_file)

print(old_dict["rsm08"])