import sys
import json

transactional_root = sys.argv[1]
payload = sys.argv[2]

# write out list of all patient_key values found in all transactional files
patient_key_counts = {}
with open(transactional_root, 'r') as f:
    for line in f:
        patient_key_counts[line.split('|')[142][:-2]] = \
            patient_key_counts.get(line.split('|')[142][:-2], 0) + 1

# explode json file based on patient counts
with open(payload, 'r') as payload_json, open('exploded.json', 'w') as output:
    for line in payload_json.readlines():
        lineJson = json.loads(line)
        if patient_key_counts.get(lineJson['hvJoinKey'], 0) == 0:
            print("couldn't find: " + lineJson['hvJoinKey'])
        for i in range(patient_key_counts.get(lineJson['hvJoinKey'], 0)):
            lineJson['pk'] = lineJson['hvJoinKey'] + '_' + str(i)
            output.write(json.dumps(lineJson) + "\n")
