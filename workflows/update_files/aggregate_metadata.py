import json

obj = {}
for file in snakemake.input:
    with open(file, 'r') as f:
        data = json.load(f)
        for key in data:
            obj[key] = data[key]

with open(snakemake.output[0], 'w') as f:
    f.write(json.dumps(obj, indent=2))
