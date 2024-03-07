import json
import os
import datetime

def format_name(name_with_wildcards: str, input_file):
    last_modified_timestamp = os.path.getmtime(input_file)
    last_modified_time = datetime.datetime.fromtimestamp(last_modified_timestamp)

    name = name_with_wildcards.replace('@month@', last_modified_time.strftime('%B'))
    name = name.replace('@day@', str(last_modified_time.day))
    name = name.replace('@year@', str(last_modified_time.year))
    return name

key = snakemake.params.source_key

if key == 'gsrs':
    gsrs_version = snakemake.params.gsrs_version
    stitcher_inxight_directory = os.environ["STITCHER_DATA_INXIGHT_DIRECTORY"]
    metadata_file = f"../{stitcher_inxight_directory}/files/gsrs_{gsrs_version}_metadata.json"
    with open(metadata_file, 'r') as f:
        metadata = json.load(f)
        metadata_object = {
            key: {
                'name': f"G-SRS, {metadata['date']}",
                'file': f"{stitcher_inxight_directory}/files/gsrs_{gsrs_version}.gsrs",
                'original': metadata['link']
            }
        }
else :
    format = snakemake.params.name_format
    data_file: str = snakemake.input[0]
    if data_file.startswith("../stitcher-inputs"):
        data_file = data_file.split("../")[1]
    metadata_object = {
        key: {
            'name': format_name(format, snakemake.input[0]),
            'file': data_file
        }
    }

with open(snakemake.output[0], 'wt') as f:
    f.write(json.dumps(metadata_object))
