import json

from singer import metadata
from tap_heap import s3

def discover_streams(bucket):
    streams = []

    merged_manifests = s3.generate_merged_manifests(bucket)
    for table_name, table_details in merged_manifests.items():
        schema = generate_schema(table_details)
        streams.append({'stream': table_name, 'tap_stream_id': table_name, 'schema': schema, 'metadata': load_metadata(schema)})

    return streams


def generate_schema(table_details):
    schema = {
        "type": "object",
        "properties": {}
    }
    for column in table_details['columns']:
        schema['properties'][column] = {"type": "string"}
    return schema


def load_metadata(schema):
    mdata = metadata.new()

    #mdata = metadata.write(mdata, (), 'table-key-properties', table_spec['key_properties'])

    for field_name in schema.get('properties', {}).keys():
        #if table_spec.get('key_properties', []) and field_name in table_spec.get('key_properties', []):
        #    mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'automatic')
        #else:
        mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'available')

    return metadata.to_list(mdata)


# {
#     "click_on_anything": {
#         "files": [],
#         "columns": [],
#         "incremental": true
#     },
#     ...
# }
