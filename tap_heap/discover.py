from singer import metadata
from tap_heap import manifest
from tap_heap.schema import generate_fake_schema

def discover_streams(bucket, selected_by_default=False):
    streams = []

    merged_manifests = manifest.generate_merged_manifests(bucket)
    for table_name, manifest_table in merged_manifests.items():
        schema = generate_fake_schema(manifest_table)
        streams.append({'stream': table_name,
                        'tap_stream_id': table_name,
                        'schema': schema,
                        'metadata': load_metadata(table_name, schema, selected_by_default)})

    return streams


def get_key_properties(table_name):
    if table_name == 'user_migrations':
        return ['from_user_id']
    if table_name == 'users':
        return ['user_id']
    return ['event_id']


def load_metadata(table_name, schema, selected_by_default=False):
    mdata = metadata.new()

    key_properties = get_key_properties(table_name)
    mdata = metadata.write(mdata, (), 'table-key-properties', key_properties)
    if selected_by_default:
        mdata = metadata.write(mdata, (), 'selected-by-default', True)

    for field_name in schema.get('properties', {}).keys():
        if field_name in key_properties:
            mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'automatic')
        else:
            mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'available')
            if selected_by_default:
                mdata = metadata.write(mdata,
                                       ('properties', field_name),
                                       'selected-by-default',
                                       True)

    return metadata.to_list(mdata)
