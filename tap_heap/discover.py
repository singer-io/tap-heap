from collections import defaultdict
from singer import metadata
from tap_heap import manifest
from tap_heap.schema import generate_fake_schema

def discover_streams(bucket):
    streams = []

    manifests = manifest.generate_manifests(bucket)

    table_name_to_columns = defaultdict(set)
    table_name_to_incremental = {}

    for all_table_manifests in manifests.values():
        for table_name, table_manifest in all_table_manifests.items():
            table_name_to_columns[table_name].update(set(table_manifest['columns']))
            table_name_to_incremental[table_name] = table_manifest.get('incremental', False)

    for table_name, columns in table_name_to_columns.items():
        schema = generate_fake_schema(columns)
        mdata = load_metadata(table_name, schema)

        if table_name_to_incremental.get(table_name, False):
            mdata = metadata.write(mdata, (), 'forced-replication-method', 'INCREMENTAL')
        else:
            mdata = metadata.write(mdata, (), 'forced-replication-method', 'FULL_TABLE')

        streams.append({'stream': table_name, 'tap_stream_id': table_name,
                        'schema': schema, 'metadata': metadata.to_list(mdata)})

    return streams


def get_key_properties(table_name):
    if table_name == 'user_migrations':
        return ['from_user_id']
    elif table_name == 'users':
        return ['user_id']
    else:
        return ['event_id']


def load_metadata(table_name, schema):
    mdata = metadata.new()

    key_properties = get_key_properties(table_name)
    mdata = metadata.write(mdata, (), 'table-key-properties', key_properties)

    for field_name in schema.get('properties', {}).keys():
        if field_name in key_properties:
            mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'automatic')
        else:
            mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'available')

    return mdata
