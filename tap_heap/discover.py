from collections import defaultdict
import singer
from singer import metadata
from tap_heap import manifest
from tap_heap.schema import generate_fake_schema

LOGGER = singer.get_logger()

def discover_streams(bucket):
    streams = []

    manifests = manifest.generate_manifests(bucket)

    table_name_to_columns = defaultdict(set)
    table_name_to_incremental_flags = defaultdict(set)

    for all_table_manifests in manifests.values():
        for table_name, table_manifest in all_table_manifests.items():
            table_name_to_columns[table_name].update(table_manifest['columns'])
            flag = table_manifest.get('incremental')
            if flag is not None:
                table_name_to_incremental_flags[table_name].add(flag)

    for table_name, columns in table_name_to_columns.items():
        schema = generate_fake_schema(columns)
        mdata = load_metadata(table_name, schema)

        flags = table_name_to_incremental_flags.get(table_name, set())
        if not flags or flags == {False}:
            replication = 'FULL_TABLE'
        elif flags == {True}:
            replication = 'INCREMENTAL'
        else:
            LOGGER.warning("Conflicting incremental flags for %s: %s. Defaulting to \
                           INCREMENTAL.", table_name, flags)
            replication = 'INCREMENTAL'

        mdata = metadata.write(mdata, (), 'forced-replication-method', replication)

        streams.append({
            'stream': table_name,
            'tap_stream_id': table_name,
            'schema': schema,
            'metadata': metadata.to_list(mdata)
        })

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
