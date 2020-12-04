import time
import re
import fastavro
import singer

from singer import metadata
from singer import Transformer
from tap_heap import s3
from tap_heap.schema import generate_schema_from_avro

LOGGER = singer.get_logger()

def filter_manifests_to_sync(manifests, table_name, state):
    """Filters a set of files for the table using 2 parts of the file name and drops up to
    the bookmark if there is a bookmark."""

    bookmark = singer.get_bookmark(state, table_name, 'file')
    # bookmark = "sync_{DUMP_ID}/{TABLE_NAME}/part-00016-{GUID}.avro"
    if bookmark:
        bookmarked_dump_id = int(bookmark.split('/')[0].replace('sync_', ''))

    incremental_dumps = [dump_id for dump_id, manifest in manifests.items() if manifest.get(table_name, {}).get('incremental') == False]
    last_incremental_dump = max(incremental_dumps)
    if bookmark:
        minimum_dump_id_to_sync = max(last_incremental_dump, bookmarked_dump_id)
    else:
        minimum_dump_id_to_sync = last_incremental_dump

    # table_manifest[dump_id] = {"files" ["file 1"], "incremental": True, "columns": ["column_1"]}
    table_manifests = {dump_id: manifest.get(table_name)
                       for dump_id, manifest in manifests.items()
                       if dump_id >= minimum_dump_id_to_sync and manifest.get(table_name)}

    should_send_activate_version = True
    if bookmark:
        should_send_activate_version = minimum_dump_id_to_sync != bookmarked_dump_id

    return (table_manifests, should_send_activate_version)

def remove_prefix(file_name, bucket):
    path_prefix = 's3://{}/'.format(bucket)

    return file_name.replace(path_prefix, '')


def get_files_to_sync(table_manifests, table_name, state, bucket):
    bookmark = singer.get_bookmark(state, table_name, 'file')

    # Get flattened file names and remove the prefix

    files = sorted([remove_prefix(file_name, bucket)
                    for manifest in table_manifests.values()
                    for file_name in manifest['files']])

    if bookmark:
        files = files[files.index(bookmark)+1:]

    return files

def sync_stream(bucket, state, stream, manifests):
    table_name = stream['stream']
    LOGGER.info('Syncing table "%s".', table_name)

    table_manifests, should_send_activate_version = filter_manifests_to_sync(manifests, table_name, state)
    files = get_files_to_sync(table_manifests, table_name, state, bucket)

    records_streamed = 0

    version = singer.get_bookmark(state, table_name, 'version')

    # Detect whether we need to create a new version
    if should_send_activate_version:
        # Set version so it can be used for an activate version message
        version = int(time.time() * 1000)

        LOGGER.info('Detected full sync for stream table name %s, setting version to %d', table_name, version)
        state = singer.write_bookmark(state, table_name, 'version', version)
        singer.write_state(state)

    for s3_file_path in files:
        file_records_streamed = sync_file(bucket, s3_file_path, stream, version)
        records_streamed += file_records_streamed
        LOGGER.info('Wrote %d records for file %s', file_records_streamed, s3_file_path)

        # Finished syncing a file, write a bookmark
        state = singer.write_bookmark(state, table_name, 'file', s3_file_path)
        singer.write_state(state)

    # After syncing, activate the new version
    if should_send_activate_version:
        LOGGER.info('Sending activate version message %d', version)
        message = singer.ActivateVersionMessage(stream=table_name, version=version)
        singer.write_message(message)

    LOGGER.info('Wrote %s records for table "%s".', records_streamed, table_name)
    return records_streamed


def sync_file(bucket, s3_path, stream, version=None):
    LOGGER.info('Syncing file "%s".', s3_path)

    table_name = stream['stream']

    s3_file_handle = s3.get_file_handle(bucket, s3_path)
    iterator = fastavro.reader(s3_file_handle._raw_stream)
    mdata = metadata.to_map(stream['metadata'])
    schema = generate_schema_from_avro(iterator.schema)

    key_properties = metadata.get(mdata, (), 'table-key-properties')
    singer.write_schema(table_name, schema, key_properties)

    records_synced = 0
    with Transformer() as transformer:
        for row in iterator:
            to_write = transformer.filter_data_by_metadata(row, mdata)
            singer.write_message(singer.RecordMessage(table_name, to_write, version=version))
            records_synced += 1

    return records_synced
