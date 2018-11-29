import time
import re
import fastavro
import singer

from singer import metadata
from singer import Transformer
from tap_heap import s3
from tap_heap.schema import generate_schema_from_avro

LOGGER = singer.get_logger()

def key_fn(key):
    """This function ensures we sort a list of manifest files based on the 'sync_id' and 'part_id'
    For example given a key of:
      'sync_852/sessions/part-00000-4a06bab5-0ef3-4b21-b9af-e772fbb37b0e-c000.avro'

    This function returns a tuple: (int("852"), int("00000")
    """

    matches = re.findall('([0-9]+)', key)
    return (int(matches[0]), int(matches[1]))


def filter_files_to_sync(files, bucket, table_name, state):
    """Filters a set of files for the table using 2 parts of the file name and drops up to
    the bookmark if there is a bookmark."""
    # Remove the prefixes from all the files
    path_prefix = 's3://{}/'.format(bucket)

    files = sorted(set(map(lambda x: x.replace(path_prefix, ''), files)), key=key_fn)

    # Drop files that we've already synced
    bookmark = singer.get_bookmark(state, table_name, 'file')
    if bookmark:
        LOGGER.info("Filtering files by bookmark %s", bookmark)
        files = files[files.index(bookmark)+1:]

    return files


def sync_stream(bucket, state, stream, manifest_table):
    table_name = stream['stream']
    LOGGER.info('Syncing table "%s".', table_name)

    files = filter_files_to_sync(manifest_table['files'], bucket, table_name, state)
    records_streamed = 0

    version = None
    if not manifest_table['incremental'] and files:
        # Filter files so that only the newest manifest's files are synced
        newest_manifest_id = sorted(manifest_table['manifests'])[-1]
        files = [f for f in files if "sync_{}".format(newest_manifest_id) in f]

        # Set version so it can be used for an activate version message
        version = int(time.time() * 1000)

    for s3_file_path in files:
        records_streamed += sync_file(bucket, s3_file_path, stream, version)

        # Finished syncing a file, write a bookmark
        state = singer.write_bookmark(state, table_name, 'file', s3_file_path)
        singer.write_state(state)

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

    # Activate a version so we execute a full table sync
    if version is not None:
        message = singer.ActivateVersionMessage(stream=table_name, version=version)
        singer.write_message(message)

    records_synced = 0
    for row in iterator:
        with Transformer() as transformer:
            to_write = transformer.transform(row, schema, mdata)

        singer.write_message(singer.RecordMessage(table_name, to_write, version=version))
        records_synced += 1

    return records_synced
