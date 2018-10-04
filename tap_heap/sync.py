import time
import fastavro
import singer

from singer import metadata
from singer import Transformer
from tap_heap import s3
from tap_heap.schema import generate_schema_from_avro

LOGGER = singer.get_logger()


def filter_files_to_sync(files, bucket, table_name, state):
    # Remove the prefixes from all the files
    path_prefix = 's3://{}/'.format(bucket)
    files = sorted(set(map(lambda x: x.replace(path_prefix, ''), files)))

    # Drop files that we've already synced
    bookmark = singer.get_bookmark(state, table_name, 'file')
    if bookmark:
        LOGGER.info("Filtering files by bookmark %s", bookmark)
        files = [f for f in files if bookmark < f]

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

        # Activate a version so we execute a full table sync
        version = int(time.time() * 1000)
        message = singer.ActivateVersionMessage(stream=table_name, version=version)
        singer.write_message(message)

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

    records_synced = 0
    for row in iterator:
        with Transformer() as transformer:
            to_write = transformer.transform(row, schema, mdata)

        singer.write_message(singer.RecordMessage(table_name, to_write, version=version))
        records_synced += 1

    return records_synced
