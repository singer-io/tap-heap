from concurrent import futures

import multiprocessing
import time
import re
import backoff
import fastavro
import singer
import queue

from singer import metadata
from singer import Transformer
from tap_heap import s3
from tap_heap.schema import generate_schema_from_avro

LOGGER = singer.get_logger()

# Define global queue
QUEUE_TIMEOUT = 120
QUEUE_MAX_LIMIT = 20000
record_queue = multiprocessing.Queue(maxsize=QUEUE_MAX_LIMIT)


# This event will signal all producer and consumer threads to stop thier execution
# if all files are extracted or any other thread exits abruptly.
terminate_event = multiprocessing.Event()


def filter_manifests_to_sync(manifests, table_name, state):
    """Filters a set of files for the table using 2 parts of the file name and drops up to
    the bookmark if there is a bookmark."""

    bookmark = singer.get_bookmark(state, table_name, 'file')
    # bookmark = "sync_{DUMP_ID}/{TABLE_NAME}/part-00016-{GUID}.avro"
    bookmarked_version = singer.get_bookmark(state, table_name, 'version')
    if bookmark and bookmarked_version:
        bookmarked_dump_id = int(bookmark.split('/')[0].replace('sync_', ''))

    full_table_dumps = [dump_id for dump_id, manifest in manifests.items()
                        if manifest.get(table_name, {}).get('incremental') is False]

    if bookmark and bookmarked_version:
        minimum_dump_id_to_sync = max([bookmarked_dump_id] + full_table_dumps)
        should_create_new_version = minimum_dump_id_to_sync != bookmarked_dump_id
    else:
        if len(full_table_dumps) > 0:
            minimum_dump_id_to_sync = max(full_table_dumps)
        else:
            minimum_dump_id_to_sync = 0

        should_create_new_version = True

    # table_manifest[dump_id] = {"files" ["file 1"], "incremental": True, "columns": ["column_1"]}
    table_manifests = {dump_id: manifest.get(table_name)
                       for dump_id, manifest in manifests.items()
                       if dump_id >= minimum_dump_id_to_sync and manifest.get(table_name)}

    return (table_manifests, should_create_new_version)

def remove_prefix(file_name, bucket):
    path_prefix = f's3://{bucket}/'

    return file_name.replace(path_prefix, '')

def key_fn(key):
    """This function ensures we sort a list of manifest files based on the 'sync_id' and 'part_id'
    For example given a key of:
      'sync_852/sessions/part-00000-4a06bab5-0ef3-4b21-b9af-e772fbb37b0e-c000.avro'
    This function returns a tuple: (int("852"), int("00000")
    """

    file_path = key.split('/')
    dump_id = file_path[0].replace('sync_', '')
    part_number = re.findall('([0-9]+)', file_path[-1])[0]
    return (int(dump_id), int(part_number))

def get_files_to_sync(table_manifests, table_name, state, bucket):
    bookmark = singer.get_bookmark(state, table_name, 'file')
    bookmarked_version = singer.get_bookmark(state, table_name, 'version')

    # Get flattened file names and remove the prefix
    files = sorted([remove_prefix(file_name, bucket)
                    for manifest in table_manifests.values()
                    for file_name in manifest['files']], key=key_fn)

    if bookmark and bookmarked_version and bookmark in files:
        # NB> The bookmark is a fully synced file, so start immediately
        # after the bookmark
        files = files[files.index(bookmark)+1:]

    return files

def write_records():
    # Consumer thread will exit when terminate event is set but
    # wait for queue to get empty after terminate event is set.
    while not terminate_event.is_set() or record_queue.qsize():
        try:
            singer.write_message(record_queue.get(timeout=5))
        except queue.Empty:
            continue
        except Exception as ex:    # pylint: disable=broad-exception-caught
            terminate_event.set()
            raise Exception(f"Consumer thread stopped abruptly!") from ex
    LOGGER.info("Existing from the consumer thread!")

def sync_stream(bucket, state, stream, manifests, batch_size=5):    # pylint: disable=too-many-locals
    table_name = stream['stream']
    LOGGER.info('Syncing table "%s".', table_name)

    table_manifests, should_create_new_version = filter_manifests_to_sync(manifests,
                                                                          table_name,
                                                                          state)

    files = get_files_to_sync(table_manifests, table_name, state, bucket)

    records_streamed = 0

    version = singer.get_bookmark(state, table_name, 'version')

    if should_create_new_version:
        # Set version so it can be used for an activate version message
        version = int(time.time() * 1000)

        LOGGER.info('Detected full sync for stream table name %s, setting version to %d',
                    table_name,
                    version)
        state = singer.write_bookmark(state, table_name, 'version', version)
        singer.write_state(state)

    with futures.ProcessPoolExecutor(max_workers=batch_size) as executor:
        # Create and start the consumer process
        consumer = multiprocessing.Process(target=write_records)
        consumer.start()

        stored_exception = None
        for i in range(0, len(files), batch_size):
            batch = files[i:i + batch_size]
            future_to_file = {executor.submit(
                sync_file, bucket, file_path, stream, version): file_path for file_path in batch}

            # Wait for the current batch to complete
            for future in futures.as_completed(future_to_file):
                file_path = future_to_file[future]
                try:
                    records_streamed += future.result()
                except Exception as ex:     # pylint: disable=broad-exception-caught
                    stored_exception = ex
                    terminate_event.set()
                    break

            if terminate_event.is_set():
                raise Exception(f"Error reading file {file_path}") from stored_exception     # pylint: disable=broad-exception-raised

            # Finished syncing a file, write a bookmark
            state = singer.write_bookmark(state, table_name, 'file', files[i + len(batch) - 1])
            singer.write_state(state)

        # Signal the consumer process to stop
        LOGGER.info("Main thread is setting the terminate event after successful extraction!")
        terminate_event.set()

        LOGGER.info("Waiting for all records in the Queue to sync.")
        consumer.join()

        # Clear any thread terminate event set earlier before stream extraction starts
        terminate_event.clear()

    if records_streamed > 0:
        LOGGER.info('Sending activate version message %d', version)
        message = singer.ActivateVersionMessage(stream=table_name, version=version)
        singer.write_message(message)
        record_queue.put(message)

    LOGGER.info('Wrote %s records for table "%s".', records_streamed, table_name)
    return records_streamed


@backoff.on_exception(backoff.expo,
                      Exception,
                      max_tries=3,
                      factor=2)
def sync_file(bucket, s3_path, stream, version=None):
    LOGGER.info('Syncing file "%s".', s3_path)

    table_name = stream['stream']

    try:
        s3_file_handle = s3.get_file_handle(bucket, s3_path)
        iterator = fastavro.reader(s3_file_handle._raw_stream)
        mdata = metadata.to_map(stream['metadata'])
        schema = generate_schema_from_avro(iterator.schema)

        key_properties = metadata.get(mdata, (), 'table-key-properties')
        record_queue.put(singer.SchemaMessage(stream=(table_name),
                                        schema=schema,
                                        key_properties=key_properties),
                timeout=QUEUE_TIMEOUT)

        records_synced = 0
        with Transformer() as transformer:
            for row in iterator:
                # Terminate the thread execution if any of producer or consumer threads exits abruptly
                if terminate_event.is_set():
                    raise Exception("Thread is terminated abruptly!")

                to_write = transformer.filter_data_by_metadata(row, mdata)
                record_queue.put(
                    singer.RecordMessage(table_name, to_write, version=version),
                    timeout=QUEUE_TIMEOUT)
                records_synced += 1

        LOGGER.info('Wrote %d records for file %s', records_synced, s3_path)
        return records_synced
    except queue.Full as ex:
        raise ex
    except Exception as ex:
        LOGGER.info(f"{str(ex)}, Terminated {s3_path} extraction thread!")
        raise ex

