from singer import metadata
from singer import Transformer
from singer import utils
import fastavro

import singer
#from singer_encodings import csv

from tap_heap import s3

LOGGER = singer.get_logger()


def generate_schema(avro_schema):
    # avro_schema
    # {
    #
    #   "fields": [  {}  ]
    # }

    # Generate properties
    properties = {}
    # iterate avro_schema['fields']
    # properties[field['name']] = translate_fn()
    # properties[field['name']]["type"] = ["type", "null"]

    for thing in avro_schema['fields']:
        properties[thing['name']] = {"type": my_fun(thing["type"])}

    # => { type: object, properties: { "": {}  }  }
    return {"type": "object", "properties": properties}


def my_fun(avro_type):

    translated_type = ["null"]

    for t in avro_type:
        if t == "null":
            continue

        if t in ["int", "long"]:
            translated_type.append("integer")
        elif t in ["float", "double"]:
            translated_type.append("number")
        elif t == 'boolean':
            translated_type.append("boolean")
        elif t in ["bytes", "string", "enum", "fixed"]:
            translated_type.append("string")
        else:
            raise Exception("ENCOUNTED A TYPE WE CAN't TRANSLATE: " + t)

    return translated_type

def sync_stream(bucket, state, stream):
    table_name = stream['stream']

    LOGGER.info('Syncing table "%s".', table_name)

    # this is a bit circular....
    merged_manifests = s3.generate_merged_manifests(bucket)

    #LOGGER.info('Found %s files to be synced.', len(s3_files))

    # Filter manifests by state?

    records_streamed = 0
    if not merged_manifests:
        return records_streamed

    # we build this thing every time only to get the one we care about in the above call-stack
    table = next(m for name, m in merged_manifests.items() if name == table_name)

    path_prefix = 's3://' + bucket + '/'
    for s3_file_path in table['files']:
        s3_file_path = s3_file_path.replace(path_prefix, '')

        records_streamed += sync_file(bucket, s3_file_path, stream)

        #state = singer.write_bookmark(state, table_name, 'modified_since', s3_file['last_modified'].isoformat())
        #singer.write_state(state)

    LOGGER.info('Wrote %s records for table "%s".', records_streamed, table_name)

    return records_streamed


def sync_file(bucket, s3_path, stream):
    LOGGER.info('Syncing file "%s".', s3_path)

    table_name = stream['stream']

    s3_file_handle = s3.get_file_handle(bucket, s3_path)

    # REPLACE ME
    iterator = fastavro.reader(s3_file_handle._raw_stream)
    schema = generate_schema(iterator.schema)
    singer.write_schema(table_name, schema, [])

    records_synced = 0

    for row in iterator:
        custom_columns = {
            #s3.SDC_SOURCE_BUCKET_COLUMN: bucket,
            #s3.SDC_SOURCE_FILE_COLUMN: s3_path,
        }
        rec = {**row, **custom_columns}

        with Transformer() as transformer:
            to_write = transformer.transform(rec, schema, metadata.to_map(stream['metadata']))

        singer.write_record(table_name, to_write)
        records_synced += 1

    return records_synced
