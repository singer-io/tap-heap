from collections import defaultdict

import boto3
from botocore.exceptions import ClientError

import singer
from singer import metadata

from tap_heap import manifest
from tap_heap.exceptions import HeapForbiddenError
from tap_heap.schema import generate_fake_schema

LOGGER = singer.get_logger()


def discover_streams(bucket):
    streams = []

    manifests = manifest.generate_manifests(bucket)

    table_name_to_columns = defaultdict(set)
    for all_table_manifests in manifests.values():
        for table_name, table_manifest in all_table_manifests.items():
            table_name_to_columns[table_name].update(set(table_manifest['columns']))

    for table_name, columns in table_name_to_columns.items():
        schema = generate_fake_schema(columns)
        streams.append({'stream': table_name, 'tap_stream_id': table_name,
                        'schema': schema, 'metadata': load_metadata(table_name, schema)})

    streams = _apply_access_checks(bucket, streams, manifests)

    return streams


def _check_stream_access(bucket, table_name, manifests, s3_client=None):
    """
    Verify read access to a stream's data files in S3.
    Returns True if accessible, False if a 403 AccessDenied error is raised.
    """
    if s3_client is None:
        s3_client = boto3.client('s3')

    # Find the first file for this table to test access
    for _, dump_manifest in manifests.items():
        table_manifest = dump_manifest.get(table_name)
        if table_manifest and table_manifest.get('files'):
            test_file = table_manifest['files'][0]
            # Remove the s3://bucket/ prefix if present
            path_prefix = f's3://{bucket}/'
            test_file = test_file.replace(path_prefix, '')
            try:
                s3_client.head_object(Bucket=bucket, Key=test_file)
                return True
            except ClientError as e:
                error_code = e.response.get('Error', {}).get('Code', '')
                if error_code in ('403', 'AccessDenied'):
                    return False
                # Re-raise non-permission errors
                raise
    # If no files found for this table, consider it accessible (schema-only)
    return True


def _apply_access_checks(bucket, streams, manifests):
    """
    Probe each stream for read access and remove inaccessible streams.
    Raises HeapForbiddenError if no streams are accessible.
    """
    s3_client = boto3.client('s3')
    inaccessible_streams = []
    accessible_streams = []

    for stream in streams:
        table_name = stream['tap_stream_id']
        if _check_stream_access(bucket, table_name, manifests, s3_client=s3_client):
            accessible_streams.append(stream)
        else:
            inaccessible_streams.append(table_name)

    if inaccessible_streams:
        if not accessible_streams:
            raise HeapForbiddenError(
                "S3 AccessDenied: The credentials do not have "
                "'read' access to any of the streams supported "
                "by the tap. Data collection cannot be initiated."
            )
        LOGGER.warning(
            "The credentials do not have 'read' access to the "
            "following stream(s): %s. "
            "These streams have been excluded from the catalog.",
            ", ".join(inaccessible_streams),
        )

    return accessible_streams


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

    return metadata.to_list(mdata)
