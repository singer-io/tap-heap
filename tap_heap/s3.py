import boto3
import json
import re
import singer


LOGGER = singer.get_logger()


def setup_aws_client(config):
    pass
    #boto3.setup_default_session(aws_access_key_id="", aws_secret_access_key="", aws_session_token=role['Credentials']['SessionToken'])

def generate_merged_manifests(bucket):
    tables = []
    manifests = list_manifest_files_in_bucket(bucket)
    for manifest in manifests:
        contents = get_file_handle(bucket, manifest['Key'])
        manifest_content = json.loads(contents.read().decode('utf-8'))
        tables += [generate_manifest_tables(manifest_content)]

    return merge_manifests(tables[0], tables[1:])


def generate_manifest_tables(manifest):
    manifest_table = {}
    for table in manifest['tables']:
        manifest_table[table['name']] = {
            "files": set(table['files']),
            "columns": set(table['columns']),
            "incremental": table['incremental']
        }
    return manifest_table


def merge_manifests(merged, rest):
    if not rest:
        return merged
    merged = merge(merged, rest[0])
    rest = rest[1:]
    return merge_manifests(merged, rest)


def merge(left, right):
    merged = left
    for table_key in right.keys():
        if type(left[table_key]) == dict and type(right[table_key]) == dict:
            for key in right[table_key].keys():
                if type(left[table_key][key]) == set and type(right[table_key][key]) == set:
                    merged[table_key][key] = merged[table_key][key] | right[table_key][key]

    return merged


def list_manifest_files_in_bucket(bucket):
    s3_client = boto3.client('s3')

    s3_objects = []
    max_results = 1000
    args = {
        'Bucket': bucket,
        'MaxKeys': max_results,
    }

    args['Prefix'] = "manifests"
    result = s3_client.list_objects_v2(**args)

    next_continuation_token = None
    if result['KeyCount'] > 0:
        s3_objects += result['Contents']
        next_continuation_token = result.get('NextContinuationToken')

    while next_continuation_token is not None:
        LOGGER.info('Continuing pagination with token "%s".', next_continuation_token)

        continuation_args = args.copy()
        continuation_args['ContinuationToken'] = next_continuation_token

        result = s3_client.list_objects_v2(**continuation_args)

        s3_objects += result['Contents']
        next_continuation_token = result.get('NextContinuationToken')

    if s3_objects:
        LOGGER.info("Found %s files.", len(s3_objects))
    else:
        LOGGER.warning('Found no files for bucket "%s" that match prefix "%s"', bucket, search_prefix)

    matcher = re.compile(".*.json")
    for s3_object in s3_objects:
        if matcher.search(s3_object['Key']):
            yield s3_object


def get_file_handle(bucket, s3_path):
    s3_client = boto3.resource('s3')

    s3_bucket = s3_client.Bucket(bucket)
    s3_object = s3_bucket.Object(s3_path)
    return s3_object.get()['Body']
