import boto3
import json
import os
import random
import re
import singer

def get_file_handle(bucket, s3_path):
    s3_client = boto3.resource('s3')

    s3_bucket = s3_client.Bucket(bucket)
    s3_object = s3_bucket.Object(s3_path)
    return s3_object.get()['Body']


def get_s3_manifest_file_contents(bucket):
    manifests = list_manifest_files_in_bucket(bucket)
    for manifest in manifests:
        contents = get_file_handle(bucket, manifest['Key'])
        yield json.loads(contents.read().decode('utf-8'))

def generate_manifests(bucket):
    return {manifest['dump_id']: manifest
            for manifest in get_s3_manifest_file_contents(bucket)}

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
        continuation_args = args.copy()
        continuation_args['ContinuationToken'] = next_continuation_token

        result = s3_client.list_objects_v2(**continuation_args)

        s3_objects += result['Contents']
        next_continuation_token = result.get('NextContinuationToken')

    matcher = re.compile(".*.json")
    for s3_object in s3_objects:
        if matcher.search(s3_object['Key']):
            yield s3_object

def copy_s3_file(file, old_dump_id, new_dump_id):
    file_without_proto = file.replace('s3://', '')
    bucket, *s3_filepath = file_without_proto.split('/')
    s3_filepath = '/'.join(s3_filepath)
    tmp_file = '/tmp/download_{random.randint(10000,99999)}'
    new_s3_filepath = s3_filepath.replace(f'sync_{old_dump_id}', f'sync_{new_dump_id}')

    s3_client = boto3.client('s3')
    s3_client.download_file(bucket, s3_filepath, tmp_file)
    s3_client.upload_file(tmp_file, bucket, new_s3_filepath)
    os.remove(tmp_file)

    return f's3://{bucket}/{new_s3_filepath}'

def map_table(table, old_dump_id, new_dump_id):
    return {
        **table,
        'files': list(map(lambda file: copy_s3_file(file, old_dump_id, new_dump_id), table.get('files', [])))
    }

def copy_latest_manifest_to_new_dump_id(bucket):
    manifests = generate_manifests(bucket)

    old_dump_id = max(manifests)
    old_manifest = manifests[old_dump_id]

    new_dump_id = old_dump_id + 1
    new_manifest = {
        **old_manifest,
        'dump_id': new_dump_id,
        'tables': list(map(lambda table: map_table(table, old_dump_id, new_dump_id),
                           old_manifest.get('tables', [])))
    }

    tmp_file = '/tmp/upload_{random.randint(10000,99999)}'
    with open(tmp_file, 'w') as f:
        json.dump(new_manifest, f)

    s3_client = boto3.client('s3')
    s3_client.upload_file(tmp_file, bucket, f'manifests/sync_{new_dump_id}.json')
    os.remove(tmp_file)

    return new_dump_id

def delete_dump(bucket, dump_id):
    s3_client = boto3.resource('s3')
    s3_bucket = s3_client.Bucket(bucket)
    s3_bucket.objects.filter(Prefix=f'sync_{dump_id}').delete()
    s3_client.Object(bucket, f'manifests/sync_{dump_id}.json').delete()
