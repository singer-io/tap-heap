import json

from tap_heap import s3

def get_s3_manifest_file_contents(bucket):
    manifests = s3.list_manifest_files_in_bucket(bucket)
    for manifest in manifests:
        contents = s3.get_file_handle(bucket, manifest['Key'])
        yield json.loads(contents.read().decode('utf-8'))

def generate_manifests(bucket):
    # NB> We drop the `property_definitions` because we don't need it
    # This will generate our manifests in the structure:
    # manifests[dump_id][table_name] =
    #   {"files" ["file 1"],
    #    "incremental": True,
    #    "columns": ["column_1"]}
    manifests = {manifest['dump_id']: {table['name']: table for table in manifest['tables']}
                 for manifest in get_s3_manifest_file_contents(bucket)}

    return manifests
