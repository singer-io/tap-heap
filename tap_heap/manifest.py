import json

from tap_heap import s3

def generate_merged_manifests(bucket):
    return generate_and_merge_all_manifest_contents(
        bucket,
        get_s3_manifest_file_contents(bucket))


def generate_and_merge_all_manifest_contents(bucket, manifest_contents):
    tables = []
    for manifest_content in manifest_contents:
        tables.append(generate_manifest_tables(manifest_content))

    if not tables:
        raise Exception('Found no Manifest files in bucket: {}/manifests'.format(bucket))
    return merge_manifests(tables[0], tables[1:])


def get_s3_manifest_file_contents(bucket):
    manifests = s3.list_manifest_files_in_bucket(bucket)
    for manifest in manifests:
        contents = s3.get_file_handle(bucket, manifest['Key'])
        yield json.loads(contents.read().decode('utf-8'))


def generate_manifest_tables(manifest):
    manifest_table = {}
    for table in manifest['tables']:
        manifest_table[table['name']] = {
            "files": set(table['files']),
            "columns": set(table['columns']),
            "manifests": set([manifest['dump_id']]),
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
    """This is a deep merge implementation.

    It's called on manifest_table dicts with the intent of merging all of
    them into a superset of all of their contents. The tricky part is that
    the values need to be merged as well."""
    merged = left

    for table_key, table_value in right.items():
        if left.get(table_key):
            for key in table_value.keys():
                if left[table_key].get(key):
                    merged[table_key][key] = merged[table_key][key] | table_value[key]
        else:
            merged[table_key] = right[table_key]

    return merged
