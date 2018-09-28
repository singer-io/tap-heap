import json

from tap_heap import s3

def generate_merged_manifests(bucket):
    tables = []
    manifests = s3.list_manifest_files_in_bucket(bucket)
    for manifest in manifests:
        contents = s3.get_file_handle(bucket, manifest['Key'])
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
    # for table_key in right.keys():
    #     if type(left[table_key]) == dict and type(right[table_key]) == dict:
    #         for key in right[table_key].keys():
    #             if type(left[table_key][key]) == set and type(right[table_key][key]) == set:
    #                 merged[table_key][key] = merged[table_key][key] | right[table_key][key]

    for table_key, table_value in right.items():
        if type(left[table_key]) == dict and type(table_value) == dict:
            for key, value in table_value.items():
                 if type(left[table_key][key]) == set and type(table_value[key]) == set:
                     merged[table_key][key] = merged[table_key][key] | table_value[key]

    return merged
