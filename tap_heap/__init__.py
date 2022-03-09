import json
import sys

import botocore

import singer
from singer import metadata

from tap_heap import manifest
from tap_heap import s3
from tap_heap.discover import discover_streams
from tap_heap.sync import sync_stream

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = ["start_date", "bucket"]

# One of the following is also required:
# Auth option 1: IAM Role
REQUIRED_CONFIG_KEYS_IAM_ROLE = ["account_id", "external_id", "role_name"]
# Auth option 2: account access keys
REQUIRED_CONFIG_KEYS_ACCESS_KEYS = ["aws_access_key_id", "aws_secret_access_key"]


def check_config_auth_keys(config, required_keys_access_keys, required_keys_iam_role):
    """Ensure that all related Auth keys are provided for at least one of the auth methods."""
    missing_keys_access_keys = [key for key in required_keys_access_keys if key not in config]
    missing_keys_iam_role = [key for key in required_keys_iam_role if key not in config]
    if missing_keys_access_keys and missing_keys_iam_role:
        raise Exception(
            "Config must contain {} if using access keys or {} if using IAM role".format(
                required_keys_access_keys, required_keys_iam_role
            )
        )


def do_discover(config):
    LOGGER.info("Starting discover")
    streams = discover_streams(config['bucket'])
    if not streams:
        raise Exception("No streams found")
    catalog = {"streams": streams}
    for stream in streams:
        LOGGER.info('Found stream %s', stream['tap_stream_id'])
    json.dump(catalog, sys.stdout, indent=2)
    LOGGER.info("Finished discover")


def stream_is_selected(mdata):
    return mdata.get((), {}).get('selected', False)

def manifest_contains_table(manifests, table_name):
    for mani in manifests.values():
        if table_name in mani:
            return True

    return False


def do_sync(config, catalog, state):
    LOGGER.info('Starting sync.')

    bucket = config['bucket']
    manifests = manifest.generate_manifests(bucket)

    for stream in catalog['streams']:
        stream_name = stream['tap_stream_id']
        mdata = metadata.to_map(stream['metadata'])

        if not stream_is_selected(mdata):
            LOGGER.info("%s: Skipping - not selected", stream_name)
            continue

        if not manifest_contains_table(manifests, stream_name):
            LOGGER.info("Selected table not found in manifests. Skipping")
            continue

        singer.write_state(state)
        LOGGER.info("%s: Starting sync", stream_name)
        counter_value = sync_stream(bucket, state, stream, manifests)
        LOGGER.info("%s: Completed sync (%s rows)", stream_name, counter_value)

    LOGGER.info('Done syncing.')


@singer.utils.handle_top_exception(LOGGER)
def main():
    args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)
    check_config_auth_keys(
        args.config,
        REQUIRED_CONFIG_KEYS_ACCESS_KEYS,
        REQUIRED_CONFIG_KEYS_IAM_ROLE
    )

    try:
        # This should never succeed in production. It exists solely for
        # development purposes where you can't actually assume the target
        # role but can nevertheless initialize your environment such that
        # you have access to the bucket.
        #
        # We disable expression-not-assigned because we're using this to
        # duck type whether we have access to the bucket or not.
        #
        # pylint: disable=expression-not-assigned
        next(s3.list_manifest_files_in_bucket(args.config['bucket']))
        LOGGER.warning("Able to access manifest files without assuming role!")
    except (botocore.exceptions.ClientError, botocore.exceptions.NoCredentialsError):
        s3.setup_aws_client(args.config)

    if args.discover:
        do_discover(args.config)
    elif args.properties:
        do_sync(args.config, args.properties, args.state)

if __name__ == '__main__':
    main()
