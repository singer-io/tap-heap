import json
import sys
import singer

from singer import metadata
from tap_heap import manifest
from tap_heap import s3
from tap_heap.discover import discover_streams
from tap_heap.sync import sync_stream

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = ["start_date", "bucket", "account_id", "external_id", "role_name"]

def do_discover(config):
    LOGGER.info("Starting discover")
    streams = discover_streams(config['bucket'])
    if not streams:
        raise Exception("No streams found")
    catalog = {"streams": streams}
    json.dump(catalog, sys.stdout, indent=2)
    LOGGER.info("Finished discover")


def stream_is_selected(mdata):
    return mdata.get((), {}).get('selected', False)


def do_sync(config, catalog, state):
    LOGGER.info('Starting sync.')

    bucket = config['bucket']
    merged_manifests = manifest.generate_merged_manifests(bucket)

    for stream in catalog['streams']:
        stream_name = stream['tap_stream_id']
        mdata = metadata.to_map(stream['metadata'])

        if not stream_is_selected(mdata):
            LOGGER.info("%s: Skipping - not selected", stream_name)
            continue

        manifest_table = next(m for name, m in merged_manifests.items() if name == stream_name)
        if not manifest_table:
            LOGGER.info("Selected table not found in manifests. Skipping")
            continue

        singer.write_state(state)
        LOGGER.info("%s: Starting sync", stream_name)
        counter_value = sync_stream(bucket, state, stream, manifest_table)
        LOGGER.info("%s: Completed sync (%s rows)", stream_name, counter_value)

    LOGGER.info('Done syncing.')


@singer.utils.handle_top_exception(LOGGER)
def main():
    args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)

    s3.setup_aws_client(args.config)

    if args.discover:
        do_discover(args.config)
    elif args.properties:
        do_sync(args.config, args.properties, args.state)

if __name__ == '__main__':
    main()
