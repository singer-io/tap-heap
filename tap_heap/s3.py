import re
import backoff
import boto3
import singer

from botocore.credentials import (
    AssumeRoleCredentialFetcher,
    CredentialResolver,
    DeferredRefreshableCredentials,
    RefreshableCredentials,
    JSONFileCache
)
from botocore.exceptions import ClientError
from botocore.session import Session
from urllib3.exceptions import ReadTimeoutError

LOGGER = singer.get_logger()

def retry_pattern():
    return backoff.on_exception(backoff.expo,
                                (ClientError, ReadTimeoutError),
                                max_tries=5,
                                on_backoff=log_backoff_attempt,
                                factor=10)


def log_backoff_attempt(details):
    LOGGER.info("Error detected communicating with Amazon, triggering backoff: %d try",
                details.get("tries"))


class AssumeRoleProvider():
    METHOD = 'assume-role'

    def __init__(self, fetcher):
        self._fetcher = fetcher

    def load(self):
        return DeferredRefreshableCredentials(
            self._fetcher.fetch_credentials,
            self.METHOD
        )


@retry_pattern()
def setup_aws_client(config):
    role_arn = f"arn:aws:iam::{config['account_id'].replace('-', '')}:role/{config['role_name']}"
    session = Session()
    fetcher = AssumeRoleCredentialFetcher(
        session.create_client,
        session.get_credentials(),
        role_arn,
        extra_args={
            'DurationSeconds': 3600,
            'RoleSessionName': 'TapHeap',
            'ExternalId': config['external_id']
        },
        cache=JSONFileCache()
    )

    refreshable_session = Session()
    refreshable_session.register_component(
        'credential_provider',
        CredentialResolver([AssumeRoleProvider(fetcher)])
    )

    LOGGER.info("Attempting to assume_role on RoleArn: %s", role_arn)
    boto3.setup_default_session(botocore_session=refreshable_session)


@retry_pattern()
def setup_aws_client_with_proxy(config):
    # pylint: disable=line-too-long
    proxy_role_arn = f"arn:aws:iam::{config['proxy_account_id'].replace('-', '')}:role/{config['proxy_role_name']}"
    cust_role_arn = f"arn:aws:iam::{config['account_id'].replace('-', '')}:role/{config['role_name']}"
    credentials_cache_path = config.get("credentials_cache_path", JSONFileCache.CACHE_DIR)
    # Step 1: Assume Role in Account Proxy and set up refreshable session
    session_proxy = Session()
    fetcher_proxy = AssumeRoleCredentialFetcher(
        client_creator=session_proxy.create_client,
        source_credentials=session_proxy.get_credentials(),
        role_arn=proxy_role_arn,
        extra_args={
            'DurationSeconds': 3600,
            'RoleSessionName': 'ProxySession'
        },
        cache=JSONFileCache(credentials_cache_path)
    )

    # Refreshable credentials for Account Proxy
    refreshable_credentials_proxy = RefreshableCredentials.create_from_metadata(
        metadata=fetcher_proxy.fetch_credentials(),
        refresh_using=fetcher_proxy.fetch_credentials,
        method="sts-assume-role"
    )

    # Step 2: Use Proxy Account's session to assume Role in Customer Account
    session_cust = Session()
    fetcher_cust = AssumeRoleCredentialFetcher(
        client_creator=session_cust.create_client,
        source_credentials=refreshable_credentials_proxy,
        role_arn=cust_role_arn,
        extra_args={
            'DurationSeconds': 3600,
            'RoleSessionName': 'TapHeapCustSession',
            'ExternalId': config['external_id']
        },
        cache=JSONFileCache(credentials_cache_path)
    )

    # Set up refreshable session for Customer Account
    refreshable_session_cust = Session()
    refreshable_session_cust.register_component(
        'credential_provider',
        CredentialResolver([AssumeRoleProvider(fetcher_cust)])
    )

    LOGGER.info("Attempting to assume_role on RoleArn: %s", cust_role_arn)
    boto3.setup_default_session(botocore_session=refreshable_session_cust)


@retry_pattern()
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
        LOGGER.info("Found %s files.", len([o for o in s3_objects if o["Key"] != "manifests/"]))
    else:
        LOGGER.warning('Found no Manifest files for bucket "%s"', bucket)

    matcher = re.compile(".*.json")
    for s3_object in s3_objects:
        if matcher.search(s3_object['Key']):
            yield s3_object


@retry_pattern()
def get_file_handle(bucket, s3_path):
    s3_client = boto3.resource('s3')

    s3_bucket = s3_client.Bucket(bucket)
    s3_object = s3_bucket.Object(s3_path)
    return s3_object.get()['Body']
