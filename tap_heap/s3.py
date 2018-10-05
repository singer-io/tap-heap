import re
import backoff
import boto3
import botocore
import singer

LOGGER = singer.get_logger()

def retry_pattern():
    return backoff.on_exception(backoff.expo,
                                botocore.exceptions.ClientError,
                                max_tries=5,
                                on_backoff=log_backoff_attempt,
                                factor=10)


def log_backoff_attempt(details):
    LOGGER.info("Error detected communicating with Amazon, triggering backoff: %d try",
                details.get("tries"))


@retry_pattern()
def setup_aws_client(config):
    client = boto3.client('sts')
    role_arn = "arn:aws:iam::{}:role/{}".format(config['account_id'].replace('-', ''),
                                                config['role_name'])

    LOGGER.info("Attempting to assume_role on RoleArn: %s", role_arn)
    role = client.assume_role(RoleArn=role_arn,
                              ExternalId=config['external_id'],
                              RoleSessionName='TapHeap')
    boto3.setup_default_session(aws_access_key_id=role['Credentials']['AccessKeyId'],
                                aws_secret_access_key=role['Credentials']['SecretAccessKey'],
                                aws_session_token=role['Credentials']['SessionToken'])


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
