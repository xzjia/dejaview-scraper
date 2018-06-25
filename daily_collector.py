import os
import sys
import json
import logging
from datetime import timedelta, date

import boto3

from Database import Database
from NYT import NYT
from Billboard import Billboard
from Wikipedia import Wikipedia
from Movies import Movies

h = logging.StreamHandler(sys.stdout)
h.setFormatter(logging.Formatter(
    '%(levelname)8s %(asctime)s [%(name)30s - %(funcName)20s] %(message)s'))


logger = logging.getLogger('daily_collector')
logger.addHandler(h)
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')
bucket_name = os.environ['BUCKET_NAME']
s3_bucket = boto3.resource("s3").Bucket(bucket_name)
db = Database()


def get_matching_s3_objects(bucket_name, prefix='', suffix=''):
    kwargs = {'Bucket': bucket_name}
    if isinstance(prefix, str):
        kwargs['Prefix'] = prefix
    while True:
        resp = s3.list_objects_v2(**kwargs)
        try:
            contents = resp['Contents']
        except KeyError:
            return
        for obj in contents:
            key = obj['Key']
            if key.startswith(prefix) and key.endswith(suffix):
                yield obj
        try:
            kwargs['ContinuationToken'] = resp['NextContinuationToken']
        except KeyError:
            break


def collect_nyt(s3_bucket, db):
    logger.info('Collecting NYT articles...')
    nyt = NYT()
    nyt.store_s3(s3_bucket)
    nyt.store_rds(db)
    logger.info('{} NYT events handled successfully'.format(len(nyt.events)))


def collect_billboard(s3_bucket, db):
    logger.info('Collecting Billboard events...')
    billboard = Billboard()
    billboard.store_s3(s3_bucket)
    billboard.store_rds(db)
    logger.info('{} Billboard events handled successfully'.format(
        len(billboard.events)))


def collect_movies(s3_bucket, db):
    logger.info('Collecting Movies ...')
    m = Movies(date(2018, 6, 15))
    if m.events:
        m.store_rds(db)
        m.store_s3(s3_bucket)
        logger.info(
            '{} Movies events handled successfully'.format(len(m.events)))
    else:
        logger.info('Nothing to do with this date for movies')


def get_most_recent(label_name):
    bucket_name = os.environ['BUCKET_NAME']
    objs = list(get_matching_s3_objects(
        bucket_name, prefix=label_name, suffix='json'))
    assert len(objs) > 0
    most_recent_key = max(objs, key=lambda o: o['Key'])['Key']
    logger.info('Loading Wikipedia cache {} from S3 ...'.format(most_recent_key))
    most_recent_obj = s3.get_object(Bucket=bucket_name, Key=most_recent_key)
    json_obj = json.load(most_recent_obj['Body'])
    return json_obj


def collect_wikipedia(s3_bucket, db):
    cached = get_most_recent('Wikipedia')
    logger.info('Loading currenet Wikipedia on this day pages ...')
    w = Wikipedia(cached=cached)
    w.store_rds(db)
    w.store_s3(s3_bucket)


def wikipedia_handler(event, context):
    collect_wikipedia(s3_bucket, db)


def lambda_handler(event, context):
    collect_nyt(s3_bucket, db)
    collect_movies(s3_bucket, db)
    collect_billboard(s3_bucket, db)


if __name__ == '__main__':
    wikipedia_handler({}, None)
