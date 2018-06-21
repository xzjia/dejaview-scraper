import os
import sys
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


def collect_wikipedia(s3_bucket, db):
    logger.info('Collecting Wikipedia ...')
    wikipedia = Wikipedia()
    for one_day in wikipedia.data:
        wikipedia.store_rds(db, wikipedia.data[one_day], one_day)


def lambda_handler(event, context):
    s3_bucket = boto3.resource("s3").Bucket(os.environ['BUCKET_NAME'])
    db = Database()
    collect_nyt(s3_bucket, db)
    collect_movies(s3_bucket, db)
    collect_billboard(s3_bucket, db)


if __name__ == '__main__':
    lambda_handler({}, None)
