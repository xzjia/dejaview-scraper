import os
import sys
import logging
from datetime import timedelta, date

import boto3

from Database import Database
from NYT import NYT
from Billboard import Billboard

h = logging.StreamHandler(sys.stdout)
h.setFormatter(logging.Formatter(
    '%(levelname)8s %(asctime)s [%(name)30s - %(funcName)20s] %(message)s'))


logger = logging.getLogger('daily_collector')
logger.addHandler(h)
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    s3_bucket = boto3.resource("s3").Bucket(os.environ['BUCKET_NAME'])
    db = Database()

    logger.info('Collecting NYT articles...')
    nyt = NYT()
    nyt.store_s3(s3_bucket)
    nyt.store_rds(db)
    logger.info('{} NYT events handled successfully'.format(len(nyt.events)))

    logger.info('Collecting Billboard articles...')
    billboard = Billboard()
    billboard.store_s3(s3_bucket)
    billboard.store_rds(db)
    logger.info('{} Billboard events handled successfully'.format(
        len(billboard.events)))


if __name__ == '__main__':
    lambda_handler({}, None)
