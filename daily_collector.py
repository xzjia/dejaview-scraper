import os
import sys
import logging
from datetime import timedelta, date

import boto3

from Database import Database
from NYT import NYT

h = logging.StreamHandler(sys.stdout)
h.setFormatter(logging.Formatter(
    '%(levelname)8s %(asctime)s [%(name)30s - %(funcName)20s] %(message)s'))


logger = logging.getLogger('daily_collector')
logger.addHandler(h)
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    s3 = boto3.resource("s3").Bucket(os.environ['BUCKET_NAME'])
    db = Database()

    logger.info('Collecting NYT articles...')
    target_date = date.today() - timedelta(days=2)
    nyt = NYT(target_date, s3)
    nyt.store_s3()
    nyt.store_rds(db)
    return '{} NYT events handled successfully'.format(len(nyt.events))


if __name__ == '__main__':
    lambda_handler({}, None)
