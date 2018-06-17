import os
import sys
import logging
import json
import time
from datetime import datetime, timedelta, date

import requests
import boto3
import sqlalchemy as sa


BUCKET_NAME = 'lifeline-cc4-jia'
NYT_ARTICLE_SEARCH_EP = 'https://api.nytimes.com/svc/search/v2/articlesearch.json'

logger = logging.getLogger('daily_collector')
for h in logger.handlers:
    logger.removeHandler(h)
 
h = logging.StreamHandler(sys.stdout)
FORMAT = '%(levelname)6s %(asctime)s [%(name)30s - %(funcName)20s] %(message)s'
h.setFormatter(logging.Formatter(FORMAT))
logger.addHandler(h)
 
logger.setLevel(logging.INFO)

s3 = boto3.resource("s3").Bucket(BUCKET_NAME)

class Database(object):
    def __init__(self):
        self.logger = logging.getLogger('daily_collector.Database')
        self.conn = self.get_db_conn()
        self.logger.info('Connected to  {}'.format(os.environ['DATABASE_URL'].split('@').pop()))
        self.label_table = sa.table('label', sa.column(
            'id', sa.Integer), sa.column('name', sa.Text))
        self.event_table = sa.table('event',
                    sa.column('timestamp', sa.DateTime),
                    sa.column('title', sa.Text),
                    sa.column('text', sa.Text),
                    sa.column('link', sa.Text),
                    sa.column('label_id', sa.Integer),
                    sa.column('image_link', sa.Text),
                    sa.column('media_link', sa.Text)
                    )

    def get_db_conn(self):
        engine = sa.create_engine(os.environ['DATABASE_URL'], echo=False)
        return engine.connect()

    def get_label_id_from_name(self, name):
        s = sa.sql.select([self.label_table.c.id, self.label_table.c.name]
                        ).where(self.label_table.c.name == name)
        result = self.conn.execute(s)
        if result.rowcount < 1:
            ins = self.label_table.insert().values(name=name)
            self.conn.execute(ins)
        result = self.conn.execute(s)
        return result.fetchone()['id']

    def map_json_array_to_rows(self, label_name, json_array, json_key, label_id):
        result = []
        if label_name == 'New-York-Times':
            for jsevt in json_array:
                try:
                    result.append({
                        'timestamp': jsevt['pub_date'],
                        'title': jsevt['headline'].get('print_headline', jsevt['headline']['main']),
                        'text': jsevt['snippet'],
                        'link': jsevt['web_url'],
                        'label_id': label_id,
                        'image_link': 'https://www.nytimes.com/' + next(filter(lambda e: e['subtype'] == 'thumbnail', jsevt['multimedia']))['url'],
                        'media_link': ''
                    })
                except Exception as exception:
                    logger.error('{:<25} {} {} {}'.format(
                        label_name,
                        json_key,
                        type(exception).__name__,
                        jsevt['pub_date']))
        return result

    def get_existing_events(self, label_id, target_timestamp, target_title):
        s = sa.sql.select([self.event_table.c.title, self.event_table.c.timestamp, self.event_table.c.link, self.event_table.c.text, self.event_table.c.image_link, self.event_table.c.media_link]).where(
            sa.and_(
                self.event_table.c.label_id == label_id,
                self.event_table.c.timestamp == target_timestamp,
                self.event_table.c.title == target_title
            )
        )
        result = self.conn.execute(s)
        return result

class NYT(object):
    def __init__(self, target_date):
        self.key_prefix='New-York-Times'
        self.logger = logging.getLogger('daily_collector.NYT')
        self.current_key_index = 0
        self.api_key_pool = os.environ['NYT_API_KEYS'].split('_')
        self.error_count = 0
        self.target_date = target_date
        current_date = target_date
        self.events = []
        while current_date <= date.today():
            self.events.extend(self.process_one_day(current_date))
            current_date = current_date + timedelta(days=1)

    def already_same(self, existing_event, row):
        return existing_event['link'] == row['link'] and existing_event['image_link'] == row['image_link'] and existing_event['media_link'] == row['media_link'] and existing_event['text'] == row['text']


    def format_date(self, date_object, with_hyphen=False):
        if with_hyphen:
            return date_object.strftime('%Y-%m-%d')
        return date_object.strftime('%Y%m%d')

    def remove_duplicate(self, event_list):
        seen_article = set()
        result = []
        for event in event_list:
            title = event['headline']['print_headline']
            if title in seen_article:
                continue
            else:
                result.append(event)
                seen_article.add(title)
        return result

    def store_s3(self):
        if len(self.events) > 0:
            s3.Object(key='{}/{}.json'.format(self.key_prefix,
                                              self.format_date(self.target_date, with_hyphen=True))).put(Body=json.dumps(self.events, indent=2))
            self.logger.info('Successfully stored {} {} events into S3'.format(
                self.format_date(self.target_date), len(self.events)))
        else:
            self.logger.warn(
                '***** No data for {} so skipping... '.format(self.format_date(self.target_date)))

    def store_rds(self, db):
        label_id = db.get_label_id_from_name(self.key_prefix)
        already_same_count = 0
        update_count = 0
        rows = db.map_json_array_to_rows(self.key_prefix, self.events, self.format_date(self.target_date, with_hyphen=True), label_id)
        inserts = []
        for row in rows:
            existing_event = db.get_existing_events(
                label_id, row['timestamp'], row['title']).fetchone()
            if existing_event and len(existing_event) > 0:
                if not self.already_same(existing_event, row):
                    update = db.event_table.update().values(text=row['text'], media_link=row['media_link'], link=row['link'], image_link=row['image_link']).where(sa.and_(
                        db.event_table.c.label_id == label_id,
                        db.event_table.c.timestamp == row['timestamp'],
                        db.event_table.c.title == row['title']
                    ))
                    update_count += 1
                    db.conn.execute(update)
                else:
                    already_same_count += 1
            else:
                inserts.append(row)
        if len(inserts) > 0:
            ins = db.event_table.insert().values(inserts)
            db.conn.execute(ins)
        self.logger.info('{} Total from json:{:>5} Inserted: {:>5} Up-to-date: {:>5} Updated: {:>5}'.format(
            self.target_date,
            len(rows),
            len(inserts),
            already_same_count,
            update_count
        ))

    def process_one_day(self, date):
        result = self.get_one_day(date)
        result = self.remove_duplicate(result)
        return result

    def get_one_day(self, target_date):
        result = []
        one_batch = self.get_one_batch(target_date)
        num_pages = one_batch['response']['meta']['hits'] // 10 + 1
        current_page = 0
        while (current_page < num_pages):
            try:
                self.logger.info('Processing for {}, progress of pages: {}/{}'.format(
                    self.format_date(target_date), current_page+1, num_pages))
                current_page += 1
                result.extend(one_batch['response']['docs'])
                time.sleep(1)
                one_batch = self.get_one_batch(
                    target_date, page_number=current_page)
            except:
                self.logger.warn(
                    'Something unexpected happened and returning the results up to this point')
                return result
        return result

    def retry_api_call(self, endpoint, payload):
        self.error_count += 1
        if self.error_count == 10:
            self.logger.warn(
                'Current key burned out, switching to next one and retrying... ')
            self.current_key_index += 1
            if self.current_key_index == len(self.api_key_pool):
                self.current_key_index = 0
            self.error_count = 0
        self.logger.warn("API Rate exceed error, sleep 2 seconds and retry")
        time.sleep(2)
        payload['api-key'] = self.api_key_pool[self.current_key_index]
        return requests.get(endpoint, params=payload).json()

    def get_one_batch(self, target_date, page_number=0):
        end_date = target_date + timedelta(days=1)
        payload = {
            'api-key': self.api_key_pool[self.current_key_index],
            'begin_date': self.format_date(target_date),
            'end_date': self.format_date(end_date),
            'page': page_number,
            'fq': 'print_page:1'
        }
        raw_response = requests.get(
            NYT_ARTICLE_SEARCH_EP, params=payload).json()
        while 'response' not in raw_response:
            raw_response = self.retry_api_call(NYT_ARTICLE_SEARCH_EP, payload)
        self.error_count = 0
        return raw_response


def lambda_handler(event, context):

    db = Database()

    logger.info('Collecting NYT articles...')
    target_date = date.today() - timedelta(days=2)
    nyt = NYT(target_date)
    nyt.store_s3()
    nyt.store_rds(db)
    return '{} NYT events handled successfully'.format(len(nyt.events))


if __name__ == '__main__':
    lambda_handler({}, None)
