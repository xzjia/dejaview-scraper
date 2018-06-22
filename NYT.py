import os
import re
import json
import logging
import time
from datetime import timedelta, date

import requests

NYT_ARTICLE_SEARCH_EP = 'https://api.nytimes.com/svc/search/v2/articlesearch.json'
FILTER_WORDS = ['-- No Title$']


class NYT(object):
    def __init__(self):
        self.label_name = 'New-York-Times'
        self.logger = logging.getLogger(
            'daily_collector.{}'.format(self.__class__.__name__))

        self.current_key_index = 0
        self.api_key_pool = os.environ['NYT_API_KEYS'].split('_')
        self.error_count = 0

        self.target_date = date.today() - timedelta(days=2)
        cursor_date = self.target_date
        self.events = []
        while cursor_date <= date.today():
            self.events.extend(self.process_one_day(cursor_date))
            cursor_date = cursor_date + timedelta(days=1)

    def already_same(self, existing_event, row):
        return existing_event['link'] == row['link'] \
            and existing_event['image_link'] == row['image_link'] \
            and existing_event['media_link'] == row['media_link'] \
            and existing_event['text'] == row['text']

    def format_date(self, date_object, with_hyphen=False):
        if with_hyphen:
            return date_object.strftime('%Y-%m-%d')
        return date_object.strftime('%Y%m%d')

    def get_title_from_event(self, event):
        try:
            candidate = event['headline'].get(
                'print_headline', event['headline']['main'])
        except KeyError:
            self.logger.error('No title found for this event {}'.format(event))
            return None
        if len(candidate) == 0:
            return None
        for bad_word in FILTER_WORDS:
            match = re.search(bad_word, candidate)
            if match:
                return None
        return candidate

    def map_json_array_to_rows(self, json_array, label_id):
        result = []
        for jsevt in json_array:
            try:
                title = self.get_title_from_event(jsevt)
                if title:
                    result.append({
                        'timestamp': jsevt['pub_date'],
                        'title': title,
                        'text': jsevt['snippet'],
                        'link': jsevt['web_url'],
                        'label_id': label_id,
                        'image_link': 'https://www.nytimes.com/' + next(filter(lambda e: e['subtype'] == 'thumbnail', jsevt['multimedia']))['url'],
                        'media_link': ''
                    })
            except Exception as exception:
                self.logger.error('{} {}'.format(
                    jsevt['pub_date'], type(exception).__name__))
        return result

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

    def store_s3(self, s3_bucket):
        if len(self.events) > 0:
            s3_bucket.Object(key='{}/{}.json'.format(self.label_name,
                                                     self.format_date(self.target_date, with_hyphen=True))).put(Body=json.dumps(self.events, indent=2))
            self.logger.info('Successfully stored {} {} events into S3'.format(
                self.format_date(self.target_date), len(self.events)))
        else:
            self.logger.warn(
                '***** No data for {} so skipping... '.format(self.format_date(self.target_date)))

    def store_rds(self, db):
        label_id = db.get_label_id_from_name(self.label_name)
        rows = self.map_json_array_to_rows(self.events, label_id)
        no_inserts, no_updates, no_notouch = db.store_rds(
            rows, label_id, self.already_same)
        self.logger.info('{} Total from json:{:>5} Inserted: {:>5} Updated: {:>5} Up-to-date: {:>5}'.format(
            self.target_date,
            len(rows),
            no_inserts,
            no_updates,
            no_notouch
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


def main():
    nyt = NYT()
    print(len(nyt.events))


if __name__ == '__main__':
    main()
