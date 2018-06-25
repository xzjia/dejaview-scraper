import os
import re
import json
import random
import logging

from datetime import date, datetime

import stopit
from wikipedia import page, PageError, DisambiguationError
from requests_html import HTMLSession


WIKI_ENTRY = 'https://en.wikipedia.org/wiki/List_of_historical_anniversaries'
SPLIT_HYPHEN = '-|–|－'
EVENTS_INDEX = 1
BIRTHS_INDEX = 2
IMAGE_YEAR_CUTOFF = '1990'


class WikiEvent(object):
    def __init__(self, event, date_without_year, suffix=''):
        self.logger = logging.getLogger(
            'daily_collector.{}'.format(self.__class__.__name__))
        self.event = event
        self.data = {}
        if event.find('ul'):
            self.logger.debug('Error: Nested list of {} {} '.format(date_without_year,
                                                                    event.text))
            raise ValueError
        splitted = re.split(SPLIT_HYPHEN, event.text, maxsplit=1)
        if len(splitted) < 2:
            self.logger.debug('Error: No hyphen of {} {} '.format(date_without_year,
                                                                  event.text))
            raise ValueError
        self.year = splitted[0].strip()
        desc = re.sub(r'\[\d+\]', '', splitted[1].strip())
        date_obj = datetime.strptime(
            self.year + '-' + date_without_year, '%Y-%m-%d')
        self.data['date'] = date_obj.strftime('%Y-%m-%d')
        self.data['title'] = desc + suffix

    def get_string(self):
        return self.data['date'] + '_' + self.data['title']

    def add_text_and_image(self):
        event_text, event_image_link = self.get_text_image_link()
        self.data['text'] = event_text
        if event_image_link:
            self.data['image_link'] = event_image_link

    @stopit.threading_timeoutable(default=None)
    def get_image_from_links(self, links_dict, event_string):
        def is_good_img(img_url):
            return all([bad_ext not in img_url for bad_ext in ['svg', 'webm']])
        for key in links_dict:
            try:
                wiki = page(key)
                if wiki and wiki.images:
                    imgs = [img for img in wiki.images if is_good_img(img)]
                    image_url = random.choice(imgs) if imgs else ''
                    self.logger.debug(
                        'Processed {} -- {}'.format(self.data['date'], event_string))
                    return image_url
            except (PageError, DisambiguationError) as e:
                self.logger.debug(
                    '{} when processing {} -- {} -- {}'.format(type(e).__name__, self.date_without_year, event_string, key))
            except stopit.TimeoutException:
                self.logger.warn(
                    'Timeout for *{}* when processing {}'.format(key, event_string))
            except:
                self.logger.warn(
                    'Unexpected exception for *{}* when processing {}'.format(key, event_string))
        return None

    def get_text_image_link(self):
        def make_text(dic):
            if dic.keys():
                return 'Learn more: ' + ', '.join(['''<a href="{}">{}</a>'''
                                                   .format(dic[key]['link'], key) for key in dic])
            else:
                return self.event.text
        result = {}
        for link in self.event.find('a'):
            if 'title' in link.attrs and 'href' in link.attrs and link.attrs['title'] != self.year:
                result[link.attrs['title']] = {
                    'link': link.absolute_links.pop()}
        self.data['text'] = make_text(result)
        image_url = self.get_image_from_links(
            result, self.event.text, timeout=5)
        self.data['image_link'] = image_url


class OneWikiDay(object):
    def __init__(self, one_date_wiki_url, cached_result):
        self.logger = logging.getLogger(
            'daily_collector.{}'.format(self.__class__.__name__))

        # Adding 2020 is a workaround for strptime default to 1900, which is not a leap year
        obj_date = datetime.strptime(
            '2020_'+one_date_wiki_url.split('/').pop(), '%Y_%B_%d')
        self.date_without_year = '{}-{}'.format(obj_date.month, obj_date.day)
        self.cache_date = self.get_cache_date(
            cached_result[self.date_without_year])
        self.data = self.get_one_date(one_date_wiki_url)
        self.logger.info('Populated {:5>} entries for {:5>}'.format(
            len(self.data), self.date_without_year))

    def already_cached(self, event):
        return event.get_string() in self.cache_date

    def get_cache_date(self, dic_list):
        return set(['_'.join([e['date'], e['title']]) for e in dic_list])

    def get_one_date(self, one_date_wiki_url):
        result = []
        session = HTMLSession()
        r = session.get(one_date_wiki_url)
        all_uls = r.html.find('ul')
        if '2 Events' in all_uls[0].text:
            # This is a workaround for January 1: https://en.wikipedia.org/wiki/January_1
            offset = 1
        elif '3 Events' in all_uls[0].text:
            # This is a workaround for February 29: https://en.wikipedia.org/wiki/February_29
            offset = 2
        else:
            assert '1 Events' in all_uls[0].text
            offset = 0
        result.extend(self.process_events(
            all_uls[EVENTS_INDEX+offset].find('li'), self.date_without_year))
        result.extend(self.process_events(
            all_uls[BIRTHS_INDEX+offset].find('li'), self.date_without_year, suffix=' was born on this day.'))
        return result

    def process_events(self, events_list, date_without_year, suffix=''):
        result = []
        for e in events_list:
            try:
                d = WikiEvent(e, date_without_year, suffix)
                if not self.already_cached(d):
                    d.get_text_image_link()
                    result.append(d.data)
            except ValueError:
                self.logger.debug('Exception when trying to parse {} {}'.format(
                    date_without_year, e.text))
        return result


class Wikipedia(object):
    def __init__(self, cached=None):
        self.logger = logging.getLogger(
            'daily_collector.{}'.format(self.__class__.__name__))
        self.label_name = 'Wikipedia'
        self.target_date = date.today().strftime('%Y-%m-%d')
        self.all_links = self.get_date_links()
        self.cached = cached
        self.data = {}
        # Due to the 300 seconds timeout limit on lambda, not all dates are examined at once
        sorted_links = sorted(self.all_links)
        target_links = sorted_links[:len(sorted_links)//2] if datetime.now(
        ).time().hour < 12 else sorted_links[len(sorted_links)//2:]
        for single_link in target_links:
            self.logger.info('About to process {} ...'.format(single_link))
            try:
                w = OneWikiDay(single_link, cached)
            except:
                self.logger.error(
                    '*********** Skipped ********* {}'.format(single_link))
                continue
            self.data[w.date_without_year] = w.data

    def store_json(self):
        # Only for development
        result = self.merge_cache_and_diff()
        with open("{}.json".format(self.target_date), 'w') as w:
            json.dump(result, w, indent=2)

    def get_date_links(self):
        session = HTMLSession()
        r = session.get(WIKI_ENTRY)
        nav = r.html.find('.navbox-list')
        return set.union(*map(lambda one_month: one_month.absolute_links, nav))

    def already_same(self, existing_event, row):
        return existing_event['image_link'] == row['image_link'] \
            and existing_event['text'] == row['text']

    def map_json_array_to_rows(self, json_array, label_id):
        result = []
        for jsevt in json_array:
            assert 'date' in jsevt and 'title' in jsevt and 'text' in jsevt and 'image_link' in jsevt
            try:
                result.append({
                    'timestamp': jsevt['date'],
                    'title': jsevt['title'],
                    'text': jsevt['text'],
                    'link': '',
                    'label_id': label_id,
                    'image_link':  jsevt['image_link'] if 'image_link' in jsevt else '',
                    'media_link':  ''
                })
            except Exception as exception:
                self.logger.error('Something unexpected happened: {} {}'.format(
                    type(exception).__name__,
                    self.target_date))
        return result

    def merge_cache_and_diff(self):
        for key in self.data:
            self.cached[key].extend(self.data[key])
        return self.cached

    def store_s3(self, s3_bucket):
        if len(self.data.keys()) > 0:
            all_events = self.merge_cache_and_diff()
            s3_bucket.Object(key='{}/{}.json'.format(self.label_name,
                                                     self.target_date)).put(Body=json.dumps(all_events, indent=2))
            self.logger.info('Successfully stored {} {} events into S3'.format(
                self.target_date, len(all_events.keys())))
        else:
            self.logger.warn(
                '***** No data for {} so skipping... '.format(self.target_date))

    def store_rds(self, db):
        events_list = [e for d in self.data for e in self.data[d]]
        label_id = db.get_label_id_from_name(self.label_name)
        rows = self.map_json_array_to_rows(events_list, label_id)
        no_inserts, no_updates, no_notouch = db.store_rds(
            rows, label_id, self.already_same)
        self.logger.info('Total from json:{:>5} Inserted: {:>5} Updated: {:>5} Up-to-date: {:>5}'.format(
            len(rows),
            no_inserts,
            no_updates,
            no_notouch
        ))


def main():
    with open('2018-06-21.json') as b21:
        cached = json.load(b21)
    w = Wikipedia(cached=cached)


if __name__ == '__main__':
    main()
