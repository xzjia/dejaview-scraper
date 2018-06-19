import os
import re
import json
import random
import logging

from datetime import date, datetime

from wikipedia import page, PageError, DisambiguationError
from requests_html import HTMLSession


WIKI_ENTRY = 'https://en.wikipedia.org/wiki/List_of_historical_anniversaries'
SPLIT_HYPHEN = '-|–|－'
EVENTS_INDEX = 1
BIRTHS_INDEX = 2


class OneWikiDay(object):
    def __init__(self, one_date_wiki_url):
        self.logger = logging.getLogger(
            'daily_collector.{}'.format(self.__class__.__name__))
        # Adding 2020 is a workaround for strptime default to 1900, which is not a leap year
        obj_date = datetime.strptime(
            '2020_'+one_date_wiki_url.split('/').pop(), '%Y_%B_%d')
        self.date_without_year = '{}-{}'.format(obj_date.month, obj_date.day)
        self.data = self.get_one_date(one_date_wiki_url)
        self.logger.info('Populated {:5>} entries for {:5>}'.format(
            len(self.data), self.date_without_year))

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
        result.extend(self.process_one_list(
            all_uls[EVENTS_INDEX+offset].find('li'), 'events', self.date_without_year))
        result.extend(self.process_one_list(
            all_uls[BIRTHS_INDEX+offset].find('li'), 'births', self.date_without_year))
        return result

    def process_one_list(self, list, type, date_without_year):
        if type == 'events':
            return self.process_events(list, date_without_year)
        elif type == 'births':
            return self.process_events(list, date_without_year, ' was born on this day.')
        else:
            return []

    def get_text_image_link(self, event, year):
        def make_text(dic):
            if dic.keys():
                return 'Learn more: ' + ', '.join(['''<a href="{}">{}</a>'''
                                                   .format(dic[key]['link'], key) for key in dic])
            else:
                return event.text
        result = {}
        for link in event.find('a'):
            if 'title' in link.attrs and 'href' in link.attrs and link.attrs['title'] != year:
                result[link.attrs['title']] = {
                    'link': link.absolute_links.pop()}
        if year > '1990':
            for key in result:
                try:
                    wiki = page(key)
                    imgs = [img for img in wiki.images if 'svg' not in img]
                    if imgs:
                        image_url = random.choice(imgs)
                        self.logger.debug(
                            'Processed {} -- {}'.format(self.date_without_year, event.text))
                        return make_text(result), image_url
                except (PageError, DisambiguationError) as e:
                    self.logger.error(
                        '{} when processing {} -- {} -- {}'.format(type(e).__name__, self.date_without_year, event.text, key))
                    pass
            return make_text(result), ''
        else:
            return make_text(result), ''

    def process_events(self, events_list, date_without_year, postfix=''):
        def event_2_dict(event):
            if event.find('ul'):
                self.logger.debug('Error: Nested list of {} {} '.format(date_without_year,
                                                                        event.text))
                return None
            splitted = re.split(SPLIT_HYPHEN, event.text, maxsplit=1)
            if len(splitted) < 2:
                self.logger.debug('Error: No hyphen of {} {} '.format(date_without_year,
                                                                      event.text))
                return None
            result = {}
            year = splitted[0].strip()
            desc = splitted[1].strip()
            string_date = year + '-' + date_without_year
            try:
                date_obj = datetime.strptime(string_date, '%Y-%m-%d')
                result['date'] = date_obj.strftime('%Y-%m-%d')
            except ValueError:
                self.logger.debug(
                    "Error when parsing {}, maybe because it's too old".format(string_date))
                return None
            event_text, event_image_link = self.get_text_image_link(
                event, year)
            result['title'] = desc + postfix
            result['text'] = event_text
            result['link'] = ''
            result['image_link'] = event_image_link
            result['media_link'] = ''
            return result
        return filter(lambda e: e, map(event_2_dict, events_list))


class Wikipedia(object):
    def __init__(self):
        self.logger = logging.getLogger(
            'daily_collector.{}'.format(self.__class__.__name__))
        self.label_name = 'Wikipedia'
        self.target_date = date.today().strftime('%Y-%m-%d')

        self.all_links = self.get_date_links()
        self.data = {}
        for single_link in self.all_links:
            # for single_link in random.sample(self.all_links, 5):
            # for single_link in ['https://en.wikipedia.org/wiki/October_30']:
            w = OneWikiDay(single_link)
            self.data[w.date_without_year] = w.data
        self.events = [event for one_day in self.data.values()
                       for event in one_day]
        with open("2018-06-19.json", 'w') as w:
            json.dump(self.events, w, indent=2)

    def get_date_links(self):
        session = HTMLSession()
        r = session.get(WIKI_ENTRY)
        nav = r.html.find('.navbox-list')
        return set.union(*map(lambda one_month: one_month.absolute_links, nav))

    def already_same(self, existing_event, row):
        return existing_event['link'] == row['link'] \
            and existing_event['image_link'] == row['image_link'] \
            and existing_event['media_link'] == row['media_link'] \
            and existing_event['text'] == row['text']

    def map_json_array_to_rows(self, json_array, label_id):
        result = []
        for jsevt in json_array:
            try:
                result.append({
                    'timestamp': jsevt['date'],
                    'title': jsevt['title'],
                    'text': jsevt['text'],
                    'link': jsevt['link'],
                    'label_id': label_id,
                    'image_link':  jsevt['image_link'],
                    'media_link':  jsevt['media_link']
                })
            except Exception as exception:
                self.logger.error('Something unexpected happened: {} {}'.format(
                    type(exception).__name__,
                    self.target_date))
        return result

    def store_s3(self, s3_bucket):
        # Pick the right name for json files.
        if len(self.events) > 0:
            s3_bucket.Object(key='{}/{}.json'.format(self.label_name,
                                                     self.target_date)).put(Body=self.chart.json())
            self.logger.info('Successfully stored {} {} events into S3'.format(
                self.target_date, len(self.events)))
        else:
            self.logger.warn(
                '***** No data for {} so skipping... '.format(self.target_date))

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


def main():
    w = Wikipedia()


if __name__ == '__main__':
    main()
