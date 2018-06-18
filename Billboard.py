import os
import json
import logging

import requests
import billboard

YOUTUBE_API = 'https://www.googleapis.com/youtube/v3/search'
YOUTUBE_LINK_PREFIX = 'https://www.youtube.com/watch?v='
YOUTUBE_SEARCH_PREFIX = 'https://www.youtube.com/results?search_query='


class Billboard(object):
    def __init__(self):
        self.chart = billboard.ChartData('hot-100')
        self.label_name = 'Billboard'
        self.target_date = self.chart.date
        self.logger = logging.getLogger(
            'daily_collector.{}'.format(self.__class__.__name__))
        self.events = [self.chart[0]]
        self.multimedia_link_memo = {}

    def get_query(self, title, artist):
        raw = title + '+' + artist
        return raw.replace(' ', '+')

    def get_media_link(self, title, artist):
        query = self.get_query(title, artist)
        if query in self.multimedia_link_memo:
            return self.multimedia_link_memo[query]
        payload = {
            'q': query,
            'maxResult': 5,
            'key': os.environ['YOUTUBE_API_KEY'],
            'part': 'snippet'
        }
        items = requests.get(YOUTUBE_API, params=payload).json()['items']
        videos = list(
            filter(lambda x: x['id']['kind'] == 'youtube#video', items))
        if len(videos) > 0:
            winner = videos[0]
            image_link = winner['snippet']['thumbnails']['default']['url']
            media_link = YOUTUBE_LINK_PREFIX + winner['id']['videoId']
        else:
            image_link = ''
            media_link = ''
        self.multimedia_link_memo[query] = (image_link, media_link)
        return image_link, media_link

    def already_same(self, existing_event, row):
        return existing_event['link'] == row['link'] \
            and existing_event['image_link'] == row['image_link'] \
            and existing_event['media_link'] == row['media_link'] \
            and existing_event['text'] == row['text']

    def map_json_array_to_rows(self, json_array, label_id):
        result = []
        for jsevt in json_array:
            try:
                image_link, media_link = self.get_media_link(
                    jsevt.title, jsevt.artist)
                result.append({
                    'timestamp': self.target_date,
                    'title': 'Billboard Hot 100 #1 Song: {} by {}'.format(jsevt.title, jsevt.artist),
                    'text': "{} was on the Billboard charts for {} weeks.".format(jsevt.title, str(jsevt.weeks)),
                    'link': YOUTUBE_SEARCH_PREFIX + self.get_query(jsevt.title, jsevt.artist),
                    'label_id': label_id,
                    'image_link':  image_link,
                    'media_link':  media_link
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
    # Some unit tests
    b = Billboard()
    print(b.events)
    print(b.map_json_array_to_rows(b.events, 5))


if __name__ == '__main__':
    main()
