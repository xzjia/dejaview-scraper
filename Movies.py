#!/usr/bin/env python3
import os
import json
import logging
import datetime

import requests
from requests_html import HTMLSession
from MovieChart import MovieChart

YOUTUBE_API = 'https://www.googleapis.com/youtube/v3/search'
YOUTUBE_LINK_PREFIX = 'https://www.youtube.com/watch?v='
IMDB_SEARCH_PREFIX = 'https://www.imdb.com/find?q='


class Movies(object):
    def __init__(self, target_date=datetime.datetime.now()):
        self.target_date = target_date
        self.chart = MovieChart(self.target_date)
        self.label_name = 'Movies'
        self.logger = logging.getLogger(
            'daily_collector.{}'.format(self.__class__.__name__))
        self.events = self.get_top_movie()
        self.multimedia_link_memo = {}

    def get_top_movie(self):
        if len(self.chart.movies) < 1:
            return []
        else:
            return [self.chart.movies[0]]

    def get_query(self, title):
        raw = title + ' ' + str(self.target_date.year)
        title = re.sub(r'[^\w]', ' ', title)
        return title.replace(' ', '+')

    def get_media_link(self, title):
        query = self.get_query(title + " official movie trailer")
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
            filter(lambda x: x['id']['kind'] == 'youtube#video' and 'Trailer' in x['snippet']['title'], items))
        if len(videos) > 0:
            winner = videos[0]
            image_link = winner['snippet']['thumbnails']['high']['url']
            media_link = YOUTUBE_LINK_PREFIX + winner['id']['videoId']
        else:
            image_link = ''
            media_link = ''
        self.multimedia_link_memo[query] = (image_link, media_link)
        return image_link, media_link

    def map_json_array_to_rows(self, json_array, label_id):
        result = []
        for jsevt in json_array:
            try:
                title = jsevt["movie"].replace("â€™", "'")
                image_link, media_link = self.get_media_link(title)
                result.append({
                    'timestamp': self.target_date.strftime("%Y-%m-%d"),
                    'title': "#1 Movie: {}".format(title),
                    'text': "{} grossed a total of {}.".format(title, str(jsevt["total_gross"])),
                    'link': IMDB_SEARCH_PREFIX + self.get_query(title),
                    'label_id': label_id,
                    'image_link':  image_link,
                    'media_link':  media_link
                })
            except Exception as exception:
                self.logger.error('Something unexpected happened: {} {}'.format(
                    type(exception).__name__,
                    self.target_date))
        return result

    def already_same(self, existing_event, row):
        return existing_event['link'] == row['link'] \
            and existing_event['image_link'] == row['image_link'] \
            and existing_event['media_link'] == row['media_link'] \
            and existing_event['text'] == row['text']

    def store_s3(self, s3_bucket):
        # Pick the right name for json files.
        if len(self.events) > 0:
            s3_bucket.Object(key='{}/{}.json'.format(self.label_name,
                                                     self.target_date.strftime("%Y-%m-%d"))).put(Body=self.chart
            self.logger.info('Successfully stored {} {} events into S3'.format(
                self.target_date, len(self.events)))
        else:
            self.logger.warn(
                '***** No data for {} so skipping... '.format(self.target_date))

    def store_rds(self, db):
        label_id=db.get_label_id_from_name(self.label_name)
        rows=self.map_json_array_to_rows(self.events, label_id)
        no_inserts, no_updates, no_notouch=db.store_rds(
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
    m=Movies()
    if m.events == []:
        print("None")
    else:
        print(m.target_date.strftime("%Y-%m-%d"))
        print(m.chart.movies[0])
        print(m.map_json_array_to_rows(m.events, 5))


if __name__ == '__main__':
    main()
