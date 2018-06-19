#!/usr/bin/env python3
import os
import json
import logging
import datetime

import requests
from requests_html import HTMLSession
from MovieOne import MovieOne

YOUTUBE_API = 'https://www.googleapis.com/youtube/v3/search'
YOUTUBE_LINK_PREFIX = 'https://www.youtube.com/watch?v='
IMDB_SEARCH_PREFIX = 'https://www.imdb.com/find?q='


class Movies(object):
    def __init__(self):
        self.target_date = datetime.datetime.now()
        self.label_name = 'Movies'
        # self.logger = logging.getLogger(
        #     'daily_collector.{}'.format(self.__class__.__name__))
        self.events = [MovieOne().movie]
        self.multimedia_link_memo = {}

    def get_query(self, title):
        raw = title + ' ' + str(self.target_date.year)
        return raw.replace(' ', '+')

    def get_media_link(self, title):
        query = self.get_query(title + " official movie trailer ")
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
            image_link, media_link = self.get_media_link(jsevt["movie"])
            result.append({
                'timestamp': self.target_date.strftime("%Y-%m-%d"),
                'title': "#1 Movie: {} ".format(jsevt["movie"]),
                'text': "{} grossed a total of {}.".format(jsevt["movie"], str(jsevt["total_gross"])),
                'link': IMDB_SEARCH_PREFIX + self.get_query(jsevt["movie"]),
                'label_id': label_id,
                'image_link':  image_link,
                'media_link':  media_link
            })
        return result


def main():
    # Some unit tests
    m = Movies()
    print(m.events[0]["movie"])
    print(m.map_json_array_to_rows(m.events, 5))


if __name__ == '__main__':
    main()
