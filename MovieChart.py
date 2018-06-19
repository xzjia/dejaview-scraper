import datetime
import json
from requests_html import HTMLSession

THE_NUMBERS_URL = "https://www.the-numbers.com/box-office-chart/"


class MovieChart(object):
    def __init__(self):
        self.target_date = datetime.datetime.now()
        self.movies = self.getMovies()

    def getMovies(self):
        movies = self.get_movies_for_day(self.target_date)
        return movies

    def get_movies_for_day(self, date):
        chart = self.get_weekly_chart(date.year, date.month, date.day)
        if len(chart) < 1:
            chart = self.get_weekend_chart(date.year, date.month, date.day)
        if len(chart) < 1:
            return []
        return chart

    def get_weekly_chart(self, year, month, date):
        session = HTMLSession()
        raw_html = session.get(
            THE_NUMBERS_URL + "weekly/{}/{}/{}".format(year, month, date))
        return self.get_chart(raw_html)

    def get_weekend_chart(self, year, month, date):
        session = HTMLSession()
        raw_html = session.get(
            THE_NUMBERS_URL + "weekend/{}/{}/{}".format(year, month, date))
        return self.get_chart(raw_html)

    def get_chart(self, raw_html):
        table = raw_html.html.find("#page_filling_chart table", first=True)
        # Ignore the first row because it is the table's headings
        rows = table.find("tr")[1:]
        keys = ["current_week_rank", "previous_week_rank", "movie", "distributor",
                "gross", "change", "num_theaters", "per_theater", "total_gross", "days"]
        results = []
        for row in rows:
            this_row = dict()
            cells = row.find("td")
            for i in range(len(cells)):
                this_row[keys[i]] = cells[i].text
            results.append(this_row)
        return results


def main():
    # Unit Test
    mc = MovieChart()
    print(mc.movies)


if __name__ == '__main__':
    main()
