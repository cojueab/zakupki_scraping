import requests
import datetime
import re
import os
import sqlite3
import random
import time
import csv
import uuid


class Zakupki(object):
    base_url = "http://www.zakupki.gov.ru/epz/order/extendedsearch/results.html"
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'en-US,en;q=0.9,ru;q=0.8',
        'Cache-Control': 'max-age=0',
        'Connection': 'keep-alive',
        'Cookie': 'routeepz0=1',
        'Host': 'www.zakupki.gov.ru',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/69.0.3497.100 Safari/537.36'
    }

    def __init__(self, proxies=False, pause=15):
        self.pause = pause
        self.proxies = proxies
        self.s = requests.Session()

    @staticmethod
    def create_param(laws_need=('fz44',), date=datetime.datetime.now().strftime("%d.%m.%Y"),
                     page_number=1, regions=5277398):
        def change_law(x):
            if x['title'] in laws_need:
                x['on'] = True
            return x

        laws = [{
            'on': False, "param": "fz44 = on", "title": 'fz44'}, {
            'on': False, "param": "fz223 = on", "title": 'fz223'}, {
            'on': False, "param": "ppRf615 = on", "title": 'ppRf615'}, {
            'on': False, "param": "fz94 = on", "title": 'fz94'
        }]
        laws = list(map(change_law, laws))
        laws = "&".join(list(map(lambda x: x['param'], filter(lambda x: x['on'], laws))))
        param = "searchString=&morphology=on&" \
                "pageNumber=" + str(page_number) + "&" \
                                                   "sortDirection=false&recordsPerPage=_50&" \
                                                   "showLotsInfoHidden=false&" + str(laws) + "" \
                                                                                             "selectedSubjects=&af=true&ca=true&pc=true&pa=true&" \
                                                                                             "priceFromGeneral=&" \
                                                                                             "priceToGeneral=&" \
                                                                                             "priceFromGWS=&" \
                                                                                             "priceToGWS=&" \
                                                                                             "priceFromUnitGWS=&" \
                                                                                             "priceToUnitGWS=&" \
                                                                                             "currencyIdGeneral=-1&" \
                                                                                             "publishDateFrom=" + str(
            date) + "&" \
                    "publishDateTo=" + str(date) + "&" \
                                                   "regions=" + str(regions) + "&" \
                                                                               "regionDeleted=false&" \
                                                                               "sortBy=UPDATE_DATE&" \
                                                                               "openMode=USE_DEFAULT_PARAMS"
        return param

    def get(self, laws_need, date, page_number, regions):
        time.sleep(self.pause)
        status = True
        col = 0
        body = ''
        while status or col > 20:
            col += 1
            if self.proxies:
                body = self.s.get(self.base_url + "?" + self.create_param(laws_need, date, page_number, regions),
                                  headers=self.headers, proxies=self.proxies)
            else:
                body = self.s.get(self.base_url + "?" + self.create_param(laws_need, date, page_number, regions),
                                  headers=self.headers)
            body.encoding = 'utf-8'
            status = body.status_code == 400
        if col > 20:
            raise RuntimeError('Proxies die')
        return body.text

    def get_clear(self, url):
        if self.proxies:
            body = self.s.get(url,
                              headers=self.headers, proxies=self.proxies)
        else:
            body = self.s.get(url,
                              headers=self.headers)
        body.encoding = 'utf-8'
        return body.text

    def get_last_page(self, text):
        page = re.findall('<li>...<\/li>\s*<li><a[^>]+>([^<]+)', text)
        if len(page) == 0:
            return 1
        else:
            return int(page[0])

    def get_links(self, text):
        urls = re.findall('descriptTenderTd">\s*<dl>\s*<dt>\s*<a\s*target="_blank"\s*href="([^"]+)', text)
        types = re.findall('<span\s*class="[^\s]+\s*noWrap">\s*([^\/]+)', text)
        links = []
        for index in range(0, len(urls)):
            links.append({'link': "http://www.zakupki.gov.ru" + str(urls[index]), 'type': types[index].strip()})
        return links

    def writelinks(self, links):
        db = sqlite3.connect('zakupki.db')
        c = db.cursor()
        c.execute("CREATE TABLE IF NOT EXISTS links (id INTEGER PRIMARY KEY AUTOINCREMENT, type text, link text);")
        db.commit()
        for link in links:
            c.execute("INSERT INTO links(type, link) VALUES(?,?)", (link['type'], link['link']))
            db.commit()
        db.close()

    def go(self, laws, date, region, current_page=1):
        body = self.get(laws, date, current_page, region)
        self.writelinks(self.get_links(body))
        last_page = self.get_last_page(body)
        if config.debug:
            last_page = 1
        while current_page != last_page:
            current_page += 1
            body = self.get(laws, date, current_page, region)
            self.writelinks(self.get_links(body))

    @staticmethod
    def write_file(file_name, raw_items):
        with open(file_name, 'w', newline='', encoding=config.encoding) as outfile:
            f = csv.writer(outfile, delimiter=';')
            for x in raw_items:
                f.writerow(map(lambda y: '{"' + y + '":"' + re.sub(r'\s+', ' ', x[y]) + '"}', x.keys()))

    def parse(self):
        db = sqlite3.connect('zakupki.db')
        c = db.cursor()
        c.execute("SELECT * from links")
        items = c.fetchall()
        random.shuffle(items)
        raw_items = []
        for item in items:
            info = {}
            body = self.get_clear(item[2])
            tables = re.findall('<table>[\w\W]+?<\/table>', body)
            for table in tables:
                trs = re.findall('<tr[^>]*>[\w\W]+?<\/tr>', table)
                for tr in trs:
                    tds = re.findall('<td[^>]*>([\w\W]+?)<\/td>', tr)
                    if len(tds) == 2:
                        info[tds[0].replace(r'\n', '').replace("\n", "").strip()] = tds[1].replace(r'\n', '').replace(
                            "\n", "").strip()
            raw_items.append(info)
            c.execute("DELETE FROM links where id=?", (item[0],))
            db.commit()
        db.close()

        path = 'files' + '/' + datetime.datetime.now().strftime("%d.%m.%Y") + '/'
        try:
            os.makedirs(path)
        except OSError:
            pass
        self.write_file(path + str(uuid.uuid1()) + '.csv', raw_items)


if __name__ == '__main__':
    import config

    zakupki = None
    if config.proxies:
        zakupki = Zakupki(proxies=config.proxies)
    else:
        zakupki = Zakupki()
    for law in config.laws:
        date = None
        if config.date == 'now':
            date = datetime.datetime.now().strftime("%d.%m.%Y")
        else:
            date = config.date
        for region in config.regions:
            zakupki.go((law,), date, region)
            zakupki.parse()
