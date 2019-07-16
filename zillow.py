from lxml import html
import requests
import unicodecsv as csv
import argparse
from bs4 import BeautifulSoup
import json
from datetime import datetime
from dateutil.rrule import rrule, MONTHLY
import traceback
import re
import sys
import zlib, base64


categories_field = []

def get_proxy():
    proxy_r='http://lum-customer-hl_f6570114-zone-static-country-us:tlmgc8x07nd6@zproxy.lum-superproxy.io:22225'
    proxies = {
        'http': proxy_r,
        'https': proxy_r
    }
    return proxies

def get_response(url, headers):
    while True:
        proxy = get_proxy()
        try:
            response = requests.get(url, headers=headers, proxies=proxy, timeout=10)
            if not "verify you're a human" in response.text:
                return response
        except:
            pass

def parse(zipcode):
    url = "https://www.zillow.com/homes/{}_rb/".format(zipcode)
    print (url)
    headers = {
        'authority': 'www.zillow.com',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.103 Safari/537.36',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
        'accept-encoding': 'gzip, deflate, br',
        'accept-language': 'en-US,en;q=0.9',
    }
    
    response = requests.get(url, headers=headers)
    with open('html/{}_1.htm'.format(zipcode), 'wb') as f:
        f.write(zlib.compress(response.text))
    parser = html.fromstring(response.text)
    cnt = 0
    try:
        cnt = int(re.sub("[^0-9]", "", parser.xpath("//title/text()")[0].split('-')[1]).strip())
    except:
        print (traceback.format_exc())
        pass
    print ("cnt: {}".format(cnt))
    if cnt>500:
        prices = ['0','5000','10000', '20000', '35000', '50000', '75000', '100000','125000', '135000', '150000','165000', '175000', '200000', '250000', '300000','350000', '400000', '500000', '750000', '1000000', '1500000', '2000000', '']
        results = []
        for i in range(1, len(prices)):
            _url = "https://www.zillow.com/homes/recently_sold/{}_rb/{}-{}_price/24m_days/".format(zipcode, prices[i-1], prices[i])
            print(_url)
            response = requests.get(_url, headers=headers)
            with open('html/{}_{}_{}_1.htm'.format(zipcode, prices[i-1], prices[i]), 'wb') as f:
                f.write(zlib.compress(response.text))
            parser = html.fromstring(response.text)
            _cnt = 0
            try:
                _cnt = int(re.sub("[^0-9]", "", parser.xpath("//title/text()")[0].split('-')[1]).strip())
            except:
                pass
            if _cnt > 500:
                _cnt = 500
                with open("uncompleted_urls.txt", "a") as uf:
                    uf.write(_url + '\n')
            pages = (_cnt - 1)/25 + 1
            print("cnt: {}, pages: {}".format(_cnt, pages))
            results = results + parser_page(parser)
            for page in range(1, pages):
                url = "{}{}_p".format(_url, page + 1)
                print (url)
                response = requests.get(url, headers=headers)
                with open('html/{}_{}_{}_{}.htm'.format(zipcode, prices[i-1], prices[i], page), 'wb') as f:
                    f.write(zlib.compress(response.text))
                parser = html.fromstring(response.text)
                results = results + parser_page(parser)
    else:
        pages = (cnt - 1) / 25 + 1
        print ("pages: {}".format(pages))
        results = []
        results = results + parser_page(parser)
        for page in range(1, pages):
            url = "https://www.zillow.com/homes/recently_sold/{}_rb/24m_days/{}_p".format(zipcode, page + 1)
            print (url)
            response = requests.get(url, headers=headers)
            with open('html/{}_{}.htm'.format(zipcode, page), 'wb') as f:
                f.write(zlib.compress(response.text))
            parser = html.fromstring(response.text)
            results = results + parser_page(parser)
    return results

def parser_page(parser):
    search_results = parser.xpath("//div[@id='search-results']//article")
    if len(search_results) == 0:
        search_results = parser.xpath("//div[@id='grid-search-results']//article")

    properties_list = []
    for properties in search_results:
        try:
            zpid = properties.xpath("./@data-zpid")[0]
            created_at = ''.join(properties.xpath(".//div[contains(@class, \"list-card-variable-text \")]//text()"))
        except:
            zpid = properties.xpath("./@id")[0].replace("zpid_", "")
            created_at = ''.join(properties.xpath(".//li[contains(@class,\"toz \")]//text()"))
        
        properties_list.append(zpid)
        with open("zpid.csv", "a") as fff:
            fff.write("{}\n".format(zpid))
        # try:
        #     properties_list.append(parse_property(zpid))
        # except:
        #     print (zpid, traceback.format_exc())
        #     pass
    return properties_list

def fmt(name):
    if name == None:
        name = ''
    a = re.sub("[^a-zA-Z0-9]", " ", name)
    return a.strip().lower()

def month_iter(start_month, start_year, end_month, end_year):
    start = datetime(start_year, start_month, 1)
    end = datetime(end_year, end_month, 1)
    return ("{} {} zestimate".format(d.strftime("%b"), d.year) for d in rrule(MONTHLY, dtstart=start, until=end))

def parse_property(zpid):
    headers = {
        'origin': 'https://www.zillow.com',
        'accept-encoding': 'gzip, deflate, br',
        'accept-language': 'en,sr-RS;q=0.9,sr;q=0.8,en-US;q=0.7',
        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.103 Safari/537.36',
        'content-type': 'text/plain',
        'accept': '*/*',
        'authority': 'www.zillow.com',
    }

    item = {}

    data = json.dumps({"operationName":"OffMarketFullRenderQuery","variables":{"zpid":zpid,"contactFormRenderParameter":{"zpid":zpid,"platform":"desktop","isDoubleScroll":False}},"clientVersion":"home-details/5.44.0.0.0.hotfix-2019-05-09.1e2feab","queryId":"4bd6ae58d3ae4bd5a8fd2c48f5e5c44e"})
    response = requests.post('https://www.zillow.com/graphql/', headers=headers, data=data).json()
    _property = response['data']['property']
    data = json.dumps({"operationName":"PriceTaxQuery","variables":{"zpid":zpid},"query":"query PriceTaxQuery($zpid: ID!) {\n  property(zpid: $zpid) {\n    zpid\n    livingArea\n    countyFIPS\n    parcelId\n    taxHistory {\n      time\n      taxPaid\n      taxIncreaseRate\n      value\n      valueIncreaseRate\n    }\n    priceHistory {\n      time\n      price\n      priceChangeRate\n      event\n      source\n      buyerAgent {\n        photo {\n          url\n        }\n        profileUrl\n        name\n      }\n      sellerAgent {\n        photo {\n          url\n        }\n        profileUrl\n        name\n      }\n      showCountyLink\n      postingIsRental\n    }\n    currency\n    country\n  }\n}\n","clientVersion":"home-details/5.44.0.0.0.hotfix-2019-05-09.1e2feab"})
    _tax_history = requests.post('https://www.zillow.com/graphql/', headers=headers, data=data).json()
    data = json.dumps({"operationName":"HomeValueChartDataQuery","variables":{"zpid":zpid,"timePeriod":"FIVE_YEARS","metricType":"LOCAL_HOME_VALUES","forecast":True},"query":"query HomeValueChartDataQuery($zpid: ID!, $metricType: HomeValueChartMetricType, $timePeriod: HomeValueChartTimePeriod) {\n  property(zpid: $zpid) {\n    homeValueChartData(metricType: $metricType, timePeriod: $timePeriod) {\n      points {\n        x\n        y\n      }\n      name\n    }\n  }\n}\n","clientVersion":"home-details/5.44.0.0.0.hotfix-2019-05-09.1e2feab"})
    _zestimates = requests.post('https://www.zillow.com/graphql/', headers=headers, data=data).json()
    try:
        for p in _tax_history['data']['property']['taxHistory']:
            dt = datetime.fromtimestamp(p["time"]/1000).strftime("%Y")
            if int(dt) < 2014:
                break
            item[dt + " tax assessment"] = p["value"]
            item[dt + " property taxes"] = p["taxPaid"]
    except:
        print (traceback.format_exc())
        pass    
    try:
        for k in _zestimates['data']['property']['homeValueChartData'][0]['points']:
            item[datetime.fromtimestamp(k['x']/1000).strftime("%b %Y zestimate")] = k['y']
    except:
        print (traceback.format_exc())
        pass    
    
    item["street address"] = _property["streetAddress"]
    item["city"] = _property["city"]
    item["state"] = _property["state"]
    item["county"] = _property["county"]
    item["zipcode"] = str(_property["zipcode"])
    item["number of bedrooms"] = _property["bedrooms"]
    item["number of bathrooms"] = _property["bathrooms"]
    item["size"] = _property["livingArea"]
    item["type"] = _property["propertyTypeDimension"]
    item["lot"] = _property["lotSize"]
    item["year built"] = _property["yearBuilt"]
    item["hoa"] = _property["hoaFee"]
    
    try:
        for cat in _property["homeFacts"]["atAGlanceFacts"]:
            if cat['factLabel'] == 'Heating':
                item["heating"] = ("No Data", cat['factValue'])[cat['factValue']!=None]
            elif cat['factLabel'] == 'Cooling':
                item["cooling"] = ("No Data", cat['factValue'])[cat['factValue']!=None]
            elif cat['factLabel'] == 'Parking':
                item["parking"] = ("No Data", cat['factValue'])[cat['factValue']!=None]
    except:
        print (traceback.format_exc())
        pass
    try:
        item["last sold date"] = datetime.fromtimestamp(_property["dateSold"]/1000).strftime("%m/%d/%Y")
    except:
        item["last sold date"] = ""
    try:
        item["last sold price"] = _property["lastSoldPrice"]
    except:
        item["last sold price"] = ""
    try:
        item["last agent name"] = _property["priceHistory"][0]["sellerAgent"]["name"]
    except:
        item["last agent name"] = ""
    try:
        item['last zestimate'] = _property["zestimate"]
    except:
        item['last zestimate'] = ""
    try:
        item['parcel'] = str(_property["parcelId"])
    except:
        item['parcel'] = ""

    for cat in _property["homeFacts"]["categoryDetails"]:
        if cat['categoryGroupName'] == "Activity On Zillow":
            for cat2 in cat['categories'][0]['categoryFacts']:
                if cat2['factLabel'] == 'Views in the past 30 days':
                    item["number views since listing"] = cat2["factValue"]
                    if not "number views since listing" in categories_field:
                        categories_field.append("number views since listing")
                elif 'shoppers saved this home' in cat2['factValue']:
                    item["number of shoppers saved this home"] = cat2['factValue'].replace(" shoppers saved this home", "")
                    if not "number of shoppers saved this home" in categories_field:
                        categories_field.append("number of shoppers saved this home")
        else:
            catetory_group_name = fmt(cat['categoryGroupName'])
            for cat2 in cat['categories']:
                for s in cat2["categoryFacts"]:
                    label = "{}:{}".format(catetory_group_name , fmt(s['factLabel']))
                    label = label.strip(":")
                    if label in item:
                        item[label] += "\n{}".format(s['factValue'])
                    else:
                        item[label] = s['factValue']
                    if not label in categories_field:
                        categories_field.append(label)

    return item

if __name__=="__main__":
    f = open('zipcode.csv', 'r')
    csv_reader = csv.reader(f)
    for zipcodes in csv_reader:
        zipcode = zipcodes[0]
        print ("Fetching data for %s"%(zipcode))
        scraped_data = parse(zipcode)
        print ("Writing data to output file")
        continue
        glance_fields = ['street address', 'city', 'state', 'county', 'zipcode', 'number of bedrooms', 'number of bathrooms', 'size', 'type', 'year built', 'heating', 'cooling', 'parking', 'lot', 'hoa', 'parcel']
        finance_fields = ['last sold price', 'last sold date', 'last zestimate', 'last agent name']
        property_taxes_fields = ['2014 property taxes', '2015 property taxes', '2016 property taxes', '2017 property taxes', '2018 property taxes']
        tax_assessment_fields = ['2014 tax assessment', '2015 tax assessment', '2016 tax assessment', '2017 tax assessment', '2018 tax assessment']
        zestimate_price_fields = []
        for m in month_iter(5, 2014, 5, 2020):
            zestimate_price_fields.append(m)
        categories_field = sorted(categories_field)
        fieldnames = glance_fields + finance_fields + property_taxes_fields + tax_assessment_fields + zestimate_price_fields + categories_field

        with open("properties-%s.csv"%(zipcode),'wb') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames, quoting=csv.QUOTE_ALL, quotechar='"', dialect='excel')
            writer.writeheader()
            for row in  scraped_data:
                writer.writerow(row)
        # break