import csv
import threading
import requests
import json
import traceback
from datetime import datetime
import re
from dateutil.rrule import rrule, MONTHLY
import random
import time
import os
import sys

def fmt(name):
    if name == None:
        name = ''
    a = re.sub("[^a-zA-Z0-9]", " ", name)
    return a.strip().lower()

def month_iter(start_month, start_year, end_month, end_year):
    start = datetime(start_year, start_month, 1)
    end = datetime(end_year, end_month, 1)
    return ("{} {} zestimate".format(d.strftime("%b"), d.year) for d in rrule(MONTHLY, dtstart=start, until=end))

def doWork(filename, writer, fieldnames):

    item = {}
    with open('json/{}'.format(filename), 'r') as f:
        data = json.loads(f.read())
    try:
        if data["property"]["errors"][0]["message"] == "Unable to fetch data":
            with open('uncompleted_zpid.csv', 'a') as wff:
                wff.write("{}\n".format(zpid))
            return
    except:
        pass
    _property = data['property']['data']['property']
    _tax_history = data['taxes']
    _zestimates = data['home']
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
    
    try:
        item["street address"] = _property["streetAddress"]
    except:
        item["street address"] = ""
    try:
        item["city"] = _property["city"]
    except:
        item["city"] = ""
    try:
        item["state"] = _property["state"]
    except:
        item["state"] = ""
    try:
        item["county"] = _property["county"]
    except:
        item["county"] = ""
    try:
        item["zipcode"] = str(_property["zipcode"])
    except:
        item["zipcode"] = ""
    try:
        item["number of bedrooms"] = _property["bedrooms"]
    except:
        item["number of bedrooms"] = ""
    try:
        item["number of bathrooms"] = _property["bathrooms"]
    except:
        item["number of bathrooms"] = ""
    try:
        item["size"] = _property["livingArea"]
    except:
        item["size"] = ""
    try:
        item["type"] = _property["propertyTypeDimension"]
    except:
        item["type"] = ""
    try:
        item["lot"] = _property["lotSize"]
    except:
        item["lot"] = ""
    try:
        item["year built"] = _property["yearBuilt"]
    except:
        item["year built"] = ""
    try:
        item["hoa"] = _property["hoaFee"]
    except:
        item["hoa"] = ""
    
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
    try:
        for cat in _property["homeFacts"]["categoryDetails"]:
            if cat['categoryGroupName'] == "Activity On Zillow":
                for cat2 in cat['categories'][0]['categoryFacts']:
                    if cat2['factLabel'] == 'Views in the past 30 days':
                        item["number views since listing"] = cat2["factValue"]
                    elif 'shoppers saved this home' in cat2['factValue']:
                        item["number of shoppers saved this home"] = cat2['factValue'].replace(" shoppers saved this home", "")
            else:
                catetory_group_name = fmt(cat['categoryGroupName'])
                for cat2 in cat['categories']:
                    for s in cat2["categoryFacts"]:
                        label = "{}:{}".format(catetory_group_name , fmt(s['factLabel']))
                        label = label.strip(":")
                        if label in fieldnames:
                            if label in item:
                                item[label] += "\n{}".format(s['factValue'])
                            else:
                                item[label] = s['factValue']
    except:
        print (traceback.format_exc())
        pass
    if os.path.exists('{}.csv'.format(item['zipcode'])):
        fieldnames = ["street address","city","state","county","zipcode","number of bedrooms","number of bathrooms","size","type","year built","heating","cooling","parking","lot","hoa","parcel","last sold price","last sold date","last zestimate","last agent name","2014 property taxes","2015 property taxes","2016 property taxes","2017 property taxes","2018 property taxes","2019 property taxes", "2014 tax assessment","2015 tax assessment","2016 tax assessment","2017 tax assessment","2018 tax assessment","2019 tax assessment","May 2014 zestimate","Jun 2014 zestimate","Jul 2014 zestimate","Aug 2014 zestimate","Sep 2014 zestimate","Oct 2014 zestimate","Nov 2014 zestimate","Dec 2014 zestimate","Jan 2015 zestimate","Feb 2015 zestimate","Mar 2015 zestimate","Apr 2015 zestimate","May 2015 zestimate","Jun 2015 zestimate","Jul 2015 zestimate","Aug 2015 zestimate","Sep 2015 zestimate","Oct 2015 zestimate","Nov 2015 zestimate","Dec 2015 zestimate","Jan 2016 zestimate","Feb 2016 zestimate","Mar 2016 zestimate","Apr 2016 zestimate","May 2016 zestimate","Jun 2016 zestimate","Jul 2016 zestimate","Aug 2016 zestimate","Sep 2016 zestimate","Oct 2016 zestimate","Nov 2016 zestimate","Dec 2016 zestimate","Jan 2017 zestimate","Feb 2017 zestimate","Mar 2017 zestimate","Apr 2017 zestimate","May 2017 zestimate","Jun 2017 zestimate","Jul 2017 zestimate","Aug 2017 zestimate","Sep 2017 zestimate","Oct 2017 zestimate","Nov 2017 zestimate","Dec 2017 zestimate","Jan 2018 zestimate","Feb 2018 zestimate","Mar 2018 zestimate","Apr 2018 zestimate","May 2018 zestimate","Jun 2018 zestimate","Jul 2018 zestimate","Aug 2018 zestimate","Sep 2018 zestimate","Oct 2018 zestimate","Nov 2018 zestimate","Dec 2018 zestimate","Jan 2019 zestimate","Feb 2019 zestimate","Mar 2019 zestimate","Apr 2019 zestimate","May 2019 zestimate","Jun 2019 zestimate","Jul 2019 zestimate","Aug 2019 zestimate","Sep 2019 zestimate","Oct 2019 zestimate","Nov 2019 zestimate","Dec 2019 zestimate","Jan 2020 zestimate","Feb 2020 zestimate","Mar 2020 zestimate","Apr 2020 zestimate","May 2020 zestimate","building","building:above grade fin sqft","building:above grade unfin sqft","building:abovegradeintsqftsource","building:accessibility features","building:below grade fin sqft","building:below grade unfin sqft","building:belowgradeintsqftsource","building:building name","building:design desc","building:features","building:handicap","building:one bedroom units count","building:pets","building:roof types","building:single room units count","building:storage","building:storylist","building:three bedroom units count","building:two bedroom units count","building:unit count","community and neighborhood","community and neighborhood:county","community and neighborhood:directions","community and neighborhood:elementary school","community and neighborhood:features","community and neighborhood:grade school","community and neighborhood:high school","community and neighborhood:localelistingstatus","community and neighborhood:location","community and neighborhood:middle school","community and neighborhood:municipality","community and neighborhood:neighborhood name","community and neighborhood:neighborhoods","community and neighborhood:school district","community and neighborhood:state","community and neighborhood:street suffix","community and neighborhood:town","community and neighborhood:zip code","construction","construction:architectural style","construction:construction completed yn","construction:construction date year built des","construction:construction materials","construction:construction type","construction:design","construction:exterior material","construction:features","construction:foundation","construction:foyer","construction:is new construction fl","construction:last remodel year","construction:listing type","construction:ownership type","construction:property sub type","construction:property type","construction:propertycondition","construction:propertytype","construction:roof","construction:roof type","construction:roofing","construction:siding","construction:siding exterior","construction:stories","construction:storms screens","construction:structure type","construction:style","construction:style desc","construction:styles","construction:sub type","construction:transaction type","construction:type","construction:typeof property","construction:zoning","exterior features","exterior features:acreage","exterior features:buildingfacadeorientation","exterior features:current use","exterior features:driveway","exterior features:driveway description","exterior features:exterior","exterior features:exterior description","exterior features:exterior features","exterior features:exterior features desc","exterior features:exteriorfeatures","exterior features:features","exterior features:hasbodyofwater","exterior features:haspool","exterior features:haswateraccess","exterior features:haswaterfront","exterior features:haswaterview","exterior features:landuse","exterior features:lot","exterior features:lot depth","exterior features:lot description","exterior features:lot size","exterior features:lot width","exterior features:lotdescription","exterior features:lotdimensions","exterior features:lotnumber","exterior features:lotsquarefootage","exterior features:out buildings","exterior features:outside features","exterior features:parcel","exterior features:pooltype","exterior features:structure type","exterior features:structurelist","exterior features:topography","exterior features:totalsquarefeet","exterior features:unit floor","exterior features:view","exterior features:view street","exterior features:water","exterior features:water access yn","exterior features:water information","exterior features:water source type","finance:associationfeefrequency","finance:community fee freq","finance:communityfeeincludes","finance:extrafee","finance:feeperiod","finance:hoa fee","finance:hoa fee freq","finance:hoafee","finance:maint fee freq","finance:tax annual amount","finance:tax lot","finance:taxes annual","interior features","interior features:addition size", "interior features:air conditioning","interior features:appliance oven","interior features:appliances","interior features:appliances included","interior features:attic desc","interior features:attics","interior features:basement","interior features:basement desc","interior features:basement type","interior features:basementtype","interior features:bath 1 level","interior features:bath 2 level","interior features:bathrooms full","interior features:bathrooms half","interior features:baths","interior features:baths description","interior features:bedroom 1 level","interior features:bedroom 2 level","interior features:bedroom 3 level","interior features:bedroom 4 level","interior features:bedroomfourth1level","interior features:bedroommaster1level","interior features:bedroomsecond1level","interior features:bedroomthird1level","interior features:beds","interior features:central air desc","interior features:cooling","interior features:cooling information","interior features:cooling type","interior features:cooling yn","interior features:dining room desc","interior features:dining room level","interior features:dining rooms","interior features:diningkitchen","interior features:entrance","interior features:family room level","interior features:familyroomlevel","interior features:features","interior features:fireplace","interior features:fireplacecount","interior features:fireplaces","interior features:floor","interior features:floor description","interior features:floor size","interior features:flooring","interior features:fuel information","interior features:fuel type","interior features:great family room","interior features:halfbaths","interior features:has central ac","interior features:hasbasement","interior features:heating","interior features:heating desc","interior features:heating type","interior features:heating yn","interior features:hoa or building fee","interior features:interior","interior features:interior description","interior features:interior features","interior features:interior features desc","interior features:interiorfeatures","interior features:interiorsquarefeetsource","interior features:kitchen","interior features:kitchen area","interior features:kitchen description","interior features:kitchen level","interior features:kitchen1level","interior features:laundryroomlevel","interior features:laundrytype","interior features:living room","interior features:living room desc","interior features:living room level","interior features:living rooms","interior features:livingroomlevel","interior features:mainbedroom","interior features:mainentrance","interior features:master bath","interior features:master bedroom","interior features:master bedroom description","interior features:other room description","interior features:other rooms","interior features:otherrm1level","interior features:otherrm2level","interior features:otherrm3level","interior features:otherrm4level","interior features:powder room level","interior features:room count","interior features:roomdimension","interior features:roomlevel","interior features:roomlist","interior features:roomtype","interior features:utility room level","interior features:waterheater","number of shoppers saved this home","number views since listing","other","other:above grade fin sqft","other:above grade finished area units","other:abovegradeintsqftsource","other:accessibility features","other:acreage","other:additionalsaleterms","other:also includes","other:appliances","other:architectural style","other:associationfeefrequency","other:attics","other:automatically update tax values y n","other:basement desc","other:basementtype","other:bath 1 level","other:bathrooms full","other:bathrooms half","other:bedroom 1 level","other:bedroom 2 level","other:bedroom 3 level","other:bedroom 4 level","other:bedroom level","other:bedroomfourth1level","other:bedroommaster1level","other:bedroomsecond1level","other:bedroomthird1level","other:belowgradeintsqftsource","other:central air desc","other:certifications","other:city","other:city town tax","other:city town tax pymnt freq","other:community pool features","other:communityfeeincludes","other:condo coop association yn","other:condofee","other:construction materials","other:cookingfuel","other:cooling yn","other:county tax","other:county tax pymnt freq","other:currentfinancing","other:cvrdparking1spaces","other:cvrdparking2spaces","other:design","other:design desc","other:dining room desc","other:dining room level","other:dining rooms","other:diningkitchen","other:directions","other:driveway","other:easement","other:electricservice","other:entrance","other:exterior","other:exterior features desc","other:exteriorfeatures","other:family room level","other:familyroomlevel","other:features","other:fireplace","other:fireplacecount","other:fireplacefeatures","other:fireplaces","other:foundation","other:garage","other:garage features","other:garagespaces","other:grade school","other:halfbaths","other:handicap","other:has ceiling fan fl","other:has lawn fl","other:has security system fl","other:hasbodyofwater","other:haspool","other:haswateraccess","other:haswaterfront","other:haswaterview","other:heating","other:heating desc","other:heating type","other:heating yn","other:hoa fee freq","other:hoafee","other:hotwater","other:included","other:interior features","other:interior features desc","other:interiorfeatures","other:interiorsquarefeetsource","other:is new construction fl","other:isforeclosure","other:isnewconstruction","other:issale","other:kitchen level","other:kitchen1level","other:last sale price sqft","other:last sold","other:laundryroomlevel","other:laundrytype","other:level","other:level 1 description","other:level 2 description","other:level 3 description","other:lismedialist","other:listagentstatelicensenumber","other:listing type","other:listingdate","other:living room desc","other:living room level","other:living rooms","other:livingroomlevel","other:localelistingstatus","other:lot size","other:lotdescription","other:lotdimensions","other:lotnumber","other:lotsquarefootage","other:mainbedroom","other:mainentrance","other:master bedroom level","other:municipal trash y n","other:neighborhoods","other:nooflevels","other:one bedroom units count","other:original listhub key","other:original mls name","other:original mls number","other:other rooms","other:otherrm1level","other:otherrm2level","other:otherrm3level","other:otherrm4level","other:ownership","other:ownership interest","other:ownership type","other:parking","other:parking   exterior","other:parking features","other:petsallowedlist","other:pool","other:pooltype","other:powder room level","other:price sqft","other:property manager y n","other:property type","other:propertycondition","other:propertytype","other:roof","other:roof types","other:roomdimension","other:roomlevel","other:roomlist","other:roomtype","other:sale rent","other:school tax pymnt freq","other:services info","other:sewer","other:sewerseptic","other:siding exterior","other:standard status","other:state","other:status","other:storage","other:storylist","other:street designation","other:structure type","other:structurelist","other:style","other:style desc","other:styles","other:sub system locale","other:tax annual amount","other:tax lot","other:tax total finished sqft","other:tax year","other:taxes amount","other:taxes annual","other:taxyear","other:total below grade sqft source","other:total sqft source","other:totalleasedunits","other:totalnoofunits","other:totalsquarefeet","other:totaltaxes","other:totalunfurnishedunits","other:two bedroom units count","other:type","other:type of parking","other:unit building type","other:utility room level","other:water","other:water heater","other:water view yn","parking","parking:cvrdparking1spaces","parking:cvrdparking2spaces","parking:features","parking:garage","parking:garage description","parking:garagespaces","parking:garagetype","parking:parking","parking:parking   exterior","parking:parking description","parking:parking driveway description","parking:parking features","rental facts:date available","rental facts:deposit   fees","rental facts:laundry","rental facts:pets","rental facts:posted","rental facts:rent sqft","senior living","senior living:features","senior living:housingforolderpersons","sources:mls","spaces and amenities","spaces and amenities:unit count","utilities","utilities:cookingfuel","utilities:electricservice","utilities:features","utilities:hotwater","utilities:sewer","utilities:sewer information","utilities:sewer type","utilities:sewerseptic","utilities:utilities","utilities:utilities info","utilities:water heater","utilities:water sewer"]
        csvfile = open("properties.csv", 'w')
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames, quoting=csv.QUOTE_ALL, quotechar='"', dialect='excel')
        writer.writeheader()
    else:
        csvfile = open("properties.csv", 'a')
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames, quoting=csv.QUOTE_ALL, quotechar='"', dialect='excel')

    writer.writerow(item)
    csvfile.close()

def main():
    # f = open('zpid.csv','r')
    # csv_reader = csv.reader(f)
    files = os.listdir()
    
    for file in files:
        doWork(file)
        # break

if __name__ == "__main__":
    main()