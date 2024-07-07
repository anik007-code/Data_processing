import re
import sys
import traceback
from datetime import datetime, timezone
import scrapy
from bs4 import BeautifulSoup
import spacy
import requests
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError

en = spacy.load("en_core_web_sm")


class GoogleComSpider(scrapy.Spider):
    name = "google.com"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.config = self.get_config()

    def get_config(self):
        config = {}
        config["BaseUrl"] = "https://www.google.com"
        config[
            "StartUrl"] = "https://www.google.com/about/careers/applications/jobs/results/?location=Zurich%2C%20Switzerland"
        return config

    def start_requests(self):
        try:
            yield scrapy.Request(self.config['StartUrl'], method="GET", callback=self.parse_all)
        except Exception as e:
            print(e)

    def parse_all(self, response):
        try:
            links = response.xpath(
                '//div[@class="VfPpkd-WsjYwc VfPpkd-WsjYwc-OWXEXe-INsAgc KC1dQ Usd1Ac AaN0Dd  kFpsj"]')
            for i in links:
                job_link = i.xpath('.//div/div[5]/div/a/@href').get()
                title = i.xpath('.//div/div/div/h3/text()').get()
                location = i.xpath('.//div/div[2]/div/span[2]/span/text()').get()
                meta = {
                    "title": title,
                    "location": location
                }
                yield response.follow(job_link, callback=self.parse_job, meta=meta)

        except Exception as e:
            print(traceback.format_exc())
            sys.stdout.flush()

    def extract_skills(self, text):
        doc = en(text)
        noun_chunks = []
        current_chunk = []
        for token in doc:
            if token.pos_ in {'NOUN', 'PROPN', 'ADJ'}:
                current_chunk.append(token.text)
            else:
                if current_chunk:
                    noun_chunks.append(" ".join(current_chunk))
                    current_chunk = []
        if current_chunk:
            noun_chunks.append(" ".join(current_chunk))
        final = list(set(noun_chunks))
        return final[:30]

    def geo_location(self, query):
        try:
            app = Nominatim(user_agent="anik")
            loc = app.geocode(query, addressdetails=True)
            if loc:
                return loc.raw
        except (GeocoderTimedOut, GeocoderServiceError) as e:
            print(f"Nominatim is not working: {e}")
            return None

    def get_location(self, text):
        ad = {}
        loc_res = self.geo_location(text)
        if not loc_res:
            return "Location not found"

        address = loc_res.get("address", {})
        loc = None

        for addr_type in ["place", "town", "city", "suburb", "region", "village"]:
            if addr_type in address:
                loc = address[addr_type]
                if addr_type in ["town", "city", "suburb"]:
                    ad["JobLocationCity"] = loc
                break

        if "state" in address:
            ad["JobLocationLocality"] = address["state"]

        if "country" in address:
            ad["JobLocationCountry"] = address["country"]
            loc = address["country"]

        if "country_code" in address:
            ad["JobLocationCountryCode"] = address["country_code"]

        if loc:
            ad["JobLocation"] = loc
            ad["JobLocationText"] = loc
        return ad

    def parse_job(self, response):
        try:
            ad = {}
            jobTitle = response.request.meta["title"]
            if jobTitle is not None:
                jobTitle = jobTitle.strip()
            else:
                jobTitle = ''

            jobLocation = response.request.meta["location"]
            if jobLocation is not None:
                jobLocation = jobLocation.strip()
            else:
                jobLocation = ''

            website_text = response.body.decode("UTF-8")
            jobs_soup = BeautifulSoup(website_text.replace("<", " <"), "html.parser")

            description = jobs_soup.find('main', {"class": "SxL7od"})
            if description is not None:
                cleanContent = re.sub('\s+', ' ', description.get_text())
                rawContent = re.sub('\s+', ' ', description.decode_contents())
            else:
                cleanContent = ''
                rawContent = ''
            ad['JobTitle'] = jobTitle
            ad = self.get_location(jobLocation)
            ad['CleanContent'] = cleanContent
            ad['Skills'] = self.extract_skills(cleanContent)
            emailList = re.findall(
                '\S+@\S+', cleanContent.strip("\n"))
            phoneList = re.findall(r'[\+\(]?[1-9][0-9 \-\(\)]{8,}[0-9]',
                                   cleanContent.strip("\n").replace('\u00a0', ' '))
            if len(emailList) > 0:
                _email = emailList[0]
                ad['JobContactEmails'] = _email
            if len(phoneList) > 0:
                for i in range(len(phoneList)):
                    phone = phoneList[i].strip().strip('(').strip(')')
                    if len(phone) > 0:
                        ad['JobContactPhone'] = phone
            yield ad
        except Exception as e:
            print(traceback.format_exc())
            sys.stdout.flush()

    def close(self, reason):
        print("Scraped")
