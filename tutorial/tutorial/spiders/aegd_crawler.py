import scrapy

import w3lib.html
import csv

import spacy
from collections import Counter
from string import punctuation

from spacy.matcher import Matcher

import hashlib

nlp = spacy.load("en_core_web_sm")


class QuotesSpider(scrapy.Spider):
    name = "aegd"

    def start_requests(self):

        programs = []
        with open('/home/eliasingea/school-search-crawl/programpages_1.csv', newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                programs.append(row)

        broken_urls = []

        for program in programs:
            url = program["url"]
            if url == "https://dental.tufts.edu/academics/postgraduate-programs/advanced-education-prosthodontics":
                print(url)
            try:
                if "programpages" in url:
                    yield scrapy.Request(url=url, callback=self.parseProgramPages, meta={"program": program})
                else:
                    yield scrapy.Request(url=url, callback=self.parseCatchAll, meta={"program": program})
            except:
                broken_urls.append(url)
                continue

    def get_hotwords(self, text):
        result = []
        pattern = [{'POS': 'ADJ', 'DEP': 'amod'},
                   {'POS': 'NOUN'}]
        matcher = Matcher(nlp.vocab)
        matcher.add("DentalVocab", [pattern])
        doc = nlp(text.lower())
        matches = matcher(doc)
        for match_id, start, end in matches:
            # Get string representation
            string_id = nlp.vocab.strings[match_id]
            span = doc[start:end]  # The matched span
            #print(match_id, string_id, start, end, span.text)
            result.append(span.text)

        return result

    def cleanUpText(self, text):
        try:
            return w3lib.html.remove_tags(text).strip()
        except Exception as e:
            return ""

    def matchKeywords(self, text):
        keywords = set()
        with open("/home/eliasingea/school-search-crawl/tutorial/tutorial/keywords.txt", "r") as keywords_file:
            for line in keywords_file.readlines():
                if line.strip() in text:
                    keywords.add(line.strip())
        return list(keywords)

    def getProgramPagesDetails(self, response):
        programInformation = response.xpath(
            "//ul[@id='information']").getall()
        returnArr = {}
        for program in programInformation:
            program = self.cleanUpText(program)
            field = program.split(program, ":")
            returnArr[field[0]] = field[1]
        return returnArr

    def yeildResults(self, title, container, response):

        container = self.cleanUpText(container)
        title = self.cleanUpText(title)

        keywords = self.matchKeywords(container)
        programs = self.getProgramPagesDetails(response)
        print(programs)
        program = response.meta["program"]
        try:
            objectID = hashlib.md5(title.encode()).hexdigest() if title != "" else hashlib.md5(
                program["Program Name"].encode()).hexdigest()
        except:
            print("error")
        yield {
            "objectID": objectID,
            "active": True,
            "title": title,
            "Program Name": program["Program Name"],
            "Program Type": program["Program Type"],
            "url": program["url"],
            "keywords": keywords,
            "state": program["state"],
            "deadline": program["deadline"],
            **programs
        }

    def parseCatchAll(self, response):
        container = "\n".join(response.xpath("//body").getall())
        title = response.xpath("//head/title/text()").get()
        return self.yeildResults(title, container, response)

    # def parseUCLA(self, response):
    #     title = response.xpath("//head/title/text()").get()
    #     container = "\n".join(response.xpath(
    #         "//div[@id='block-dentistry-content']//p").getall())
    #     return self.yeildResults(title, container, response)

    # def parseUNC(self, response):
    #     container = "\n".join(response.xpath(
    #         "//article[@class='chapters-container']//p").getall())
    #     title = response.xpath("//head/title/text()").get()
    #     return self.yeildResults(title, container, response)

    def parseProgramPages(self, response):
        print("hello")
        active = True
        container = response.xpath("//div[@id='container']").get()
        if "Program is NOT active" in container:
            active = False

        title = response.xpath("//div[@align='center']/text()").get()
        if not active:
            yield {
                "objectID": hashlib.md5(title.encode()).hexdigest(),
                "title": title,
                "active": False
            }
        return self.yeildResults(title, container, response)
