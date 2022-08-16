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
            clean = w3lib.html.remove_tags(text).strip()
            cleaned = clean.replace("\n", "").replace(
                "\t", "").replace("\r", "").replace("\xa0", "").strip()
            return " ".join(cleaned.split())
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
        print("in program page details")
        programInformation = response.xpath(
            "//ul[@id='information']")
        questions = programInformation.xpath("//p[normalize-space()]").getall()
        for question in questions:
            if "?" in self.cleanUpText(question) or ":" in self.cleanUpText(question):
                self.cleanDetails(self.cleanUpText(question))
      #  programClean = self.cleanUpText(programInformation).strip()
       # returnArr.append(programClean)
        # return returnArr

    def cleanDetails(self, question):
        responses = {}
        list_of_questions = ["length", "start on",
                             "program number", "match", "available positions"]
        for q in list_of_questions:
            if q in question:
                if "?" in question:
                    fields = question.split("?")
                else:
                    fields = question.split(":")
                responses[q.replace(" ", "_")] = fields[1]
                continue

    def yeildResults(self, title, container, response):
        print("in yield")
        container = self.cleanUpText(container)
        title = self.cleanUpText(title)

        keywords = self.matchKeywords(container)
        programDetails = self.getProgramPagesDetails(response)
        self.cleanDetails(programDetails)
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
            "details": programDetails
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
        print(active)
        title = response.xpath("//div[@align='center']/text()").get()
        return self.yeildResults(title, container, response)
