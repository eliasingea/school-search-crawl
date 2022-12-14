from tokenize import group
import scrapy

import w3lib.html
import csv

import spacy
from collections import Counter
from string import punctuation

from spacy.matcher import Matcher

import hashlib
import os
from config.definitions import ROOT_DIR
import re

nlp = spacy.load("en_core_web_sm")


class QuotesSpider(scrapy.Spider):
    name = "aegd"

    def get_file_name(self, path):
        return os.path.join(ROOT_DIR, path)

    def start_requests(self):

        programs = []
        with open(self.get_file_name('programpages_1.csv'), newline='') as csvfile:
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
        with open(self.get_file_name("tutorial/tutorial/keywords.txt"), "r") as keywords_file:
            for line in keywords_file.readlines():
                if line.strip() in text:
                    keywords.add(line.strip())
        return list(keywords)

    def getProgramPagesDetails(self, response):
        programInformation = response.xpath(
            "//ul[@id='information']")
        questions = programInformation.xpath("//p[normalize-space()]").getall()
        responses = {}
        for question in questions:
            if "?" in self.cleanUpText(question) or ":" in self.cleanUpText(question):
                self.cleanDetails(self.cleanUpText(question), responses)
        return responses
      #  programClean = self.cleanUpText(programInformation).strip()
       # returnArr.append(programClean)
        # return returnArr

    def cleanDetails(self, question, responses):
        list_of_questions = ["length", "start on",
                             "program number", "match", "available positions", "email", "phone"]
        for q in list_of_questions:
            if q in question.lower():
                if "?" in question:
                    fields = question.split("?")
                else:
                    fields = question.split(":")
                responses[q.replace(" ", "_")] = fields[1]
                continue

    def yeildResults(self, title, container, response):
        container = self.cleanUpText(container)
        title = self.cleanUpText(title)
        keywords = self.matchKeywords(container)
        programDetails = {}
        programDetails = self.getProgramPagesDetails(response)
        program = response.meta["program"]
        programName = program["Program Type"]
        length = "12 months"
        regex = r"(\d+)\s(months|years)"

        if "Program Type" in program:
            matches = re.search(regex, program["Program Type"])
            if matches:
                if len(matches.groups()) > 1:
                    if matches.group(2) == "months":
                        length = str(matches.group(1)) + " months"
                    elif matches.group(2) == "years":
                        length = str(matches.group(1) * 12) + " months"
            if "Advanced Education in General Dentistry" in program["Program Type"]:
                programName = "AEGD"
            elif "Oral and Maxillofacial Surgery" in program["Program Type"]:
                programName = "OMS"
            elif "General Practice Residency" in program["Program Type"]:
                programName = "GPR"

        if program["Program Name"] == "NA":
            program["Program Name"] = program["title"].split("-")[0].strip()

        if programName == "#REF!":
            programName = program["title"].split("/")[1].strip()

        if "match" in programDetails:
            if programDetails["match"] != "Yes" and programDetails["match"] != "No":
                programDetails["match"] = "No"

        if "length" in programDetails:
            if "months" not in programDetails["length"] and "year" not in programDetails["length"] and "years" not in programDetails["length"]:
                programDetails["length"] = "12 months"
            if "1 year" in programDetails["length"].strip():
                programDetails["length"] = "12 months"

        if programDetails and "length" in programDetails:
            length = programDetails["length"]
        else:
            programDetails["length"] = length

        if "months months" in programDetails["length"]:
            programDetails["length"] = programDetails["length"].replace(
                "month", "", 1)
        elif "optional 24" in programDetails["length"]:
            programDetails["length"] = "12-24 months"
        elif "2nd year" in programDetails["length"]:
            programDetails["length"] = "12-24 months"
        elif "years" in programDetails["length"]:
            lenSplit = programDetails["length"].split()
            year = int(lenSplit[0])
            month = year * 12
            programDetails["length"] = str(month) + " months"
        try:
            objectID = hashlib.md5(title.encode()).hexdigest() if title != "" else hashlib.md5(
                program["Program Name"].encode()).hexdigest()
        except:
            print("error")
        yield {
            "objectID": objectID,
            "active": True,
            "title": title,
            "program": programName,
            "Program Type": program["Program Type"],
            "url": program["url"],
            "keywords": keywords,
            "state": program["state"],
            "deadline": program["deadline"],
            **programDetails
        }

    def parseCatchAll(self, response):
        container = "\n".join(response.xpath("//body").getall())
        title = response.xpath("//head/title/text()").get()
        return self.yeildResults(title, container, response)

    def parseProgramPages(self, response):
        active = True
        container = response.xpath("//div[@id='container']").get()
        if "Program is NOT active" in container:
            active = False
        title = response.xpath("//div[@align='center']/text()").get()
        return self.yeildResults(title, container, response)
