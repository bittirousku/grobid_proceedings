# -*- coding: utf-8 -*-
# Copyright (C) 2016 CERN.

"""
Python module to extract information from a pdf file. This is designed to work
with Moriond conference proceedings. PDF extraction uses Grobid, which runs
as a service on http://inspire-grobid.cern.ch:8080/. Conversion from Grobid
output file formats to HEPRecord MARCXML is done with existing code from Invenio
and Inspire:

`invenio_grobid.mapping`,
`inspirehep.dojson.utils`, and
`invenio_utils.text`.


How it works:

1. Input one directory for this script. This dir must be on AFS (unless just testing).

2. Go through every pdf file in that directory (`process_pdf_dir`).

3. For every file, open them as a raw string (`open_pdf`)
   and give the string to Grobid (`process_pdf_stream`). Grobid outputs TEI
   format XML files.

4. Take the TEI XML file and convert it to a record dictionary (`build_dicts`).

5. Take the dictionary and modify its key names to match with
   MARC21 HEPRecord. Finally convert (`utils.legacy_export_as_marc`)
   and print the dictionary to a MARCXML file (`build_marc_xml`).


USAGE EXAMPLE: $ python grobid_proceedings.py -i test/

"""

from __future__ import print_function
from __future__ import absolute_import

import sys
import getopt
import os
import re

import fnmatch
import json
import logging

import requests

from grobid_proceedings import (
    mapping,
    utils,
    )

#input_dir = "test/"
#GROBID_HOST = "http://localhost:8080/"  # Local installation
GROBID_HOST = "http://inspire-grobid.cern.ch:8080/"


# Please remove whitespaces from filenames first.
FILE_SEARCH_PATTERN = [
    # Example: Pages_from_C88-01-23_15-24.pdf
    ('cnum and page range with prefix but take only start page',
     re.compile(r'^Pages_from_(C\d\d-\d\d-\d\d)[-_](\d+)\-\d+\.pdfa?$'),
     ('773__w', '773__c')),
    # Example: Pages_from_C75-03-02_101.pdf
    ('cnum and page start with prefix with optional dot',
     re.compile(r'^Pages_from_(C\d\d-\d\d-\d\d?.?\d)[-_](\d+)\.pdfa?$'),
     ('773__w', '773__c')),
    # Example: Pages_from_C88-03-06.1_79-89.pdf
    ('cnum and page range with prefix but take only start page with optional dot',
     re.compile(r'^Pages_from_(C\d\d-\d\d-\d\d?.?\d)[-_](\d+)\-\d+\.pdfa?$'),
     ('773__w', '773__c')),
    # Example: C73-03-04_Proceedings.pdf
    ('cnum with Proceedings suffixed with optional dot',
     re.compile(r'^(C\d\d-\d\d-\d\d?.?\d)[-_](Proceedings).pdfa?$'),
     ('773__w', '980__a')),
    # Example: anything.pdf
    ('garbage',
     re.compile(r'(?i)^(.+)\.pdfa?$'),
     ('rec_garbage', ))
    ]


# Set up a logger
logging.basicConfig(level=logging.DEBUG,
                    filename="grobid.log",
                    filemode="a+",
                    format="%(asctime)-15s %(levelname)-8s %(message)s")
logger = logging.getLogger("Grobid proceedings")
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)-12s: %(levelname)-5s %(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)


def parse_filename(pdf_file):
    """Get cnum and page numbers from pdf filename."""
    for (pattern_name, file_pattern, fields) in FILE_SEARCH_PATTERN:
        search_result = file_pattern.search(pdf_file)
        if search_result:
            logger.info('Recognised ' + pattern_name)
            logger.info(
                "cnum: " + search_result.group(1) + " fpage:" + search_result.group(2))
            return search_result.groups()
    logger.warning('No known pattern for ' + pdf_file)

def open_pdf(pdf_file):
    """Open one pdf file as a raw string."""
    with open(pdf_file, "r") as pfile:
        pdf_string = pfile.read()
    return pdf_string

def process_pdf_stream(pdf_file):
    """Process a PDF file stream with Grobid, returning TEI XML results."""
    response = requests.post(
        url=os.path.join(GROBID_HOST, "processFulltextDocument"),
        files={'input': open_pdf(pdf_file)}
        )

    if response.status_code == 200:
        return response.text

def process_pdf_dir(input_dir):
    """Process the entire directory, but take only pdf files.

    Return cnum, first page, and XML (parsed pdf) in Grobid TEI format.
    """
    paths = []
    filenames = []
    for root, dirnames, filenames in os.walk(input_dir):
        for filename in fnmatch.filter(filenames, '*.pdf'):
            paths.append(os.path.join(root, filename))
            filenames.append(filename)

    for filename, pdf_path in zip(filenames, paths):
        yield (
            os.path.abspath(pdf_path),
            parse_filename(filename),
            process_pdf_stream(pdf_path),
            )

def build_dicts(input_dir):
    """Create dictionaries from the TEI XML data."""
    for processed_pdf in process_pdf_dir(input_dir):
        pdf_path, (cnum, fpage), tei = processed_pdf
        rec_dict = mapping.tei_to_dict(tei)  # NOTE: this includes some empty elements, which is not cool
        rec_dict["pdf_path"] = pdf_path
        rec_dict["cnum"] = cnum
        rec_dict["fpage"] = fpage
        #import ipdb; ipdb.set_trace()
        yield rec_dict


def write_jsons(dic):
    """Write json files. For testing."""
    filename = dic["cnum"] + "_" + dic["fpage"] + ".json"
    target_folder = "tmp/"
    if not os.path.exists(target_folder):
        os.makedirs(target_folder)

    jsondic = json.dumps(dic, indent=4, sort_keys=True)
    with open(target_folder + filename, "w") as jfile:
        print(jsondic, file=jfile)


def write_xml(filename, cnum, marcxml):
    """Write MARCXML to a file."""
    target_folder = "tmp/marc_records/" + cnum + "/"
    if not os.path.exists(target_folder):
        os.makedirs(target_folder)

    with open(target_folder + filename, "w") as xfile:
        print(marcxml, file=xfile)

def get_authors(aut):
    """Get author name and affiliation. Format: 'lastname, firstname'."""
    author_name = ''
    surname = ''
    surname, given_names = utils.split_fullname(aut.get("name"), surname_first=False)
    if surname and "collaboration" in surname.lower():
        author_name = surname 
    if surname and given_names:
        if len(given_names) == 1:  # Handle initials
            given_names += "."
        author_name = u"{}, {}".format(surname, given_names)
    elif surname:
        author_name = surname
    affiliations = []
    aff_raws = aut.get("affiliations")
    if aff_raws:
        for aff in aff_raws:
            affiliations.append(aff.get("value").strip("()"))

    return author_name, affiliations



def build_marc_xml(input_dir, pubdate):
    """Build a MARCXML file from the HEPRecord dictionary."""
    counter = 0
    for dic in build_dicts(input_dir):
        marcdict = {}
        authors_raw = dic.get("authors")
        authors = []
        if authors_raw:
            # delete authors which have empty values:
            for author in authors_raw:
                author_not_empty = dict((k, v) for k, v in author.iteritems() if v)
                if author_not_empty:
                    authors.append(author_not_empty)
        if authors:
            marcdict["100"] = []
            marcdict["700"] = []
            # Only the first author should be put in the 100 field, others to 700
            author_name, affiliations = get_authors(authors[0])
            if not author_name:
                # "If you have a separate field for the affiliation it should always be 700 and no subfield $$a."
                marcdict["700"].append({"v":affiliations})
            else:
                marcdict["100"].append({"v":affiliations, "a":author_name})
            if len(authors) != 1:
                for aut in authors[1:]:
                    author_name, affiliations = get_authors(aut)
                    marcdict["700"].append({"v":affiliations, "a":author_name})
    
        title = dic.get("title")
        if title:
            marcdict["245"] = {"a": title.title()}
        if pubdate:
            marcdict["260"] = {"c": pubdate}
        abstract = dic.get("abstract")
        if abstract:
            marcdict["520"] = {"a": abstract}
        marcdict["773"] = {"c": dic.get("fpage"), "w":dic.get("cnum")}
        marcdict["980"] = [{"a": "ConferencePaper"}, {"a": "HEP"}]
        marcdict["FFT"] = {
            "a": dic["pdf_path"],
            "d": "Fulltext",
            "t": "INSPIRE-PUBLIC",
            }

        # NOTE: we don't need the references at this point
        #marcdict["999C5"] = []
        #for ref in dic["references"]:
            #authors = ", ".join([aut["name"] for aut in ref["authors"]])
            #title = ref["journal_pubnote"].get("journal_title", "")
             ## See above, should the section be here?
            #volume = ref["journal_pubnote"].get("journal_volume", "")
            #pages = ref["journal_pubnote"].get("page_range", "")
            #year = ref["journal_pubnote"].get("year", "")
            #pubnote = u"{},{},{}".format(title, volume, pages)
            #marcdict["999C5"].append({"s":pubnote, "y":year})

        marcxml = utils.legacy_export_as_marc(marcdict)
        filename = dic["cnum"] + "_" + dic["fpage"] + ".xml"
        print(marcdict["FFT"]["a"])
        print(marcxml)
        write_xml(filename, dic["cnum"], marcxml)
        counter += 1
    logger.info("Wrote " + str(counter) + " records.")


def main(argv):
    """Main function."""
    input_dir = ''
    pubdate = ''
    helptext = ("Usage: python grobid_proceedings.py -i <input_dir> -d <pubdate>\n"
        "<input_dir> is the CNUM of the conference, e.g. `C12-03-10`\n\v"
        "Pubdate has to be manually inserted, because the pdfs contain no "
        "information about that.")
    try:
        opts, args = getopt.getopt(argv, "hi:p:", ["ifile=", "pubdate="])
    except getopt.GetoptError:
        print(helptext)
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print(helptext)
            sys.exit()
        elif opt in ("-i", "--ifile"):
            input_dir = arg
        elif opt in ("-p", "--pubdate"):
            pubdate = arg
        #elif opt in ("-o", "--ofile"):
            #outputfile = arg

    if input_dir:
        #input_dir = "/afs/cern.ch/project/inspire/uploads/library/moriond/for_grobid/" + input_dir
        if os.path.exists(input_dir):
            print('Processing directory (CNUM)"', input_dir + '"')
            build_marc_xml(input_dir, pubdate)
        else:
            print("Path `"+ input_dir +"` doesn't exist!")
    else:
        print(helptext)

if __name__ == "__main__":
    main(sys.argv[1:])
