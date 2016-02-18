# -*- coding: utf-8 -*-
# Copyright (C) 2016 CERN.


"""API functions to interact with Grobid REST API."""

from __future__ import print_function

import os

import requests

import re

import fnmatch

import json

import logging


import mapping

import utils


""" TODO:

* This should work on lxplus, make it so!
* This should work from command line
* Check everything's working
* Check notes and fixmes
* Could this be cool as a class?
* Main function is ugly, could it be better?
* Is it okay to have - after fpage number?
* Make meaningful and WORKING log messages
* Check directory creation




"""
logging.basicConfig(level=logging.DEBUG, filename="grobid.log", filemode="a+", format="%(asctime)-15s %(levelname)-8s %(message)s")
logger = logging.getLogger(__name__)

#GROBID_HOST = "http://localhost:8080/"  # Local installation
GROBID_HOST = "http://inspire-grobid.cern.ch:8080/"
input_dir = "test/"

# NOTE: Please remove whitespaces from filenames first!
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



#def __init__():
    ## TODO: should do this when called from command line!
    #build_marc_xml(input_dir)

def parse_filename(pdf_file):
    """Get cnum and page numbers from pdf filename."""
    for (pattern_name, file_pattern, fields) in FILE_SEARCH_PATTERN:
        search_result = file_pattern.search(pdf_file)
        if search_result:
            #print('Recognised ' + pattern_name)
            return search_result.groups()

    print('No known pattern for ' + pdf_file)
    #return None

def open_pdf(pdf_file):
    """Open one pdf file as a raw string."""
    with open(pdf_file, "r") as f:
        pdf_string = f.read()
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
        yield (os.path.abspath(pdf_path),
               parse_filename(filename),
               process_pdf_stream(pdf_path),
               )

def build_dicts(input_dir):
    """Create dictionaries from the TEI XML data."""
    for processed_pdf in process_pdf_dir(input_dir):
        pdf_path, (cnum, fpage), tei = processed_pdf
        rec_dict = mapping.tei_to_dict(tei)
        rec_dict["pdf_path"] = pdf_path
        rec_dict["cnum"] = cnum
        rec_dict["fpage"] = fpage
        yield rec_dict


def write_jsons(input_dir):
    """Write json files. This is not the final product! Only for testing."""
    # NOTE: remove this function when finished
    for dic in build_dicts(input_dir):
        filename = dic["cnum"] + "_" + dic["fpage"] + ".json"
        target_folder = "tmp/"
        if not os.path.exists(target_folder):
            os.makedirs(target_folder)

        jsondic = json.dumps(dic, indent=4, sort_keys=True)
        with open(target_folder + filename, "w") as f:
            print(jsondic, file=f)


def write_xml(filename, cnum, marcxml):
    target_folder = "marc_records/" + cnum + "/"
    if not os.path.exists(target_folder):
        os.makedirs(target_folder)

    with open(target_folder + filename, "w") as f:
        print(marcxml, file=f)



def build_marc_xml(input_dir):
    """Build a MARCXML file from a HEPRecord dictionary."""
    marcdict = {}
    counter = 0
    for dic in build_dicts(input_dir):
        marcdict["100"] = []
        if dic.get("authors"):
            for aut in dic["authors"]:
                surname, given_names = utils.split_fullname(aut["name"], surname_first=True)
                fullname = u"{}, {}".format(surname, given_names)
                affiliations = []
                aff_raw = aut.get("affiliations")
                if aff_raw:
                    for aff in aff_raw:
                        affiliations.append(aff.get("value"))
                marcdict["100"].append({"u":affiliations, "a":fullname})
            
        marcdict["245"] = {"a": dic.get("title")}
        marcdict["520"] = {"a": dic.get("abstract")}
        marcdict["773"] = {"c": dic.get("fpage")+"-", "w":dic["cnum"]}
        marcdict["980"] = {"a": ["ConferencePaper", "CORE", "HEP"]}
        marcdict["FFT"] = {"a": dic["pdf_path"],
                           "d": "Fulltext",
                           "t": "INSPIRE-PUBLIC",
                           }
        
        # NOTE: we don't need the references at this point
        #marcdict["999C5"] = []
        #for ref in dic["references"]:
            ## FIXME: what should be done with `None` elements?
            #authors = ", ".join([aut["name"] for aut in ref["authors"]])
            ## FIXME: here should we use standard names and split the section?
            #title = ref["journal_pubnote"].get("journal_title", "") 
             ## See above, should the section be here?
            #volume = ref["journal_pubnote"].get("journal_volume", "") 
            #pages = ref["journal_pubnote"].get("page_range", "")
            #year = ref["journal_pubnote"].get("year", "")
            #pubnote = u"{},{},{}".format(title, volume, pages)
            #marcdict["999C5"].append({"s":pubnote, "y":year})

        marcxml = utils.legacy_export_as_marc(marcdict)
        print(marcxml)
        filename = dic["cnum"] + "_" + dic["fpage"] + ".xml"
        write_xml(filename, dic["cnum"], marcxml)
        counter += 1
    logger.info("Wrote " + str(counter) + " records.")  # FIXME: how to make this print also to screen!!?



import sys, getopt
def main(argv):
    input_dir = ''
    outputfile = ''
    helptext = "Usage: grobid_files.py -i <input_dir> \n"\
        "<input_dir> is the CNUM of the conference, e.g. `C12-03-10`"
    try:
        opts, args = getopt.getopt(argv,"hi:o:",["ifile=","ofile="])
    except getopt.GetoptError:
        print(helptext)
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print(helptext)
            sys.exit()
        elif opt in ("-i", "--ifile"):
            input_dir = arg
        elif opt in ("-o", "--ofile"):
            outputfile = arg

    if input_dir:
        input_dir = "/afs/cern.ch/project/inspire/uploads/library/moriond/for_grobid/" + input_dir
        if os.path.exists(input_dir):
            print('Processing directory (CNUM)"', input_dir + '"')
            build_marc_xml(input_dir)
        else:
            print("Path `"+ input_dir +"` doesn't exist!")
    else:
        print(helptext)

if __name__ == "__main__":
   main(sys.argv[1:])
