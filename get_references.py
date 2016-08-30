"""
With this you can get references for a given record

*First get all the records with an inspire query.
*Check which don't have references.
*Get their fpage and recid

*With the fpage locate the corresponding pdf file
*Extract references from the pdf
*Create XMLs with the references and the recid for appending.


"""
from __future__ import print_function

import re
import os
import sys

from tempfile import mkstemp

from lxml import etree

from refextract import extract_references_from_file

from get_inspire_records import fetch_records
from grobid_proceedings import (
    # mapping,
    utils,
    )



PDF_FILE_PATTERN = re.compile(r'^C\d\d-\d\d-\d\d_(\d+)-\d+.*pdfa?$')
# Extract volume and fpage from unrecognized "misc" pubstring
# example: 'NI.M. 591, 453 (2008)'
ref_pubstring_pattern = re.compile(r'^.*NI.M.\s(\w*\d+),\s(\d+)\s\((\d+)\)')
# Collaboration pattern:
# e.g. [CMS collaboration]
COLLABORATION_PATTERN = re.compile(r'^.*?\s*[\[\(]*(\w+)\s[cC]ollaboration.*')

def load_xml_files(inspire_xml_paths):
    """Load existing xml files to etree objects."""
    collections = []
    for xml_file in inspire_xml_paths:
        with open(xml_file, "r") as f:
            collections.append(etree.parse(f))
    return collections

def load_xml_strings(inspire_xml_strings):
    collections = []
    for collection in inspire_xml_strings:
        collections.append(etree.fromstring(collection))
    return collections

def marc_to_dict(node, tag):
    """Convert a MARCXML node to a dictionary."""
    marcdict = {}
    marc_node = node.xpath("./*[local-name()='datafield'][@tag='"+ tag +"']")
    subfields = marc_node[0].xpath("./*[local-name()='subfield']")
    for subfield in subfields:
        try:
            # There might be empty subfields
            dkey = subfield.xpath("@code")[0]
            dvalue = subfield.xpath("text()")[0]
        except IndexError:
            continue
        marcdict[dkey] = dvalue

    return marcdict

def find_local_files(directory):
    """Return the contents with full paths of a directory."""
    return [os.path.join(directory, f) for f in os.listdir(directory)]

def get_inspire_collections(inspire_pattern=None, inspire_outdir=None, indir=None):
    """Fetch stuff from Inspire using invenioclient."""
    collections = []
    if inspire_outdir:
        inspire_xml_paths = fetch_records(inspire_pattern, 100, outdir=inspire_outdir)
        collections = load_xml_files(inspire_xml_paths)
    elif indir:
        inspire_xml_paths = find_local_files(indir)
        collections = load_xml_files(inspire_xml_paths)
    else:
        inspire_xml_strings = fetch_records(inspire_pattern, 50)
        collections = load_xml_strings(inspire_xml_strings)

    return collections


def find_file(fpage):
    """Find the pdf file with given fpage."""
    directory = "test/lake"
    all_files = os.listdir(directory)
    for pdf in all_files:
        result = PDF_FILE_PATTERN.search(pdf)
        if result:
            if result.group(1) == fpage:
                return pdf
    return ""


def get_references(collections):
    """Go through all the records and determine if they have references already."""
    all_the_references = []
    for collection in collections:
        for record in collection.xpath("//*[local-name()='record']"):
            # import ipdb; ipdb.set_trace()
            recid = record.xpath("./*[local-name()='controlfield'][@tag='001']/text()")
            refs = record.xpath("./*[local-name()='datafield'][@tag='999']")
            if refs:
                # Skip if references exist
                continue

            page_range = record.xpath("./*[local-name()='datafield'][@tag='773']/*[local-name()='subfield'][@code='c']/text()")[0]
            if "-" in page_range:
                fpage = page_range.split("-")[0]
            else:
                fpage = page_range
            fpage = filter(lambda x: x.isdigit(), fpage)
            # then get the filename corresponding to this fpage

            # fpage = "1"  # HACK: remove this when done testing!!
            pdf_file = find_file(fpage)
            if pdf_file:
                fullpath = os.path.join("test/lake", pdf_file)
                metadata_from_refextract = extract_references_from_file(fullpath)
                marcxml = convert_dict_to_marc(metadata_from_refextract, recid, marcxml=True)
                # print(marcxml)
                yield marcxml
            else:
                print("didn't find pdf.")



def convert_dict_to_marc(dic, recid, marcxml=False):
    """Convert dict from refextract to a dict of MARC or to MARCXML string."""
    marcdict = {}
    marcdict["001"] = recid
    marcdict["999C5"] = []
    for ref in dic["references"]:
        refdict = {}
        ref = clean_reference(ref)
        if ref.get("author"):
            authors = ", ".join([aut for aut in ref["author"]])
            refdict["h"] = authors
        journal_title = ref.get("journal_title") or ""
        if journal_title and journal_title[0] == "N.I.M.":
            journal_title = ["Nucl.Instrum.Meth."]
        volume = ref.get("journal_volume", "")
        pages = ref.get("journal_page", "")
        reportnumber = ref.get("reportnumber", "")
        pubstring = ""
        if journal_title and volume and pages:
            journal_title = "".join(journal_title[0].split())
            pubstring = u"{},{},{}".format(journal_title, volume[0], pages[0])
            refdict["s"] = pubstring

        refdict["m"] = ref.get("misc", "")
        refdict["o"] = ref.get("linemarker", "")
        refdict["r"] = reportnumber
        refdict["v"] = volume
        refdict["y"] = ref.get("year", "")
        refdict["c"] = ref.get("collaboration", "")
        refdict["t"] = ref.get("title", "")


        # Get also the recid of reference records:
        reference_recids = []
        if pubstring:
            pubstring_pattern = "773__p:" + journal_title + " 773__v:" + volume[0] + " 773__c:" + pages[0] + "*"
            reference_recids = get_recid_for_record(pubstring_pattern)
        if not reference_recids and reportnumber:
            reference_recids = get_recid_for_record(reportnumber[0])
        if reference_recids:
            refdict["0"] = reference_recids

        # refdict = {"c":pages, "m": misc, "p":journal_title, "v":volume,  "y":year}
        marcdict["999C5"].append(refdict)


    if marcxml:
        return utils.legacy_export_as_marc(marcdict)
    else:
        return marcdict



def get_recid_for_record(inspire_pattern):
    from invenio_client import InvenioConnector
    inspire = InvenioConnector("https://inspirehep.net")
    reference_recids = inspire.search(p=inspire_pattern,of="id")

    return reference_recids


def clean_reference(ref):
    """Clean various values in reference dictionary.

    Also try to get as much useful data out of 'misc' key as possible.
    """

    # TODO: recognise dates

    if "misc" in ref:
        for misc in ref["misc"]:
            # Extract collaboration info:
            if "collaboration" in misc.lower() and "c" not in ref:
                collaboration_search_result = COLLABORATION_PATTERN.search(misc)
                if collaboration_search_result:
                    collaboration = collaboration_search_result.group(1)
                    if "Auger" in collaboration:
                        collaboration = "Pierre Auger"
                    ref["collaboration"] = collaboration + " Collaboration"
            if "Pierre Auger Collaboration" in misc:
                ref["collaboration"] = "Pierre Auger Collaboration"
            if "NI.M." in misc and "journal_volume" not in ref:
                search_result = ref_pubstring_pattern.search(misc)
                if search_result:
                    volume, fpage, year = search_result.groups()
                    ref["journal_title"] = ["N.I.M."]
                    ref["journal_volume"] = [volume]
                    ref["journal_page"] = [fpage]
                    ref["year"] = [year]

    if "reportnumber" in ref:
        old_reportnos = ref["reportnumber"]
        new_reportnos = []
        for reportno in old_reportnos:
            # Fix arxiv report numbers:
            if "astro-ph" in reportno:
                reportno = reportno.replace(" [astro-ph]", "")
                if "arXiv" not in reportno:
                    new_reportnos.append("arXiv:" + reportno)
                else:
                    new_reportnos.append(reportno)
            elif "hep-ph" in reportno:
                reportno = reportno.replace(" [hep-ph]", "")
                if "arXiv" not in reportno:
                    new_reportnos.append("arXiv:" + reportno)
                else:
                    new_reportnos.append(reportno)
            else:
                new_reportnos.append(reportno)
        if new_reportnos:
            ref["reportnumber"] = new_reportnos

    return ref



def write_to_file(references_xml):
    """Write the references to a file."""
    _, outfile = mkstemp(prefix="append" + "_",
                         dir="tmp/append/",
                         suffix=".xml")
    counter = 0
    line = \
    '<collection>\n'
    for record in references_xml:
        line += record
        counter += 1
    line += \
    '</collection>'

    with open(outfile, "w") as f:
        f.write(line)
    print("Wrote " + str(counter) + " records to file: " + outfile)




inspire_pattern_for_all_lake_papers = "773__w:C08/02/18 or 773__w:C08-02-18 and 980__a:ConferencePaper"
inspire_outdir = "tmp/inspire_xmls"
# collections = get_inspire_collections(inspire_pattern_for_all_lake_papers, inspire_outdir)  # Fetch new stuff.
collections = get_inspire_collections(indir=inspire_outdir)  # Use already fetched stuff.
references_xml = get_references(collections)
write_to_file(references_xml)
