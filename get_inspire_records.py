# -*- coding: utf-8 -*-

"""
Get all the Inspire records for a given query.

The query will break the full list down to `list_size` (e.g. 50 or 250)
records per file. The search can be continued with keyword `jrec`.
All the results are kept on separate files. Result format is MARCXML.

You can use this as a stand-alone script or a module in another python code.
`fetch_records` can either return list if XML record strings or write the strings
to files and return a list of filepaths.

Example usage:
    python get_inspire_records.py -p 'tc proceedings and 773__p:Nucl.Instrum.Meth.' -o 'inspire_xmls'

"""

from __future__ import print_function

import os
import sys
import re
import getopt

from tempfile import mkstemp

import getpass
# import logging  # FIXME: do we want fancy logging?

from lxml import etree

import splinter
from splinter.exceptions import ElementDoesNotExist

from invenio_client import InvenioConnector

class FixedConnector(InvenioConnector):
    """By default InvenioConnector is using phantomjs, which doesn't work.

    Have to override the browser initialization here.
    $ pip install splinter[zope.testbrowser]
    """
    def _init_browser(self):
        """Overide in appropriate way to prepare a logged in browser."""
        self.browser = splinter.Browser('zope.testbrowser')
        self.browser.visit(self.server_url + "/youraccount/login")
        try:
            self.browser.fill('nickname', self.user)
            self.browser.fill('password', self.password)
        except ElementDoesNotExist:
            self.browser.fill('p_un', self.user)
            self.browser.fill('p_pw', self.password)
        # FIXME: what is this and why it doesn't work:
        # self.browser.fill('login_method', self.login_method)
        self.browser.find_by_css('input[type=submit]').click()

def fetch_records(inspire_pattern, list_size, outdir=None):
    """Get records from Inspire with InvenioConnector and write to file."""
    if outdir and not os.path.exists(outdir):
        os.makedirs(outdir)
    files_created = []
    records_fetched = []

    def write_to_file(records, startpoint):
        """Write records to file.

        Ideally n_records == list_size
        """
        n_records = get_number_of_records_in_batch(records)
        _, outfile = mkstemp(prefix="records" + str(startpoint) + "_", dir=outdir, suffix=".xml")
        with open(outfile, "w") as f:
            f.write(records)
        print("Wrote " + str(n_records) + " INSPIRE records to file " + outfile)
        files_created.append(outfile)

    def move_to_next_startpoint(startpoint, list_size):
        """Increment startpoint counter.

        Startpoint is used when INSPIRE breaks the result list to multiple
        batches of list_size results.
        """
        return startpoint + list_size + 1

    if "*" in inspire_pattern:
        # Have to add `wl=0` to make wildcards function properly.
        # This requires authentication.
        uname = raw_input("Inspire login: ")
        pword = getpass.getpass()
        inspire = FixedConnector(
            "https://inspirehep.net",
            user=uname,
            password=pword
            )
    else:
        inspire = FixedConnector("https://inspirehep.net")


    # Get the first batch
    startpoint = 0
    records = inspire.search(
        p=inspire_pattern,
        of="xm",
        rg=list_size,
        wl=0
        )

    # Get total number of search results
    total_amount = get_total_number_of_records(records)
    if not total_amount:
        print("No records found.")
        sys.exit()
    print("Total amount of results: " + total_amount)

    # Get all the rest
    while startpoint < int(total_amount):
        if outdir:
            write_to_file(records, startpoint)
        else:
            records_fetched.append(records)
        startpoint = move_to_next_startpoint(startpoint, list_size)

        records = inspire.search(
            p=inspire_pattern,
            of="xm",
            rg=list_size,
            jrec=startpoint,
            wl=0)

    if outdir:
        return files_created
    else:
        return records_fetched



def get_number_of_records_in_batch(records_string):
    """Get the number of record nodes in an XML string."""
    collection = etree.fromstring(records_string)
    return len(collection.xpath("//*[local-name()='record']"))

def get_total_number_of_records(records_string):
    """Get the total number of search results."""
    collection = etree.fromstring(records_string)
    for comment in collection.xpath("//comment()"):
        if "Search-Engine-Total-Number-Of-Results" in comment.text:
            tot_num = re.search(r'.?Results:\s(\d+).?', comment.text).group(1)
            return tot_num

def main(argv=None):
    """
    You have to input inspire search pattern or path to file of recids.
    Optional arguments are output directory and size of one result page.
    """
    if argv is None:
        argv = sys.argv

    outdir = "inspire_xmls/"
    list_size = 50
    inspire_pattern = ""
    helptext = 'USAGE: \n\t python get_inspire_records.py -p <pattern> [-o <outdir> -r <recid_file>]'

    # Parse search pattern and optional output dir from the arguments
    try:
        opts, _ = getopt.getopt(
            argv,
            "ho:p:r:l:",
            ["outdir=", "pattern=", "recid_file=", "list_size="]
        )
    except getopt.GetoptError:
        print(helptext)
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print(helptext)
            sys.exit()
        elif opt in ("-o", "--ofile"):
            outdir = os.path.join(arg, '')
        elif opt in ("-l", "--list_size"):
            list_size = arg  # should be int
        elif opt in ("-p", "--pattern"):
            inspire_pattern = arg
        elif opt in ("-r", "--recid_file"):
            with open(arg, "r") as f:
                recids = f.read().split()
            if recids:
                inspire_pattern = "recid " + " or ".join(recids)
    if not argv:
        print(helptext)
        sys.exit()
    if not inspire_pattern:
        print("Search pattern is required.")
        sys.exit(2)
    print('Output dir is ' + outdir)
    print("Inspire search pattern: " + inspire_pattern)

    # TEST pattern:
    # inspire_pattern = 'tc proceedings and 773__p:Nucl.Instrum.Meth.'
    fetch_records(inspire_pattern, list_size, outdir=outdir)


if __name__ == "__main__":
    main(sys.argv[1:])
