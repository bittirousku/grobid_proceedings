# grobid_proceedings #

Small script to grobid Moriond proceedings pdfs. 

As an input it will accept a directory containing Moriond pdfs.

It will extract information from the pdfs and the pdf file names. After that it will create a MARCXML HEPRecords for all suitable pdf files. These XML files can then be uploaded to Inspire.

Note that the pdf directory should be in a place that the Inspire can accessa (e.g. AFS).


