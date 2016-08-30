#!/bin/bash
# This script splits one pdf file to multiple files when page range is given.
# It will take two inputs: the file to split and the file which contains the 
# page ranges. Page range file should contain one range per line, e.g.:
# 17-34
# 35-44
# 45-57
#
# Example usage: 
# ./split_proceedings.sh test/test/C76-02-29_Proceedings.pdf  test/test/pageranges.txt
#
#
# Output files fill be generated to the same directory where the original file
# resides.
#
# For simplifying shell commands:
# alias moriond="workon grobid_proceedings && cdvirtualenv && cd src/grobid_proceedings"


args=("$@")

# Check if any arguments given
if [ $# -eq 0 ]; then
    echo "Usage: ./split_proceedings.sh <path to Proceedings.pdf>  <path to pageranges.txt>"
    exit 1
fi

PROCEEDINGS=${args[0]} # proceedings path
PAGEFILE=${args[1]} # page ranges file
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"  # current dir
PROCEEDINGS_DIR=$(dirname "${PROCEEDINGS}") # dir of the proceedings file
PROCEEDINGS_FILE=$(basename "${PROCEEDINGS}") # file name of the proceedings

#Get the CNUM out of the filename, it can be like:
# C16-02-01.pdf
# C16-02-01.1.pdf
# C16-02-01.1_Proceedings.pdf
regex='(C[0-9]{2}-[0-9]{2}-[0-9]{2}?.[0-9]).*.pdf'
if [[ $PROCEEDINGS_FILE =~ $regex ]]; then
    CNUM=${BASH_REMATCH[1]}
fi

# Page range files might be created in MS Windows, therefore we
# should convert the DOS-style CRLF line terminators to Unix line feeds:
NEWPAGEFILE=$PROCEEDINGS_DIR/pagerange_unix.txt
perl -lne 's/\r//g; print' $PAGEFILE > $NEWPAGEFILE
mv $NEWPAGEFILE $PAGEFILE
#Ensure the pagerange file ends in newline:
sed -i -e '$a\' $NEWPAGEFILE
    
echo Script dir: ${SCRIPT_DIR}
echo Proceedings dir: ${PROCEEDINGS_DIR}


Go through the page range list and split the pdf file accordingly
if [ -e $args ]; then
    echo Proceedings path: ${PROCEEDINGS}
    echo Page file path:   ${PAGEFILE}
    echo CNUM: ${CNUM}
    echo Splitting conference files...
    #Read the pagenumber file line by line:
    exec < $PAGEFILE
    while read LINE; do
        #Get firstpage:
        regex='([[:digit:]]*)-[[:digit:]]*'
        if [[ $LINE =~ $regex ]];then 
            FPAGE=${BASH_REMATCH[1]}
        fi
        # Create new file:
        outfile=$PROCEEDINGS_DIR/Pages\_from\_$CNUM\_$FPAGE.pdf
        echo pdftk $PROCEEDINGS cat $LINE output $outfile
        pdftk $PROCEEDINGS cat $LINE output $outfile
    done

else
    echo Files don\'t exists!
fi
