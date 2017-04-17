curl https://www.fda.gov/Drugs/InformationOnDrugs/ucm079750.htm | grep "Drugs@FDA Download File\&nbsp"| perl -ne 'print "https://www.fda.gov.$1" if /href=\"(\S+)\"/' | xargs curl -o drugsAtfda.zip
