# For search engine text result processing
import re
#
stopword_set = set() # initialize a set of the stop words
with open("STOPWORD.list") as f:
    for line in f:
        stopword_set.add(line.strip())
# add punctuation
stopword_set.update(["''",",",".","``","'","!",'"',"#","$","%","&","(",")","*","+","-","/",
    ":",";","<","=",">","?","@","[","\\","]","^","_","`","{","|","}","~"])
#
punct_set = set()
punct_set.update(["''",",",".","``","'","!",'"',"#","$","%","&","(",")","*","+","-","/",
    ":",";","<","=",">","?","@","[","\\","]","^","_","`","{","|","}","~"])
##
conversational_set = set()
conversational_set.update(["(",")",".","..","...","!","?"]) # no comma
#
prose_set = set()
prose_set.update([".",",",";","[","]","(",")","-"])
#
explanatory_set = set()
explanatory_set.update([":","''","``","'",'"',"(",")","[","]","`","{","}","/"])
#
email_url_set = set()
email_url_set.update(["@","/","//",".","%","_"])
#
math_set = set()
math_set.update(["*","+","-","<","=",">","[","]","{","}","/","(",")","|","^","~","%"])
#
nonstandard_set = set()
nonstandard_set.update(["#","$","&","*","\\","^","`","|","~","<",">"])
##
stopword_alphaset = set()
with open("STOPWORD.list") as f:
    for line in f:
        stopword_alphaset.add(line.strip())
#
cid_pat = re.compile(r"\(cid:\d+\)")
hyphenline_pat = re.compile(r"-\s*\n\s*")
multiwhite_pat = re.compile(r"\s+")
nonlet = re.compile(r"([^A-Za-z0-9 ])")
punctuation_pat = re.compile(r"""([!"#$%&\'()*+,-./:;<=>?@[\\\]^_`{|}~])""")
