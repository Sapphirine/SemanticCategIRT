# http://www.wordfrequency.info/sample.asp#genre
# Short sample 6,000 entries every 10th word of 60,000 word list
# Ranks are therefore 1st, 11th, 21st, 31st, 41st, 51st ... 111th ...

import csv
import numpy as np
from nltk.stem.wordnet import WordNetLemmatizer
from nltk.stem.lancaster import LancasterStemmer
from nltk.stem.porter import PorterStemmer
from nltk.stem import SnowballStemmer
with open('../wfi-corpus-contemp-ameri-eng/genres_sample.csv','r') as csvfile:
    # open file for reading
    rd = csv.reader(csvfile.read().splitlines(),delimiter=',')
    # set for lemmas as given
    lemmas = set()
    lemmas_dict = {}
    # prepare nltk stemmers and lemmatizers
    lmtzr = WordNetLemmatizer()
    ss = SnowballStemmer('english')
    ps = PorterStemmer()
    ls = LancasterStemmer()
    # init sets for nltk proc lemmas
    lmtz_lemmas = set()
    lmtz_lemmas_dict = {}
    ss_lemmas = set()
    ss_lemmas_dict = {}
    ps_lemmas = set()
    ps_lemmas_dict = {}
    ls_lemmas = set()
    ls_lemmas_dict = {}
    # collect lemma_scores
    lemma_scores = np.zeros([6003,49]).astype(float)
    for i,ln in enumerate(rd):
        if i==0:
            genres = ln[3:]
            # print genres
        elif i==1:
            overal_frequencies = ln[4:10]
            # print overal_frequencies
        else:
            lemmas.add(ln[1])
            lemmas_dict[ln[1]]=i-2
            # lemmatize using wordnet (tree)
            lemmatized_lemma = lmtzr.lemmatize(ln[1])
            lmtz_lemmas.add(lemmatized_lemma)
            lmtz_lemmas_dict[lemmatized_lemma]=i-2
            # stem using algorithms: snowball
            ss_lemma = ss.stem(ln[1])
            ss_lemmas.add(ss_lemma)
            ss_lemmas_dict[ss_lemma]=i-2
            # porter stem
            ps_lemma = ps.stem(ln[1])
            ps_lemmas.add(ps_lemma)
            ps_lemmas_dict[ps_lemma]=i-2
            # lancaster stem
            ls_lemma = ls.stem(ln[1])
            ls_lemmas.add(ls_lemma)
            ls_lemmas_dict[ls_lemma]=i-2
            # 
            lemma_scores[i-2,:]=ln[3:]
            if not lemma_scores[i-2,4]==0.0:
                lemma_scores[i-2,7:18]=lemma_scores[i-2,7:18]/lemma_scores[i-2,1]#/lemma_scores[i-2,4]

            if not lemma_scores[i-2,5]==0.0:
                lemma_scores[i-2,18:26]=lemma_scores[i-2,18:26]/lemma_scores[i-2,1]#/lemma_scores[i-2,5]

            if not lemma_scores[i-2,6]==0.0:
                lemma_scores[i-2,26:35]=lemma_scores[i-2,26:35]/lemma_scores[i-2,1]#/lemma_scores[i-2,6]

            if not lemma_scores[i-2,2]==0.0:
                lemma_scores[i-2,35:44]=lemma_scores[i-2,35:44]/lemma_scores[i-2,1]#/lemma_scores[i-2,2]

            if not lemma_scores[i-2,3]==0.0:
                lemma_scores[i-2,44:49]=lemma_scores[i-2,44:49]/lemma_scores[i-2,1]#/lemma_scores[i-2,3]
            # done . . . . . .
    max_lemma_scores = lemma_scores[:,7:].max()
    print
    print 'MAX_LEMMA_SCORES'
    print max_lemma_scores
    print
    # print lemma_scores[:,7:]
# precompiledGenrSubScor . . .
# : lemmas . . . . . . . . . .
# : lemma_scores . . . . . . .
# : genres . . . . . . . . . .
# : overal_frequencies . . . .

""" 
                        Skipping 0,1,2 of ln (i.e. T{ ln_idx = idx-3 } )
    [ 'DISPERSION',0
        'TOT FREQ',1
        'SPOKEN',2
        'FICTION',3
        'POP MAG',4
        'NEWSPAPER',5
        'ACADEMIC',6

        7:18(17)
        'MAG:News/Opin', 'MAG:Financial', 'MAG:Sci/Tech', 'MAG:Soc/Arts',
        'MAG:Religion', 'MAG:Sports', 'MAG:Entertain', 'MAG:Home/Health',
        'MAG:Afric-Amer', 'MAG:Children', 'MAG:Women/Men',

        18:26(25)
        'NEWS:Misc', 'NEWS:News_Intl', 'NEWS:News_Natl', 'NEWS:News_Local',
        'NEWS:Money', 'NEWS:Life', 'NEWS:Sports', 'NEWS:Editorial',

        26:35(34)
        'ACAD:History', 'ACAD:Education', 'ACAD:Geog/SocSci', 'ACAD:Law/PolSci',
        'ACAD:Humanities', 'ACAD:Phil/Rel', 'ACAD:Sci/Tech', 'ACAD:Medicine', 'ACAD:Misc',

        35:44(43)
        'SPOK:ABC', 'SPOK:NBC', 'SPOK:CBS', 'SPOK:CNN', 'SPOK:FOX', 'SPOK:MSNBC',
        'SPOK:PBS', 'SPOK:NPR', 'SPOK:Indep',

        44:49(48)
        'FIC:Gen (Book)', 'FIC:Gen (Jrnl)', 'FIC:SciFi/Fant', 'FIC:Juvenile', 'FIC:Movies']
"""

# Note: all frequencies normalized to total appearences of 'the'
# Subsequently, max normalized frequency saved over reference
# Then max normalized detected subset altogether
# Normalize by all subset scores (0.1 range low scores) by average of (cumsum(0.1r)+{1 obviously since max/max == 1})/2

