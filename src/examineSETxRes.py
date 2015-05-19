# examineSearchEngineTextResults.py
import re
from precompiledRegex import *
from precompiledGenrSubScor import *
from pyspark import SparkContext
from pyspark.mllib.feature import HashingTF, IDF
from pyspark.mllib.feature import Word2Vec
import numpy as np
from nltk.stem.wordnet import WordNetLemmatizer
from nltk.stem.lancaster import LancasterStemmer
from nltk.stem.porter import PorterStemmer
from nltk.stem import SnowballStemmer
import random
from pyspark.mllib.clustering import KMeans
from math import sqrt
from gensim import corpora
import pickle

# Start a spark distributed ops context
sc = SparkContext(appName='info-globulator')

def exfiltrate_fields(results, which):
    results_split = results.split('-=-=-')
    field_ti = 'TITLE:'
    field_sn = 'SNIPPET:'
    field_arg = 'GOOGLE ARTICLE'
    field_arb = 'BING ARTICLE'
    field_arw = 'WEBHOSE ARTICLE'
    field_tx = 'TEXT:'
    # collect text
    tx_list = []
    at_index = 0
    num_text_fields = 0
    num_gathered_text_fields = 0
    num_title_fields = 0
    num_snippet_fields = 0
    num_article_fields = 0
    num_misalignments = 0
    num_split_fields = len(results_split)
    print 'There are %i split fields'%num_split_fields
    print 'Expecting approximately %i pages.'%(num_split_fields/4)
    for split_string in results_split:
        if split_string=='':
            continue
        elif split_string.startswith(field_tx):
            num_text_fields+=1
            if which % 2 == 0:
                tx_list.append(split_string[len(field_ti):])
                num_gathered_text_fields+=1
            if at_index % 4 != 3:
                print 'Error: Misalignment at text %i.'%at_index
                print split_string
                num_misalignments+=1
            at_index+=1
        elif split_string.startswith(field_ti):
            num_title_fields+=1
            if which % 3 == 0:
                ti_list.append(split_string)
            if at_index % 4 != 1:
                print 'Error: Misalignment at title %i.'%at_index
                print split_string
                num_misalignments+=1
            at_index+=1
        elif split_string.startswith(field_sn):
            num_snippet_fields+=1
            if which % 5 == 0:
                sn_list.append(split_string)
            if at_index % 4 != 2:
                print 'Error: Misalignment at snippet %i.'%at_index
                print split_string
                num_misalignments+=1
            at_index+=1
        elif split_string.startswith(field_arg) or \
             split_string.startswith(field_arb) or \
             split_string.startswith(field_arw):
            num_article_fields+=1
            if which % 7 == 0:
                ar_list.append(split_string)
            if at_index % 4 != 0:
                print 'Error: Misalignment at article #id index %i.'%at_index
                print split_string
                num_misalignments+=1
            at_index+=1
        else:
            print 'Error: Unkown field. Expected field{%i} at index %i'%(at_index%4, at_index)
    print 'Gathered text from a total of %i pages.'%num_gathered_text_fields
    print 'Counted a total of %i text fields.'%num_text_fields
    print 'Counted a total of %i article urls.'%num_article_fields
    print 'Counted a total of %i titles.'%num_title_fields
    print 'Counted a total of %i snippets.'%num_snippet_fields
    print 'Counted a total of %i misalignment errors.'%num_misalignments
    print 'Checksum: %i'%(1+num_text_fields+num_article_fields+num_title_fields+num_snippet_fields)
    return_list = []
    if which % 2 == 0:
        return_list.append(tx_list)
    if which % 3 == 0:
        return_list.append(tx_list)
    if which % 5 == 0:
        return_list.append(tx_list)
    if which % 7 == 0:
        return_list.append(tx_list)
    if len(return_list)==1:
        return_list = return_list[0]
    return return_list

def preprocess(text):
    text = text.decode("utf-8")
    # normalize to lower case
    text = text.lower()
    # regex proc
    text = hyphenline_pat.sub("", text) # replace hyphenlines
    text = punctuation_pat.sub(r" \1 ", text) # pad 1st bt match with(in) ws
    text = re.sub("\n"," NL ", text) # replaces special character NL
    text = nonlet.sub(r" \1 ", text) # pad 1st bt match with(in) 1ws's
    text = multiwhite_pat.sub(" ", text) # replace multiple by 1 ws
    save_len = 0
    while (save_len != len(text)):
        save_len = len(text)
        text = text.replace("NL NL","NL")
    text = text.strip().encode('utf-8') # strip for ws (add more? e.g NL at start/end)
    return text

def recover_encoding(collected_regex_proc_rdd_list):
    for did in xrange(len(collected_regex_proc_rdd_list)):
        collected_regex_proc_rdd_list[did]=\
            collected_regex_proc_rdd_list[did].decode('utf-8').encode('utf-8')
    return collected_regex_proc_rdd_list

def return_words(doc):
    doc_withno_punct_stopwords = []
    for word in doc.split():
        if word.isalpha() or word.isalnum() and not word.isdigit(): # <<<here<<<
            if len(word) > 1:
                if word not in stopword_set and word != 'NL':
                    doc_withno_punct_stopwords.append(word)
    return doc_withno_punct_stopwords # returns split text

def check(doc):
    doc = doc[1]
    doc_withno_punct_stopwords = []
    for word in doc.split():
        if word.isalpha(): # <<<not here<<<
            if len(word) > 1:
                if word not in stopword_set and word != 'NL': # ++if not in proper dictionary incl. common abbrevs
                    doc_withno_punct_stopwords.append(word)
    if doc_withno_punct_stopwords: # returns split text
        return True
    else:
        return False

def genre_score(doc,type2=False):
    ##############################
    lmtzr = WordNetLemmatizer()
    ss = SnowballStemmer('english')
    ps = PorterStemmer()
    ls = LancasterStemmer()
    ##############################
    doc = doc.split()
    ##############################
    punctuation_words = 0
    special_characters = 0
    current_date_words = 0 # for years
    date_compliant_words = 0 # for years
    digital_words = 0
    english_words = 0
    stop_words = 0
    newlines = 0
    ##############################
    if (type2):
        conversational_score = 0
        prose_score = 0
        explanatory_score = 0
        email_url_score = 0
        math_score = 0
        nonstandard_score = 0
    ##############################
    scores = np.zeros(49)
    ##############################
    for w in xrange(len(doc)):
        # Whatis word:
        word = doc[w]

        # If hyphenated:
        hyphenated = False
        if w > 0 and w < len(doc)-1: # i.e. w<=len(doc)-2
            if doc[w]=='-' and doc[w-1].isalnum() and doc[w+1].isalnum():
                hyphenated = True
                word = ''.join(doc[w-1:w+2])
                punctuation_words += 1

        # Start scoring:
        if not hyphenated and not word.isalnum():

            if word in punct_set:
                punctuation_words += 1
                #print 'Punct-char: "%s"'%word
                if (type2):
                    if w>0:
                        if word=='.' and doc[w-1]=='.':
                            word = ''.join(doc[w-1:w+1])
                        if word=='/' and doc[w-1]=='/':
                            word = ''.join(doc[w-1:w+1])
                    if word in conversational_set:
                        conversational_score += 1
                    if word in prose_set:
                        prose_score += 1
                    if word in explanatory_set:
                        explanatory_score += 1
                    if word in email_url_set:
                        email_url_score += 1
                    if word in math_set:
                        math_score += 1
                    if word in nonstandard_set:
                        nonstandard_score += 1

            else:
                special_characters += 1
                #print 'Speci-char: "%s"'%word

        elif not word.isdigit(): # if hyphernated or word.isalnum()

            if word in stopword_alphaset:
                stop_words += 1
            elif word=='NL':
                newlines += 1
                continue
            else:
                english_words += 1

            ####################################################################
            lmtzr_match = -1
            ss_match = -1
            ps_match = -1
            ls_match = -1
            max_sim = 0
            max_match = ''
            lemword = lmtzr.lemmatize(word)
            ssword = ss.stem(word)
            psword = ps.stem(word)
            lsword = ls.stem(word)
            if lemword in lemmas:
                lmtzr_match = lemmas_dict[lemword]
            if lemword in lmtz_lemmas:
                lmtzr_match = lmtz_lemmas_dict[lemword]
            if ssword in lemmas:
                ss_match = lemmas_dict[ssword]
            if ssword in ss_lemmas:
                ss_match = ss_lemmas_dict[ssword]
            if psword in lemmas:
                ps_match = lemmas_dict[psword]
            if psword in ps_lemmas:
                ps_match = ps_lemmas_dict[psword]
            if lsword in lemmas:
                ls_match = lemmas_dict[lsword]
            if lsword in ls_lemmas:
                ls_match = ls_lemmas_dict[lsword]

            if lmtzr_match>-1:
                scores += lemma_scores[lmtzr_match,:]
            elif ss_match>-1:
                scores += lemma_scores[ss_match,:]
            elif ps_match>-1:
                scores += lemma_scores[ps_match,:]
            elif ls_match>-1:
                scores += lemma_scores[ls_match,:]
            #print 'Word-detected-: "%s"'%word
            #print 'Lemmatization-: "%s"'%lemword
            #print 'Stem-agreement: "snowb" %i: "porter" %i: "lancaster" %i'%(ss_match,ps_match,ls_match)

        else: # if word.isdigit()
            assert(word.isdigit())
            if len(word)==4 and (word[0:2]==19 or word[0:2]==20):
                current_date_words += 1
                #print 'Year-detected: "%s"'%word
            elif len(word)==4 and word[0:2]<21:
                date_compliant_words += 1
                #print 'Historical-detected: "%s"'%word
            else:
                digital_words += 1
                #print 'Digits-detected: "%s"'%word

    checksum = punctuation_words+special_characters+current_date_words \
        +date_compliant_words+digital_words+english_words+stop_words+newlines
    #print 'Checksum==%i: doc-length==%i'%(checksum,len(doc))

    frequencies = np.zeros(10)
    frequencies[0] = float(checksum)
    frequencies[1] = float(punctuation_words)/float(checksum)
    frequencies[2] = float(english_words)/float(checksum)
    frequencies[3] = float(stop_words)/float(checksum)
    if punctuation_words>0:
        frequencies[4] = float(english_words+stop_words)/float(punctuation_words)
    #
    frequencies[5] = float(special_characters)/float(checksum)
    #
    frequencies[6] = float(digital_words)/float(checksum)
    frequencies[7] = float(current_date_words)/float(checksum) # for years
    frequencies[8] = float(date_compliant_words)/float(checksum) # for years
    #
    frequencies[9] = float(newlines)/float(checksum)
    if (type2):
        frequencies = np.append(frequencies,np.zeros(6))
        # won't sum up to 1
        if punctuation_words > 0:
            frequencies[10]=float(conversational_score)/float(punctuation_words)
            frequencies[11]=float(prose_score)/float(punctuation_words)
            frequencies[12]=float(explanatory_score)/float(punctuation_words)
            frequencies[13]=float(email_url_score)/float(punctuation_words)
            frequencies[14]=float(math_score)/float(punctuation_words)
            frequencies[15]=float(nonstandard_score)/float(punctuation_words)
    return_item = np.append(frequencies, 2.0*scores/(scores.max()+max_lemma_scores))
    # print
    # print 'SCORES MAX (even if distributed computing)'
    # print scores.max()
    # print
    # Should scale spok-fict-mag-news-ac freq by \
    # Get max-max all scores and min-max all scores ->
    # Get max all scores and use .max() from score here
    # Scale max by max; scale smaller max neither by max nor .max() but
    # something in between: scale by (max+.max())/2 (all inverse)
    # Or choose /dividend between 1->2 for range of max: 0.5->1
    return return_item

def process_doc2vec_word_counts(doc2vec,normalizer=0):
    if normalizer!=0:
        doc2vec[0]=float(doc2vec[0])/float(normalizer)
        return doc2vec
    return doc2vec[1:]

def distributed_ops( corpus, sanit=False, recall=False, corpred=False, \
                     streams=False, segred=False, tfidf=False, lda=False, \
                     word2vec=False, fin=None, segclust=None):

    # Return item for end results
    return_list = []

    ##########################################

    # Default actions:
    if (segred):
        zipped_corpus = zip(segclust,corpus)
        #print zipped_corpus
    corpus = sc.parallelize(corpus).cache()

    if (sanit or recall):
        corpus = corpus.map(lambda doc: preprocess(doc))
        # Here we "recover all" text, after having removed multi-ws & ws-pad punctuation
        # & replace \n by NL etc... (see function "preprocess" above)
        # We use the same regex sub/filtration rules as in the implementation found
        # @ https://github.com/alexalemi/segmentation (from which we got files in
        # directory: representation.py, tools.py and splitters.py, and which
        # segmentSETxRes.py is based on)
        if (recall):
            return_list.append(recover_encoding(corpus.collect()))

    # Here we return only potentially "meaningful words" - see function "return_words" above
    # Keeps alpha-numeric (removes numeric and non-alphabetical/alphanumeric)
    corpus_distrib = corpus.map(lambda doc: return_words(doc))
    print 'Original number of docs in corpus {filtering *docs* for alpha(+alphanumeric)-only words}: %i'%corpus_distrib.count()
    
    # merge corpus docs into one continuous split text
    corpus_merge = []
    corpus_collect = corpus_distrib.collect() # rdd2list
    for list_of_words in corpus_collect:
        corpus_merge.extend(list_of_words) # list-of-wordslist2{single-wordslist}
    
    # use numpy functions to sort dict words based on term-frequency
    corpus_merge_array = np.array(corpus_merge)
    corpus_merge_sorted = np.sort(corpus_merge_array)
    corpus_merge_unique, counts = np.unique(corpus_merge_sorted,return_counts=True)
    sort_ixs = np.argsort(counts)[::-1]
    counts = counts[sort_ixs]
    corpus_merge_unique = corpus_merge_unique[sort_ixs]
    return_list.append(corpus_merge_unique)
    return_list.append(counts)
    print
    for i,w in enumerate(corpus_merge_unique):
        print ('Counted word "%s" _%i_ many times.'%(w,counts[i]))
    print

    #########################################################################################
    # Next we split the text based on "verbosity/density/sparsity" as would
    # befit an articulate document (i.e. articles/papers/journal entries)
    # or more conversational/blog-entry-like/Q&A style/headings-only-
    # -retrieved website results.
    def error(point):
        center = clusters.centers[clusters.predict(point)]
        return sqrt(sum([x**2 for x in (point-center)]))

    # The following will further sanitize text.
    if (corpred):
        # Use pretrained term frequencies:
        # Experimentally, the following clustering has helped us get rid of
        # irrelevant search engine text results.
        corpus2vec = corpus.map(lambda doc: genre_score(doc,type2=False))
        corpus2vec = corpus2vec.map(lambda doc: process_doc2vec_word_counts(doc)).cache()
        # print 'Corpus vectorized'
        # collected = corpus2vec.collect()
        tempor = corpus.collect()
        print
        print
        for i,vec in enumerate(corpus2vec.collect()):
            print 'Got vecs:'
            print vec
            print 'Of text:'
            print tempor[i].split()
            print
        print

        # choose 5 clusters
        clusters = KMeans.train(corpus2vec, 5, maxIterations=90, runs=10, initializationMode="k-means||")
        WSSE = corpus2vec.map(lambda point: error(point)).reduce(lambda x,y: x+y) # cumsum
        print
        print 'Within Set Sum of Squared Error = ' + str(WSSE)
        print 'The cluster centers:'
        print clusters.centers
        print
        print
        return_list.append(corpus2vec.map(lambda pt: clusters.predict(pt)).collect())

    # The following will cluster for article length + content
    if (streams):
        corpus2vec = corpus.map(lambda doc: genre_score(doc,type2=True))
        temple = corpus.collect()
        print
        print
        for i,vec in enumerate(corpus2vec.collect()):
            print 'Got vecs:'
            print vec
            print 'Of text:'
            print temple[i].split()
            print
        print
        sumall = corpus2vec.reduce(lambda vecx,vecy: np.array([vecx[0]+vecy[0]]))
        corpus2vec = corpus2vec.map(lambda doc: process_doc2vec_word_counts(doc,normalizer=sumall)).cache()
        #
        clusters = KMeans.train(corpus2vec, 5, maxIterations=90, runs=10, initializationMode="k-means||")
        WSSE = corpus2vec.map(lambda point: error(point)).reduce(lambda x,y: x+y) # cumsum
        print
        print 'Within Set Sum of Squared Error = ' + str(WSSE)
        print 'The cluster centers:'
        print clusters.centers
        print
        print
        return_list.append(corpus2vec.map(lambda pt: clusters.predict(pt)).collect())

    #########################################################################################

    # Here we want to remove documents from the corpus which do not contain
    # 'english' dictionary words at all, or words that can be word2vec transformed
    # and "synonimized".
    if (segred):
        corpus_english_prose = sc.parallelize(zipped_corpus).filter(lambda doc: check(doc))
        zipped_corpus = zip(*corpus_english_prose.collect())
        red_clusts = list(zipped_corpus[0])
        red_text = recover_encoding(list(zipped_corpus[1]))
        return_list.append(red_clusts)
        return_list.append(red_text)
        print 'Number of docs in corpus {filtering *corpus* for alpha(+alphanumeric)-only words}: %i'%corpus_english_prose.count()

        f1 = open(''.join([filename,'-document_clusters.txt']),'w')
        f1.write('\n'.join(map(str,red_clusts)))
        f1.close()
        f2 = open(''.join([filename,'-documents_sanitized.txt']),'w')
        f2.write('\n'.join(red_text))
        f2.close()
        f3 = open(''.join([filename,'-documents_dict.txt']),'w')
        f3.write('\n'.join(corpus_merge_unique))
        f3.close()

    #########################################################################################

    if (tfidf):
        # generate document term frequences
        htf = HashingTF()
        tf = htf.transform(corpus_distrib)
        # generate idf = log{ frac{#docs}{#docs w. term} }
        idf = IDF().fit(tf)
        # scale tf * idf
        tfidf = idf.transform(tf)
        # collect tfidf for future use
        doc_tfidf = tfidf.collect()
        # generate unique word : HashingTF hash dict
        corpus_dict_tfidf_t = {}
        # uniquifie merged corpus into terms
        #corpus_merge_unique = sorted(set(corpus_merge))
        # fill in unique word : HashingTF hash dict
        for word in corpus_merge_unique:
            idx = htf.indexOf(word)
            corpus_dict_tfidf_t[word] = idx
            # index not necessarily found in doc_tfidf.

        # no return item

    #########################################################################################

    if (lda):
        corpus_dict = {}
        for c,word in enumerate(corpus_merge_unique):
            corpus_dict[word]=counts[c]
        def return_freq_words(doc,corpus_dict):
            return [word for word in doc if word in corpus_dict if corpus_dict[word]>2]
        corpus_distrib_red = corpus_distrib.map(lambda doc: return_freq_words(doc,corpus_dict)).cache()
        gensim_corpora_id2word = corpora.Dictionary(corpus_distrib_red.collect())
        gensim_doc2bow_doctf = corpus_distrib_red.map(lambda doc: gensim_corpora_id2word.doc2bow(doc)).collect()
        f1 = open(''.join([filename,'-gensim_corpora_id2word.pkl']),'w')
        pickle.dump(gensim_corpora_id2word,f1)
        f1.close()
        f2 = open(''.join([filename,'-gensim_doc2bow_doctf.pkl']),'w')
        pickle.dump(gensim_doc2bow_doctf,f2)
        f2.close()
        f3 = open(''.join([filename,'-corpus.pkl']),'w')
        pickle.dump(corpus_distrib.collect(),f3)
        f3.close()

    if (word2vec):
        #
        def increase_tf(doc): # only words with freq >= 5 are vectorized
            ret_doc = []
            for i in xrange(5):  # <<<
                ret_doc.extend(doc)  # <<<
            return ret_doc
        #
        corpus_distrib_ext = corpus_distrib.map(lambda doc: increase_tf(doc))
        word_mbd = Word2Vec().setVectorSize(50).setSeed(42L).fit(corpus_distrib_ext)
        word2vec_dict = {}
        for i,w in enumerate(corpus_merge_unique):
            #print ('Counted word "%s" _%i_ many times.'%(w,counts[i]))
            word2vec_dict[w] = word_mbd.transform(w)
            try:
                print ('Top 5 embedding cosine similarity synonyms of word "%s":'%w)
                proximal_synonyms = word_mbd.findSynonyms(w,5)
                for s,cs in proximal_synonyms:
                    print ('  "%s" with score _%f_'%(s,cs))
            except:
                print 'No synonyms found (word not in dict).'
        print
        print 'Processing + Spark MLLib has given us %i word2vec vectors.'%len(word2vec_dict)
        return_list.append(word2vec_dict)
        f4 = open(''.join([filename,'-word2vec_dict.pkl']),'w')
        pickle.dump(word2vec_dict,f4)
        f4.close()

    if len(return_list)==1:
        return_list = return_list[0]
    return return_list
    # possibilities: return: 1.sanit_text ... 2.corpus_dict_unique_words ... 
    # 3.counts ... 4.scored_text_cluster_labels(x2) ... 5.corpus_reduced ...
    # 6.vecs ...

if __name__ == "__main__":
    # Load text file / search engine results
    filename = '../corpora-proc/vlsi.txt'
    #filename = 'tri-articles/stage3FF constitution.txt'
    with open(filename, 'r') as f:
        results = f.read()
    # Extract relevant search engine field results
    # 2: text
    # 3: title
    # 5: article number
    # 7: snippet
    which = 2 # set to any subset of 2 * 3 * 5 * 7
    corpus = exfiltrate_fields(results, 2)
    #############################
    # for doc in corpus:
    #     print cleanup_text(doc)
    #############################
    print 
    print 'Text before preprocessing'
    for doc in list(corpus):
        print doc
    print
    sanit_text, corpus_unique_dict, counts, cluster_lab = \
        distributed_ops(corpus,recall=True,corpred=True)
    # Random checker #
    # print genre_score(sanit_text[random.randrange(0,len(sanit_text))])
    # Loop all #
    # for i in xrange(len(sanit_text)):
    #     print
    #     print 'Got vecs'
    #     print genre_score(sanit_text[i])
    #     print
    # sanit_text above obtained using distributed_ops with option recall (and/or streams)
    cluster_sortix = np.argsort(np.array(cluster_lab))
    sanit_text = [ sanit_text[i] for i in cluster_sortix ]
    cluster_lab = [ cluster_lab[i] for i in cluster_sortix ]
    # print cluster_lab
    print
    print 'Ordered by cluster and sanitized'
    for i,c in enumerate(cluster_lab):
        print c
        print sanit_text[i] # has been reshuffled to cluster order
    print
    # print '-=-=NL=-=-'
    # print
    #############################
    # Extending functionality:
    # 0) We may further add average of inter-punctuation word counts for clustering
    # 1) We want to take only relevant clusters
    # 2) We will separate these into streams again
    # 3) We should segment separate documents since many
    clusters_unique, counts = np.unique(np.array(cluster_lab),return_counts=True)
    clusters_argmax = np.argmax(counts)
    print
    print 'Clusters max is clust# %i'%clusters_unique[clusters_argmax]
    print
    startix = 0
    for i in xrange(clusters_argmax):
        startix += counts[i]
    endix = startix + counts[clusters_argmax]
    sanit_text = sanit_text[startix:endix]
    #print sanit_text
    red_dict, red_counts, new_clusts = distributed_ops(sanit_text,streams=True)
    new_cluster_sortix = np.argsort(np.array(new_clusts))
    sanit_text = [ sanit_text[i] for i in new_cluster_sortix ]
    new_clusts = [ new_clusts[i] for i in new_cluster_sortix ]
    # print len(red_dict)
    # print red_counts
    # print len(new_clusts)
    print
    print 'Streaming (a better name for previous clustering is streaming and this clustering)'
    for i,c in enumerate(new_clusts):
        print c
        print sanit_text[i]
    print
    # print
    # print '-=-=NL=-=-'
    # print
    #############################
    # red_dict, red_counts = distributed_ops(sanit_text,lda=True,filename=filename)
    # ^lda option saves to pkl files - Continue through examineSetxRes-LDA.py.
    # A snapshot of this script with the line above (with lda=True) uncommented
    # has been saved, and a shell script using human_cloning.txt as an example
    # 'do-human-cloning-LDA.sh' to run the saved snapshot followed by
    # 'python examineSetxRes-LDA.py'. Topic probabilities for each document will
    # be generated.
    red_dict, red_counts, red_clusts, red_text, word2vec_dict = \
        distributed_ops(sanit_text,segred=True,segclust=new_clusts,word2vec=True,fin=filename)
    # It is ok if word2vec_dict, red_dict and red_counts use larger docset
    # print 'Exeunt:'
    # print len(red_dict)
    # print red_dict
    # print red_counts
    # print word2vec_dict
    # print len(red_clusts)
    # for i,c in enumerate(red_clusts):
    #     print c
    #     print red_text[i]


