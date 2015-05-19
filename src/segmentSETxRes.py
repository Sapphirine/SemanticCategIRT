# segmentSearchEngineTextResults.py
# import examineSETxRes
import precompiledRegex
import numpy as np
import tools
import splitters
from random import shuffle
import pickle
from pyspark import SparkContext

# Start a spark distributed ops context
sc = SparkContext(appName='info-globtree')

def missing_preps(text):
    text = precompiledRegex.cid_pat.sub(" UNK ", text.decode("utf-8"))
    return "".join(["START ", text.encode('utf-8'), " END"])

def word2vect(doc_filt,corpus_dict_set,word2vec_dict,merge=False):
    text2vec = []
    tx2dict_wix_mapper = {}
    intext_dictonly_word_count = 0
    for intext_word_index,word in enumerate(doc_filt.split()):
        if word in corpus_dict_set:
            # print 'Word: "%s" -=-2Vec-=->'%word
            # print word2vec_dict[word]
            tx2dict_wix_mapper[intext_word_index] = intext_dictonly_word_count
            intext_dictonly_word_count+=1
            text2vec.append(word2vec_dict[word])
        # else:
        #     if word not in examineSETxRes.stopword_set \
        #         and word!='NL' and word!='START' and word!='END':
        #         print 'Odd word: "%s" not stop words or in dict.'%word
    return text2vec, tx2dict_wix_mapper

def segment_doc(doc_filt,corpus_dict_set,word2vec_dict,merge=False):
    # See implementation above:
    text2vec, tx2dict_wix_mapper = word2vect(doc_filt,corpus_dict_set,word2vec_dict,merge=False)
    dict2txt_wix_mapper = \
        {dictix:textix for textix,dictix in tx2dict_wix_mapper.iteritems()}
    text2vec = np.array(text2vec)
    # print 'text2vec length:', text2vec.shape[0]
    # print 'vec length:', text2vec.shape[1]
    K = min(int(len(doc_filt)/50), 80)
    print 'Numsegs = %i'%K
    print
    sig = splitters.gensig_model(text2vec)
    # print "Splitting..."
    splits,e = splitters.greedysplit(text2vec.shape[0], K, sig)
    # print splits
    # print "Refining..."
    splitsr = splitters.refine(splits, sig, 20)
    # print splitsr
    prev = 0
    doc_filt = doc_filt.split() # split continuous string at spaces to get words
    segments = []
    for s in splitsr:
        k = dict2txt_wix_mapper.get(s,len(doc_filt))
        segment = " ".join(doc_filt[prev:k]) # join together single segment words ws delimited
        # segment = segment.replace("NL","\n") # replace all NL by actual newline
        # segment = ''.join(segment, "\nBREAK\n") # append to end of a segment
        segments.append(segment)
        prev = k
    # print "Done"
    return segments

if __name__ == "__main__":
    #
    # Load pickled and prepped corpus
    filename = "../corpora-proc/vlsi.txt"
    f1 = open(''.join([filename,'-documents_sanitized.txt']))
    corpus = f1.read().split('\n')
    f1.close()
    f2 = open(''.join([filename,'-document_clusters.txt']))
    corpus_clusts = f2.read().split('\n')
    f2.close()
    f3 = open(''.join([filename,'-documents_dict.txt']))
    corpus_dict = f3.read().split('\n')
    f3.close()
    f4 = open(''.join([filename,'-word2vec_dict.pkl']))
    word2vec = pickle.load(f4)
    f4.close()
    #
    shuffle = False
    merge = False
    if (shuffle):
        # Option: randomize docs order for fun
        shuffle(corpus)
    if (merge):
        # Option: merge all docs into one
        corpus = map(missing_preps, corpus)
        corpus = " ".join(corpus) # Treat corpus as one doc?
    #
    # Provided must be a list of continuous unsplit strings.
    corpus = sc.parallelize(corpus).map(lambda doc: segment_doc(doc,corpus_dict,word2vec)).collect()
    #corpus = [segment_doc(doc, corpus_dict, word2vec) for doc in corpus]
    # corpus = " ".join(corpus)
    segments = []
    seg_counter = 0
    for doc in corpus:
        print 'New doc'
        for segment in doc:
            print '------New-segment------'
            print segment
            seg_counter += 1
            segments.append(segment)
        print
        print
    # HAC
    print 'Corpus len: %i'%len(corpus)
    print 'Num segments: %i'%seg_counter
    print
    # print 'Listing all segments:'
    # print segments
    # print
    segments = sc.parallelize(segments).map(lambda segment: word2vect(segment,corpus_dict,word2vec)[0]).collect()
    # print  'Vectorized now:'
    # print segments
    # print
    for i,segment_word2vecs in enumerate(segments):
        fname = ''.join([filename,'-segment_word2vecs/segment-',str(i),'.txt'])
        np.savetxt(fname,np.array(segment_word2vecs).astype(np.float32))
    # Now have word vecs
