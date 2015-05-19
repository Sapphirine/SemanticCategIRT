import pickle
from gensim import corpora, models
import sys
from itertools import chain
import numpy as np

# Perform LDA on a bag of dict words representation of a given corpora
# All preprocessing and bag of wordization in examineSETxRES.py

# The following is to argvize:
if len(sys.argv)>1:
    filename = sys.argv[1]

    f1 = open(''.join([filename,'-gensim_corpora_id2word.pkl']),'r')
    gensim_corpora_id2word = pickle.load(f1)
    f1.close()
    f2 = open(''.join([filename,'-gensim_doc2bow_doctf.pkl']),'r')
    gensim_doc2bow_doctf = pickle.load(f2)
    f2.close()
    f3 = open(''.join([filename,'-corpus.pkl']),'r')
    corpus = pickle.load(f3)
    f3.close()
    num_tops = 20
    lda = models.ldamulticore.LdaMulticore(
        corpus=gensim_doc2bow_doctf,
        num_topics=num_tops,
        id2word=gensim_corpora_id2word,
        workers=4,
        chunksize=10000,
        passes=10,
        batch=False,
        alpha='symmetric',
        eta=None,
        decay=0.5,
        offset=1.0,
        eval_every=10,
        iterations=100,
        gamma_threshold=0.001
        )

    print 'Showing mixture of num_tops top words for the num_tops trained topics:'
    for top in lda.show_topics(num_words=num_tops):
        print top
    lda_corpus = lda[gensim_doc2bow_doctf]

    scores = list(chain(*[ 
        [score for topic_id,score in topic] for topic in [doc for doc in lda_corpus]
        ]))
    threshold = float(sum(scores))/float(len(scores))
    print 'Threshold: %f (arithmetic average of{{ sum of{doc topic-probabilities} }})'%threshold
    # zip creates: list1[tuples(topic,score),...],list2[str(doc_words),...]
    # save_doc_topic_mixtures = zip(lda_corpus,corpus)
    # print save_doc_topic_mixtures
    corpus2vec_topic_scores = np.zeros([len(corpus),num_tops]).astype(np.float)
    print corpus2vec_topic_scores.shape
    print len(lda_corpus)
    print len(corpus)
    lda_corpus = list(lda_corpus)
    for docix in xrange(len(corpus)):
        for topic,score in lda_corpus[docix]:
            corpus2vec_topic_scores[docix,topic]=score
    print corpus2vec_topic_scores
    np.savetxt(''.join([filename,'-corpus2vec_topic_scores-100.txt']),corpus2vec_topic_scores,delimiter=',')

