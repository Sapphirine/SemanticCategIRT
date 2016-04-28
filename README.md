# SemanticCategIRT
Search Engine corpus (document text, Semantic and Categorical) Information Retrieval and processing Tool

Is a proof of concept / demo type of project only!
For a write-up and background ideas view the link (project ID 201505-24).

It is probably preferable to organize files in the src directory into sub-directories, and refactor the
source code which does the analysis so that there is only one "main" file (get rid of the "snapshot" file)
and further refactor so that the code becomes more clearly modularized (facts acknowledged!).

The following can be used as a guide to naviguate through the source / project-demo files:

- BigDataFrontEnd.py was written by Miguel Costa, and fetches documents from the web using search engine
APIs authorized with trial accounts!

The analytical pipeline was written by Sami Mourad:

- STOPWORD.list is used by precompiledRegex.py.
- precompiledRegex.py(c), precompiledGenrSubScor.py(c) are included in examineSETxRES.py and
examineSETxRES-snapshot.py, which is just a copy of the former with minor modifications
(for the input).
- do-human-cloning-LDA.sh runs the LDA analysis on a query of documents corresponding to the
expression "human cloning" (which according to google is a hot topic). First it executes 
examineSETxRES-snapshot.py then examineSETxRES-LDA.py
- vlsi-demo.sh executes the segmentation algorithm on documents queried using the term "vlsi".
First it executes examineSETxRES.py then segmentSETxRES.py
- The segmentation algorithm is implemented in representations.py, splitters.py and tools.py
(work of alexalemi).
