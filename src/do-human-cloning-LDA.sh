#!/bin/sh
pyspark examineSETxRES-snapshot.py
python examineSETxRES-LDA.py ../corpora-proc/human_cloning.txt