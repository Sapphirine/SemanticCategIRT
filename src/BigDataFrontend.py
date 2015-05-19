from pyspark import SparkContext
sc = SparkContext("local", "Simple App")
import math
import numpy
import pylab
import StringIO
import pprint
import re
from apiclient.discovery import build
from alchemyapi import AlchemyAPI
alchemyapi = AlchemyAPI()
import json
import urllib
import urllib2
from pybing import Bing
import webhose

def retrieveg(terms,stage1):
	#Initiate Google Search instance, return 100 results
  	service = build("customsearch", "v1",
            	developerKey="AIzaSyBQnWQCPdkmx47aEB6smkusoSlg7UjbMPs")
	pages = [1, 11, 21, 31, 41, 51, 61, 71, 81, 91]
	open(stage1, 'w').close()
	s1 = open(stage1, "a")
	for i in pages:
  		res = service.cse().list(
      		q=terms,
		start = i,
		num=10,
      		cx='000183839826076829333:4hsbjcenq7e',
    		).execute()
		s1.write(str(res))
	return

def retrieveb(terms,stage1):
	#Initiate Bing Search instance, return 50 results
    	key= 'HEgJwwZTJfFUDuOIJrvyO/FJwS38R5R5+AxLB/x0bYs'
    	query = urllib.quote(terms)
   	#Create credentials for authentication
    	user_agent = 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Trident/4.0; FDM; .NET CLR 2.0.50727; InfoPath.2; .NET CLR 1.1.4322)'
    	credentials = (':%s' % key).encode('base64')[:-1]
    	auth = 'Basic %s' % credentials
    	#url = 'https://api.datamarket.azure.com/Data.ashx/Bing/Search/'+'Web'+'?Query=%27'+query+'%27&$top=5&$format=json'
	url = 'https://api.datamarket.azure.com/Data.ashx/Bing/Search/'+'Web'+'?Query=%27'+query+'%27&$top=50&$format=json'
    	request = urllib2.Request(url)
    	request.add_header('Authorization', auth)
    	request.add_header('User-Agent', user_agent)
    	request_opener = urllib2.build_opener()
    	response = request_opener.open(request) 
    	response_data = response.read()
    	json_result = json.loads(response_data)
    	result_list = json_result['d']['results']
	open(stage1, 'w').close()
	s1 = open(stage1, "a")
	s1.write(str(result_list))
	return

def extract(stage1,stage2a,stage2b,stage2c,code):
	#Load results into string
	streamread = open(stage1, "r")
	stream = str(streamread.readlines())

	#Extract relevant urls
	if code == 'g':
		reg1 = r"(?<=formattedUrl).*?(?=',)"
	elif code == 'b':
		reg1 = r"(?<=DisplayUrl).*?(?=',)"
	aurls = re.findall(reg1, stream)
	burls = sc.parallelize(aurls)
	curls = burls.map(lambda x: x.split("'")[2])
	furls = curls.map(lambda x: x.replace("\\", "").replace("'", ""))
	s2a = open(stage2a, 'w')
	s2a.write(str(furls.collect()))

	#Extract relevant titles
	if code == 'g':
		reg2 = r"(?<=htmlTitle).*?(?=',)"
	elif code == 'b':
		reg2 = r"(?<=Title).*?(?=',)"
	atitles = re.findall(reg2, stream)
	btitles = sc.parallelize(atitles)
	ctitles = btitles.map(lambda x: x.split("'")[2])
	ftitles = ctitles.map(lambda x: x.replace("\\", "").replace("'", ""))
	s2b = open(stage2b, 'w')
	s2b.write(str(ftitles.collect()))

	#Extract relevant snippets
	if code == 'g':
		reg3 = r"(?<=snippet).*?(?=',)"
	elif code == 'b':
		reg3 = r"(?<=Description).*?(?=',)"
	asnippets = re.findall(reg3, stream)
	bsnippets = sc.parallelize(asnippets)
	csnippets = bsnippets.map(lambda x: x.split("'")[2])
	fsnippets = csnippets.map(lambda x: x.replace("\\", "").replace("'", ""))
	s2c = open(stage2c, 'w')
	s2c.write(str(fsnippets.collect()))
	return

def textretrieve(stage2a,stage2b,stage2c,stage3):
	#Load URLs into list
	urlread = open(stage2a, "r")
	url = str(urlread.readlines())
	zurl = url.replace(" ", "").replace("[", "").replace("]", "")
	urls = zurl.replace("'", "").replace('"', "").split(",")
	
	#Load titles into list
	titleread = open(stage2b, "r")
	title = str(titleread.readlines())
	ztitle = title.replace("[", "").replace("]", "")
	titles = ztitle.replace('"', "").split("',")

	#Load snippets into list; prepare output
	snippetread = open(stage2c, "r")
	snippet = str(snippetread.readlines())
	zsnippet = snippet.replace("[", "").replace(" ]", "")
	snippets = zsnippet.replace('"', "").split("',")
	open(stage3, 'w').close()
	s3 = open(stage3, "a")

	#Pull text for each link URL from Alchemy API
	#for i in range(0,1):
	for i in range(0,len(urls)):
		try:
			s3.write("-=-=-ARTICLE " + str(i+1) + ": " + urls[i] + "\n")
		except:
			s3.write("-=-=-ARTICLE " + str(i+1) + "\n")
		try:
			s3.write("-=-=-TITLE: " + titles[i].replace("'", "").replace("\\", "") + "\n")
		except:
			s3.write("-=-=-TITLE: \n")
		try:
			s3.write("-=-=-SNIPPET: " + snippets[i].replace("'", "").replace("\\", "") + "\n")
		except:
			s3.write("-=-=-SNIPPET: \n")
		try:
			s3.write("-=-=-TEXT: ")
			response = alchemyapi.text('url', urls[i])
			if response['status'] == 'OK':
    				s3.write(str(response['text'].encode('utf-8')) + "\n")
			else:
    				s3.write("\n")
		except:
			s3.write("\n")

	return

def fullretrievew(terms,stage1):
	#Initiate Webhose search instance, return max results
    	webhose.config(token = 'b88b78c1-0dac-4793-913e-7d20e0559144')
	re = webhose.search(terms)

	open(stage1, 'w').close()
	s1 = open(stage1, "a")
	i = 0
	for post in re:
		i = i+1
		try:
			s1.write("-=-=-ARTICLE " + str(i) + ": " + str(post.thread.url.encode('utf-8')) + "\n")
		except:
			s1.write("-=-=-ARTICLE " + str(i) + "\n")
		try:
			s1.write("-=-=-TITLE: " + str(post.thread.title_full.encode('utf-8')) + "\n")
		except:
			s1.write("-=-=-TITLE: \n")
		s1.write("-=-=-SNIPPET: \n")
		try:
			s1.write("-=-=-TEXT: " + str(post.text.encode('utf-8')) + "\n")
		except:
			s1.write("-=-=-TEXT: \n")
	return

def reduction(stage3g,stage3b,stage3w,stage3F):
	#Begin by opening files and copying contents of stage3g
	s3g = open(stage3g, "r")
	s3b = open(stage3b, "r")
	s3w = open(stage3w, "r")
	open(stage3F, 'w').close()
	s3F = open(stage3F, "a")
	initdatag = s3g.read()
	initdatab = s3b.read()
	initdataw = s3w.read()
	s3F.write(initdatag)

	#Build Google result RDD for comparison
	linebylineg = initdatag.split("\n")
	linebylg = sc.parallelize(linebylineg)
	lineblg = linebylg.filter(lambda x: "-=-=-ARTICLE " in x)
	lnblg = lineblg.map(lambda x: x.split(":")[1])
	lblg = lnblg.collect()

	#Build Bing result RDD for comparison
	linebylineb = initdatab.split("\n")
	lineblb = sc.parallelize(linebylineb)
	lblb = lineblb.collect()
	
	#Build Webhose result RDD for comparison
	linebylinew = initdataw.split("\n")
	lineblw = sc.parallelize(linebylinew)
	lblw = lineblw.collect()
	
	#Perform comparison with Bing results
	s3F.write("<-=-><-=-><-=-><-=-><-=-><-=-><-=-><-=-><-=-><-=->\n")
	pos = 101
	for i in range(0,len(lblb)):
		if "-=-=-ARTICLE " in lblb[i]:
			flag = False
			for j in range(0,len(lblg)):
				if lblg[j] == lblb[i].split(":")[1]:
					flag = True
			if flag == True:
				pass
			else:
				s3F.write("-=-=-ARTICLE " + str(pos) + ":" + lblb[i].split(":")[1] + "\n")
				pos = pos + 1
		if flag == True:
			pass
		elif "-=-=-ARTICLE " not in lblb[i]:
			s3F.write(lblb[i] + "\n")
	
	#Perform comparison with Webhose results
	s3F.write("<-=-><-=-><-=-><-=-><-=-><-=-><-=-><-=-><-=-><-=->\n")
	for i in range(0,len(lblw)):
		if "-=-=-ARTICLE " in lblw[i]:
			flag = False
			for j in range(0,len(lblg)):
				lblwm = lblw[i].split(":")[1] + ":" + lblw[i].split(":")[2]
				if lblg[j] == lblwm:
					flag = True
			if flag == True:
				pass
			else:
				s3F.write("-=-=-ARTICLE " + str(pos) + ":" + lblwm + "\n")
				pos = pos + 1
		if flag == True:
			pass
		elif "-=-=-ARTICLE " not in lblw[i]:
			s3F.write(lblw[i] + "\n")
	return

def reorg(stage3F,stage3FF):
	#Begin by opening files and reading contents of stage3F
	s3F = open(stage3F, "r")
	open(stage3FF, 'w').close()
	s3FF = open(stage3FF, "a")
	indata = s3F.read()

	#Build reduced group arrays
	idata = indata.split("<-=-><-=-><-=-><-=-><-=-><-=-><-=-><-=-><-=-><-=->\n")
	gdata = idata[0].split("-=-=-ARTICLE ")
	bdata = idata[1].split("-=-=-ARTICLE ")
 	wdata = idata[2].split("-=-=-ARTICLE ")
	glength = len(gdata)
	blength = len(bdata)
	wlength = len(wdata)
	mlength = max(glength,blength,wlength)

	#Merge reduced arrays in new sequence
	for i in range(1,mlength):
		if i < glength:
			s3FF.write("-=-=-GOOGLE ARTICLE " + str(i) + ":" + gdata[i].split(":", 1)[1] + "\n")
		if i < blength:
			s3FF.write("-=-=-BING ARTICLE " + str(i) + ":" + bdata[i].split(":", 1)[1] + "\n")
		if i < wlength:
			s3FF.write("-=-=-WEBHOSE ARTICLE " + str(i) + ":" + wdata[i].split(":", 1)[1] + "\n")
	return

#Define search terms
terms = 'VLSI'

#Retrieve data from Google
stage1g = 'stage1g'
stage2gu = 'stage2gu'
stage2gt = 'stage2gt'
stage2gs = 'stage2gs'
stage3g = 'stage3g'
codeg = 'g'
retrieveg(terms,stage1g)
extract(stage1g,stage2gu,stage2gt,stage2gs,codeg)
textretrieve(stage2gu,stage2gt,stage2gs,stage3g)
print ">>>>>>>>>>GOOGLE RETRIEVAL COMPLETE>>>>>>>>>>"

#Retrieve data from Bing
stage1b = 'stage1b'
stage2bu = 'stage2bu'
stage2bt = 'stage2bt'
stage2bs = 'stage2bs'
stage3b = 'stage3b'
codeb = 'b'
retrieveb(terms,stage1b)
extract(stage1b,stage2bu,stage2bt,stage2bs,codeb)
textretrieve(stage2bu,stage2bt,stage2bs,stage3b)
print ">>>>>>>>>>BING RETRIEVAL COMPLETE>>>>>>>>>>"

#Retrieve data from Webhose
stage3w = 'stage3w'
fullretrievew(terms,stage3w)
print ">>>>>>>>>>WEBHOSE RETRIEVAL COMPLETE>>>>>>>>>>"

#Reduce working set
stage3F = 'stage3F'
reduction(stage3g,stage3b,stage3w,stage3F)
print ">>>>>>>>>>SET REDUCTION COMPLETE>>>>>>>>>>"

#Reduce working set
stage3FF = 'stage3FF'
reorg(stage3F,stage3FF)
print ">>>>>>>>>>SET REORGANIZATION COMPLETE>>>>>>>>>>"


