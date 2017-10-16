__author__ = 'gjlawran'
# an automated site survey of CKAN installations
# script visits each CKAN deployment and tries to determine the status, version, extensions and webserver
# script logs what it finds at each site and makes a summary of some observations


import requests
import json
import time
import logging
import csv

logging.captureWarnings(True)

#open a file to output the results of the harvest to  text files
try:
    logfile1 = open('./results/log_'+time.strftime("%Y%m%d")+'.out','wb') # by opening in binary mode ensures don't create double-line on windows
    logfilecsv = csv.writer(logfile1)
    logfilecsv.writerow(['date','url','serverType','version'])
except IOError:
    print 'Some sort of write error with the CSV file!'

surveyed = open('./results/surveyed_'+time.strftime("%Y%m%d")+'.out','w')

surveyed.write("Harvest start: " + time.strftime("%c")+"\n\n")
surveyed.write("Sites not responding to request:\n")

#use the OKF registry of CKAN instances as the list to harvest from
#TODO - expand to include those sites in dataportals
#r = requests.get('http://instances.ckan.org/config/instances.json') old
r = requests.get('https://raw.githubusercontent.com/ckan/ckan-instances/gh-pages/config/instances.json')

data = r.json()
#TODO - remove after testing is complete
data = data[0:9]  # remove after testing


#define dictionaries for collecting list of extensions, versions, OS
extCollection = {}
verCollection = {}
OSCollection = {}

# for each site determine status and store results to dicts

for i in range(0,len(data)):
    #compose and make status request for the site
    #TODO consider validating and parsing URL with urlparse module
    if data[i]['url'] : URLinst = data[i]['url'].rstrip('//') +'/api/action/status_show'

    try:
        #TODO does this handle both https and http?
        m = requests.get(URLinst)

        # if possible determine the server type
        serverType = m.headers['server'] if 'server' in m.headers else 'unknown'

        if m.status_code == 200 and m.headers['content-type'] == 'application/json;charset=utf-8':
            # if we find a working machine print the  URL, status, and server type to console
            print URLinst, m.status_code, serverType," CKAN:", m.json()['result']['ckan_version']


            # log the siteURL, version, extensions
            logfilecsv.writerow([time.strftime("%Y%m%d"),URLinst, serverType," CKAN:"+ m.json()['result']['ckan_version'],m.json()['result']['extensions']])

            # store the version, server info and extensions into 3 dict
            verCollection.setdefault(m.json()['result']['ckan_version'],[]).append(data[i]['url'])
            OSCollection.setdefault(serverType,[]).append(data[i]['url'])

            # create a dictionary of the extensions used
            # key is extension, values are the URL of the site with extension
            for eachext in m.json()['result']['extensions']:
                extCollection.setdefault(eachext,[]).append(data[i]['url'])

        else:   # output those sites that failed
            print "FAIL#"+URLinst, m.status_code, serverType
            surveyed.write( data[i]['url'] +" , "+ str(m.status_code) + "\n")

    except requests.exceptions.RequestException as e:
        print e


surveyed.write('\n CKAN Version Deployments \n')
json.dump(verCollection, surveyed)  #json encode dictionary and put it to the file hanlde surveyed
surveyed.write( '\n \n CKAN Extension Deployments \n')
json.dump(extCollection, surveyed)
surveyed.write( '\n \n Web Server Information \n')
json.dump(OSCollection, surveyed)

def sortOutDict(dictname):
    print (dictname)
    print len(dictname)
    for keys in sorted(dictname):
        print len(dictname[keys]), ",", keys, ",", dictname[keys]
    print "\n"

# TODO working on restructuring reports as JSON object for using within bootstrap tables
# exist {u'2.2b': [u'http://data.gov.uk'], u'2.4.0': [u'https://catalogodatos.gub.uy/'], u'2.2.2': [u'http://dados.gov.br']}
# need  {"versName":"2.0.2","num":15, "sites":["http://opendata.lisra.jp"]},
# jsonobj = exist ...
'''
trying to do this the hard way - now using JSONdumps
def jsonObjective(dictname):
    print ('[')
    for keys in sorted(dictname):
        #print len(dictname[keys]), ",", keys, ",", dictname[keys]
        print '{"versName":"'+keys +'", "num":',len(dictname[keys]),', "sites":',dictname[keys],'}'
    print "\n"
    print (']')
'''

def jsonObjective(dictname):


    #dictstr = json.dumps(dictname)
    #print (dictstr)
    import pandas as pd
    df = pd.Series(dictname, name='sites')
    df.index.name= 'vers'
    print (df.reset_index())
    df = df.assign(num=df.sites.len())
    print (df.reset_index())



jsonObjective(verCollection)

#sortOutDict(verCollection)
#sortOutDict(OSCollection)
#sortOutDict(extCollection)

logfile1.close()
surveyed.close()
