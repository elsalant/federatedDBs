from flask import Flask, request, jsonify
from curlUtils import composeAndExecuteOPACurl, handleQuery
from prestoUtils import queryPresto
from jwtUtils import getTokenDict
from constants import USER_KEY, ROLE_KEY, TESTING
import config
import jwt
import json

import logging

ACCESS_DENIED_CODE = 403
ERROR_CODE = 406
VALID_RETURN = 200

FLASK_PORT_NUM = 5559  # this application

CM_PATH = '/etc/confmod/moduleconfig.yaml'  #k8s mount of configmap for general configuration parameters

app = Flask(__name__)
logger = logging.getLogger('flaskUtils.py')
logger.setLevel(logging.DEBUG)
logger.info('setting log level to DEBUG')

def genericQuery(request, sqlQuery, redactNeeded):
    tokenDict = getTokenDict(request)
    opaDict = composeAndExecuteOPACurl(request, tokenDict[ROLE_KEY], tokenDict[USER_KEY])
    logger.info('After call to OPA, opaDict = ' + str(opaDict))
    for resultDict in opaDict['transformations']:
        filterAction = resultDict['action']
        logger.debug('filterAction = ' + str(filterAction))
        if filterAction == "BlockURL":
            return ("Access denied!", ACCESS_DENIED_CODE)
    print('sqlQuery = ' + sqlQuery)
    dataDF = queryPresto(sqlQuery)

    print('dataDF = ')
    print(dataDF)
    # Apply redaction
#    jDict = dataDF.to_dict()
    if redactNeeded:
        try:
            for resultDict in opaDict['transformations']:
                action = resultDict['action']
                # Note: can have both "RedactColumn" and "BlockColumn" actions in line
                columns = resultDict['columns']
                for keySearch in columns:
 #                   recurseAndRedact(jDict, keySearch.split('.'), action)
                    dataDF = redact(dataDF, keySearch, action)
        except:
            logger.debug('no redaction rules returned')
    return(dataDF)

# Get all Observations - meant for role=Researcher
@app.route('/allrecords',methods=['GET'])
def allObservations(queryString=None):
    queryAll = setupQueriesAll()
    redactNeeded = True
    dataDF = genericQuery(request, queryAll, redactNeeded)
    print(dataDF)
    if dataDF is None:
        return('')
    returnJson = dataDF.to_json(orient='table', index=True)
    # prettify the return
    parsedJson = json.loads(returnJson)
    prettyJson = json.dumps(parsedJson, indent=2)
    return(prettyJson, VALID_RETURN)
 #   return(dataDF.to_json(orient='records'), VALID_RETURN)

# Get Observations for a given id.  Id will be passed in the JWT
@app.route('/myrecords',methods=['GET'])
def patientObservations():
    tokenDict = getTokenDict(request)
    queryID = setupQueriesPatient(tokenDict[USER_KEY])
    redactNeeded = False
    dataDF = genericQuery(request, queryID, redactNeeded)
    print(dataDF)
    if dataDF is None:
        return ('')
#    returnJson = dataDF.to_json(orient='records')
    returnJson = dataDF.to_json(orient='table', index=True)
    # prettify the return
    parsedJson = json.loads(returnJson)
    prettyJson = json.dumps(parsedJson, indent=2)
    return(prettyJson, VALID_RETURN)
 #   return (json.dumps(dataDict), VALID_RETURN)

def decryptJWT(encryptedToken, flatKey):
# String with "Bearer <token>".  Strip out "Bearer"...
    prefix = 'Bearer'
    assert encryptedToken.startswith(prefix), '\"Bearer\" not found in token' + encryptedToken
    strippedToken = encryptedToken[len(prefix):].strip()
 #   decodedJWT = jwt.api_jwt.decode(strippedToken, options={"verify_signature": False})
    decodedJWT = jwt.decode(strippedToken, options={"verify_signature": False})
#    logger.info(decodedJWT)

 #   flatKey = os.getenv("SCHEMA_ROLE") if os.getenv("SCHEMA_ROLE") else FIXED_SCHEMA_ROLE
# We might have an nested key in JWT (dict within dict).  In that case, flatKey will express the hierarchy and so we
# will interatively chunk through it.
    decodedKey = None
    while type(decodedJWT) is dict:
        for s in flatKey.split('.'):
            if s in decodedJWT:
                decodedJWT = decodedJWT[s]
                decodedKey = decodedJWT
            else:
                logger.debug("warning: " + s + " not found in decodedKey!")
                return None
    return decodedKey

def redact(dataDF, keySearch, action):
    if len(keySearch) == 0:
        return(dataDF)
    if not keySearch in dataDF.keys():
        print("ERROR: " + keySearch + " not found in data set!")
        return(dataDF)
    if action == 'RedactColumn':
        dataDF[keySearch] = 'XXX'
        print('redacted')
    elif action == 'HashColumn':
        dataDF[keySearch] = dataDF[keySearch].apply(hash)
        print('column hashed')
    else:
        del dataDF[keySearch]
        return(dataDF)
    return(dataDF)

# Get the observation resources for a given subject.id
# The query template is like:
'''
WITH s0 as 
  (with source0 as (select id ID, resource RESOURCE, json_extract(resource, '$.subject') subjects 
   FROM postgresqlk8s.public.observation) 
   select json_extract_scalar(subjects,'$.id') PATIENT_ID, 
     json_extract(subjects,'$.id') PATIENTID, 
     json_extract(resource, '$.value') VALUE, 
     json_extract(resource,'$.code') CODE,
     json_extract(resource, '$.encounter') ENCOUNTER
      from source0) , 
s1 as 
  (with source1 as (select id ID, resource RESOURCE, json_extract(resource, '$.subject') subjects 
  FROM postgresqlk8s.public.observation) 
  select json_extract_scalar(subjects,'$.id') PATIENT_ID, 
    subjects PATIENTID, json_extract(resource, '$.value') VALUE, 
    json_extract(resource,'$.code') CODE,
    json_extract(resource, '$.encounter') ENCOUNTER 
     from source1) 
    SELECT PATIENTID, ENCOUNTER, VALUE, CODE from s0 WHERE PATIENT_ID = 'urn:uuid:ccef190b-3282-47a3-8a38-be111c4ab299' 
UNION ALL 
  select PATIENTID, ENCOUNTER, VALUE, CODE from s1 WHERE PATIENT_ID = 'urn:uuid:ccef190b-3282-47a3-8a38-be111c4ab299' 
  limit 5;
'''

def baseQuery(id):
    registries = config.cmDict['REGISTRIES']
    numRegistries = len(registries)
    queryStr = ' WITH '
    for index in range(numRegistries):
        queryStr = queryStr + "s" + str(index) + " as (with source" + str(
            index) + " as (select id ID, resource RESOURCE, json_extract(resource, '$.subject') subjects  FROM " \
                   + registries[0] + ".observation) " + \
                   " select json_extract_scalar(subjects,'$.id') PATIENT_ID, subjects PATIENTID, json_extract(resource, '$.value') VALUE, json_extract(resource,'$.code') CODE, json_extract(resource, '$.encounter') ENCOUNTER from source" + str(
            index) + ") "
        if (index != len(registries) - 1 and len(registries) > 1):
            queryStr += ', '
    queryStr += " SELECT PATIENTID, ENCOUNTER, VALUE, CODE from s0 "
    if (id):
        queryStr += " WHERE PATIENT_ID = '"+id+"'"
    if numRegistries > 1:
        for index in range(1, numRegistries):
            queryStr += ' UNION ALL '
            queryStr += " select PATIENTID, ENCOUNTER, VALUE, CODE from s" + str(index)
            if (id):
                queryStr += " WHERE PATIENT_ID = '" + id + "'"
    queryStr += ' LIMIT 50'
    print('queryStr = ' + queryStr)
    return (queryStr)

def setupQueriesPatient(id):
    queryStr = baseQuery(id)
    print('queryStr = ' + queryStr)
    return(queryStr)

# Get all observation resources
def setupQueriesAll():
    queryStr = baseQuery(None)
    return(queryStr)


def startServer():
    app.run(port=FLASK_PORT_NUM, host='0.0.0.0')