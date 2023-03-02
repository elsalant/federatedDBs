from flask import Flask, request, redirect
from curlUtils import composeAndExecuteOPACurl, handleQuery
from prestoUtils import queryPresto
from jwtUtils import getTokenDict
from constants import USER_KEY, ROLE_KEY, TESTING
import config
import jwt

import logging
import json
import os

ACCESS_DENIED_CODE = 403
ERROR_CODE = 406
VALID_RETURN = 200

FLASK_PORT_NUM = 5559  # this application

CM_PATH = '/etc/confmod/moduleconfig.yaml'  #k8s mount of configmap for general configuration parameters

app = Flask(__name__)
logger = logging.getLogger('flaskUtils.py')
logger.setLevel(logging.DEBUG)
logger.info('setting log level to DEBUG')

def genericQuery(request, sqlQuery):
    tokenDict = getTokenDict(request)
    opaDict = composeAndExecuteOPACurl(request, tokenDict[ROLE_KEY], tokenDict[USER_KEY])
    logger.info('After call to OPA, opaDict = ' + str(opaDict))
    for resultDict in opaDict['transformations']:
        filterAction = resultDict['action']
        logger.debug('filterAction = ' + str(filterAction))
        if filterAction == "BlockURL":
            return ("Access denied!", ACCESS_DENIED_CODE)
    queryAll = setupQueries('ALL')
    print('queryAll = ' + queryAll)
    dataDF = queryPresto(queryAll)
    print('dataDF = ')
    print(dataDF)
    # Apply redaction
#    jDict = dataDF.to_dict()
    try:
        for resultDict in opaDict['transformations']:
            action = resultDict['action']
            # Note: can have both "RedactColumn" and "BlockColumn" actions in line
            if (action == 'RedactColumn' or action == 'BlockColumn'):
                columns = resultDict['columns']
                for keySearch in columns:
 #                   recurseAndRedact(jDict, keySearch.split('.'), action)
                    dataDF = redact(dataDF, keySearch, action)
    except:
        logger.debug('no redaction rules returned')
    return(dataDF)

# Get all Observations
@app.route('/allrecords',methods=['GET'])
def allObservations(queryString=None):
    queryAll = setupQueries('ALL')
    dataDF = genericQuery(request, queryAll)
    print(dataDF)
    if dataDF is None:
        return('')
    return(dataDF.to_json(orient='records'))
 #   return(json.dumps(dataDict), VALID_RETURN)

# Get Observations for a given id.  Id will be passed in the JWT
@app.route('/myrecords',methods=['GET'])
def patientObservations():
    tokenDict = getTokenDict(request)
    queryID = setupQueries(tokenDict[USER_KEY])
    dataDF = genericQuery(request, queryID)
    print(dataDF)
    if dataDF is None:
        return ('')
    return(dataDF.to_json(orient='records'))
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
    else:
        del dataDF[keySearch]
        return(dataDF)
    return(dataDF)

def setupQueries(forWhom):
    alphabet='abcdefghijklmnopqrstuvwxyz'
    registries = config.cmDict['REGISTRIES']
    numRegistries = len(registries)
    queryStr = 'select a.id ID'
    for index, item in enumerate(registries):
        queryStr = queryStr + ', '+alphabet[index]+'.resource ' + item.upper().split('.')[0]  # alias cannot contain a '.'
    queryStr = queryStr + ' FROM '
    for index, item in enumerate(registries):
        queryStr += item+'.observation' + ' '+alphabet[index]+', '
    # get rid of last ', '
    queryStr = queryStr[:-2]
    if forWhom == 'ALL':
        queryStr += ' WHERE '
        for i in range(numRegistries-1):
            queryStr += 'a.id = '+ alphabet[i+1]+'.id AND '
        queryStr = queryStr[:-len(' AND')]+' LIMIT 5'
    else:   # ID string is being passed
        for i in range(numRegistries - 1):
            queryStr += 'a.id = ' + id + ' AND '
        queryStr = queryStr[:-len(' AND')] + ' LIMIT 5'
    print('queryStr = ' + queryStr)
    return(queryStr)

def startServer():
    app.run(port=FLASK_PORT_NUM, host='0.0.0.0')