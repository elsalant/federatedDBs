from flask import Flask, request, redirect
from curlUtils import composeAndExecuteOPACurl, handleQuery
from prestoUtils import queryPresto
from jwtUtils import getTokenDict
from constants import USER_KEY, ROLE_KEY, TESTING
import config
import jwt

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
                if (action == 'RedactColumn' or action == 'BlockColumn'):
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
    return(dataDF.to_json(orient='records'), VALID_RETURN)

# Get Observations for a given id.  Id will be passed in the JWT
@app.route('/myrecords',methods=['GET'])
def patientObservations():
    tokenDict = getTokenDict(request)
    queryID = setupQueriesUnion(tokenDict[USER_KEY])
    redactNeeded = False
    dataDF = genericQuery(request, queryID, redactNeeded)
    print(dataDF)
    if dataDF is None:
        return ('')
    return(dataDF.to_json(orient='records'), VALID_RETURN)
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

# Get all observation resources
def setupQueriesAll():
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
    queryStr += ' WHERE '
    for i in range(numRegistries-1):
        queryStr += 'a.id = '+ alphabet[i+1]+'.id AND '
    queryStr = queryStr[:-len(' AND')]
    print('queryStr = ' + queryStr)
    return(queryStr)

# Get the observation resources for a given subject.id
# The query template is like:
'''
WITH 
subjects as (select json_extract(resource, '$.subject') from postgresqlk8s.public.observation subjects), 
attributes as (select * from postgresqlk8s.public.observation attributes), 
subjects1 as (select json_extract(resource, '$.subject') from postgresql.public.observation subjects),
attributes1 as (select * from postgresqlk8s.public.observation attributes1)
SELECT json_extract_scalar(subject,'$.id') ID, attributes.resource ATTRIBUTES
FROM subjects as t(subject), attributes
WHERE json_extract_scalar(subject,'$.id')='08723d97-8dd3-4481-a5f1-a9427488d729'
UNION ALL
SELECT json_extract_scalar(subject1,'$.id') ID, attributes1.resource ATTRIBUTES FROM subjects1 as t(subject1),
attributes1 WHERE json_extract_scalar(subject1,'$.id')='urn:uuid:ad023201-471e-4780-b424-0eb172c074f2' limit 5;
'''

def setupQueriesUnion(id):
    alphabet='abcdefghijklmnopqrstuvwxyz'
    registries = config.cmDict['REGISTRIES']
    numRegistries = len(registries)
    queryStr = ' WITH '
    for index, item in enumerate(registries):
        queryStr = queryStr + "subjects"+str(index) + " as (SELECT json_extract(resource,'$.subject') FROM " \
                +item+".observation subjects"+str(index) + "), " + \
                   " attributes"+str(index) + " as (SELECT * FROM " \
                 +item+".observation attributes"+str(index) + ") "
        if (index != len(registries) -1):
            queryStr += ', '
    for index, item in enumerate(registries):
        if index > 0:
            queryStr += ' UNION ALL '
        queryStr += " SELECT json_extract_scalar(subject"+str(index)+",'$.id') ID, attributes"+str(index) + ".resource ATTRIBUTES"+str(index) + \
                " FROM subjects"+str(index)+" as t(subject"+str(index)+"), attributes"+str(index) + \
               " WHERE json_extract_scalar(subject"+str(index)+",'$.id')=" + "'"+id+"'"
  #      queryStr += ' LIMIT 5'
    print('queryStr = ' + queryStr)
    return(queryStr)

def setupQueriesUnionOLD(id):
    alphabet='abcdefghijklmnopqrstuvwxyz'
    registries = config.cmDict['REGISTRIES']
    numRegistries = len(registries)
    queryStr = ''
    for index, item in enumerate(registries):
        if index > 0:
            queryStr += ' UNION '
        queryStr += ' SELECT '
        queryStr = queryStr + alphabet[index]+'.id, ' +alphabet[index]+'.resource ' + item.upper().split('.')[0]  # alias cannot contain a '.'
        queryStr = queryStr + ' FROM '
        queryStr += item+'.observation ' +alphabet[index]
        queryStr += ' WHERE '
        queryStr += alphabet[index]+'.id =\''+id+'\' '

    print('queryStr = ' + queryStr)
    return(queryStr)

def startServer():
    app.run(port=FLASK_PORT_NUM, host='0.0.0.0')