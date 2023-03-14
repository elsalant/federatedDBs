import urllib.parse as urlparse
import requests
import os
import logging
import json
from constants import TESTING

logger = logging.getLogger('curlUtils.py')
logger.setLevel(logging.DEBUG)


if TESTING:
    OPA_SERVER = 'localhost'
else:
    OPA_SERVER = 'opa.default'

OPA_PORT = os.getenv("OPA_SERVICE_PORT") if os.getenv("OPA_SERVICE_PORT") else 8181
OPA_ENDPT = os.getenv("OPA_URL") if os.getenv("OPA_URL") else '/v1/data/dataapi/authz/rule'
OPA_HEADER = {"Content-Type": "application/json"}
ASSET_NAMESPACE = os.getenv("ASSET_NAMESPACE") if os.getenv("ASSET_NAMESPACE") else 'default'

def composeAndExecuteOPACurl(requests, role, id):

    ## TBD - role is being put into the header as a string - it should go in as a list for Rego.  What we are doing
    ## now requires the Rego to do a substring search, rather than search in a list
    if TESTING:
        return({'SUBMITTER': 'EliotSalant', 'assetID': 'test1', 'SECRET_NSPACE': 'rest-fhir',
          'SECRET_FNAME': 'fhir-credentials', 'FHIR_SERVER' : 'https://localhost:9443/fhir-server/api/v4/', 'transformations': [
                {'action': 'JoinAndRedact', 'joinTable': 'Consent',
                            'whereclause': ' WHERE consent.provision_provision_0_period_end > CURRENT_TIMESTAMP',
                            'joinStatement': ' JOIN consent ON observation.subject_reference = consent.patient_reference ',
                            'columns': ['subject.reference', 'subject.display']},
        {'action': 'HashColumn', 'description': 'redact columns: [subject.id]',
         'intent': 'research', 'columns': ['subject.id'],
         'options': {'redactValue': 'XXXXX'}}]})
    opa_query_body = '{ \"input\": { \
        \"request\": { \
        \"role\": \"' + str(role) + '\", \
        \"id\": \"' + str(id) + '\", \
        }  \
        }  \
        }'

    urlString = 'http://' + OPA_SERVER + ":" + str(OPA_PORT) + OPA_ENDPT
    logger.debug('For OPA query: urlString = ' + urlString + " opa_query_body " + opa_query_body)

    r = requests.post(urlString, data=opa_query_body, headers=OPA_HEADER)

    if (r is None):  # should never happen
        raise Exception("No values returned from OPA! for " + urlString + " data " + opa_query_body)
    try:
        returnString = r.json()
    except Exception as e:
        logger.debug("r.json fails - " + urlString + " data " + opa_query_body)
        raise Exception("No values returned from OPA! for " + urlString + " data " + opa_query_body)

    logger.debug('returnString = ' + str(returnString))
    return (returnString)


def forwardQuery(destinationURL, request):
    # Go out to the actual destination webserver
    logger.debug("queryGatewayURL= " + destinationURL + " request.method = " + request.method)
    content, returnCode = handleQuery(destinationURL, request)
    return (content, returnCode)

def handleQuery(queryGatewayURL, request):
    curlString = queryGatewayURL
    logger.debug("curlCommands: curlString = " + curlString)
    httpAuthJSON = {'Authorization': request.headers.environ['HTTP_AUTHORIZATION']}
    try:
        if (request.method == 'POST'):
            print('request.content_type = ' + str(request.content_type))
            data = ''
            if 'file' in request.files:
                data = request.files['file']
                logger.info('request.files found')
            else:
                if (request.content_type.startswith('application/json')):
                    data = json.dumps(request.get_json()) if type(request.get_json()) == dict else request.get_json()
                    logger.info('application/json found, data = ' + data)
                else:
                    if (request.content_type.startswith('video/')):
                        data = request.get_data()
                        print('data returned')
            newHeaders = dict(request.headers)
            newHeaders.pop('Content-Length')
            newHeaders.pop('Host')
            r = requests.post(curlString, headers=newHeaders, data=data, params=request.args)
        else:
            r = requests.get(curlString, data=request.form, params=request.args,headers=httpAuthJSON )
    except Exception as e:
        logger.debug(
            "Exception in handleQuery, curlString = " + curlString + ", method = " + request.method + " passedHeaders = " + str(
                request.headers) + " values = " + str(request.form))
        raise ConnectionError('Error connecting ')
    return (r.content, r.status_code)

def decodeQuery(queryString):
    return (urlparse.unquote_plus(queryString))
