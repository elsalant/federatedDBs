import urllib.parse as urlparse
import requests
import os
import logging
import json
import yaml
from constants import TESTING, TESTING_NO_OPA

logger = logging.getLogger('curlUtils.py')
logger.setLevel(logging.DEBUG)

if TESTING:
    OPA_SERVER = 'localhost'
else:
    OPA_SERVER = 'opa.fybrik-system'

OPA_PORT = os.getenv("OPA_SERVICE_PORT") if os.getenv("OPA_SERVICE_PORT") else 8181
OPA_ENDPT = os.getenv("OPA_URL") if os.getenv("OPA_URL") else '/v1/data/dataapi/authz/rule'
OPA_HEADER = {"Content-Type": "application/json"}
ASSET_NAMESPACE = os.getenv("ASSET_NAMESPACE") if os.getenv("ASSET_NAMESPACE") else 'default'

def composeAndExecuteOPACurl(role, id):

    ## TBD - role is being put into the header as a string - it should go in as a list for Rego.  What we are doing
    ## now requires the Rego to do a substring search, rather than search in a list
    if TESTING_NO_OPA:
        return({'decision_id': 'ffa2de2a-bc0d-4cb6-9c89-3c385afc3ec5', 'result': [{}, {'action': {'name': 'HashColumn', 'columns': '["id"]', 'description': 'Hash PII values'}}, {'action': {'name': 'Testing', 'columns': '["id"]', 'description': 'Just testing'}}]})
    ## When running as a Fybrik module, the Assets will automatically be sent to OPA as input.  In test mode, let's
    ## read directly the information from the asset yaml file and pass that to OPA here.
    if TESTING:
        ASSET_PATH = '../yaml/asset.yaml'
        try:
            with open(ASSET_PATH, 'r') as stream:
                assetValues = yaml.safe_load(stream)
        except Exception as e:
            raise ValueError('Error reading from file! ' + ASSET_PATH)
        opa_query_body = '{ \"input\": { \
                \"request\": { \
                \"role\": \"' + str(role) + '\", \
                \"id\": \"' + str(id) + '\" \
                }, \
                \"resource\":' + json.dumps(assetValues["spec"]) + '}  \
                }'
    else:
        opa_query_body = '{ \"input\": { \
            \"request\": { \
            \"role\": \"' + str(role) + '\", \
            \"id\": \"' + str(id) + '\" \
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
