# Support JWT token for OAuth 2.1
import logging
import jwt

from constants import USER_KEY, ROLE_KEY, TESTING

logger = logging.getLogger('jwtUtils.py')
logger.setLevel(logging.DEBUG)
logger.info('setting log level to DEBUG')

def getTokenDict(request):
    noJWT = True
    payloadEncrypted = request.headers.get('Authorization')
    role = 'Not defined'
    id = 'Not defined'
    if (payloadEncrypted == None):
        logger.debug("----> payloadEncrypted = None !!")
    if (payloadEncrypted != None):
        noJWT = False
        roleKey = ROLE_KEY
        try:
            role = decryptJWT(payloadEncrypted, roleKey)
            # Role will be returned as a list.  To make life simple in the policy, assume that only
            # first element is the real role.  Maybe fix this one day...
            if type(role) is list:
                role = role[0]
        except:
            logger.error("Error: no role in JWT!")
            role = 'ERROR NO ROLE!'

        id = USER_KEY
        try:
            user = decryptJWT(payloadEncrypted, id)
        except:
            logger.error("No id in JWT!")
    if (noJWT):
        role = request.headers.get('role')   # testing only
    if (role == None):
        role = 'ERROR NO ROLE!'
    logger.debug('-> role = ' + str(role) +  " user = " + str(id))
    tokenDict = {"role": str(role), "id": str(id)}

    return(tokenDict)

def decryptJWT(encryptedToken, flatKey):
# String with "Bearer <token>".  Strip out "Bearer"...
    prefix = 'Bearer'
    assert encryptedToken.startswith(prefix), '\"Bearer\" not found in token' + encryptedToken
    strippedToken = encryptedToken[len(prefix):].strip()
 #   decodedJWT = jwt.api_jwt.decode(strippedToken, options={"verify_signature": False})
    decodedJWT = jwt.decode(strippedToken, options={"verify_signature": False})
#    logger.info(decodedJWT)

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