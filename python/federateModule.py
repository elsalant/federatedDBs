import logging
from constants import TESTING
import flaskUtils
import yaml
import config

if TESTING:
    CM_PATH = '../yaml/postgres_config.yaml'
else:      # Mount from Fybrik
    CM_PATH = '/etc/conf/conf.yaml' # from the "volumeMounts" parameter in templates/deployment.yaml
if TESTING:
    PIIfields = ['id', 'name', 'maritalStatus']

PATIENT_QUERY = "select a.id ID, a.resource SOURCE1, c.resource SOURCE2 \
    from postgresqlk8s.public.patient a, postgresql.public.patient c \
    where a.id = '6fcf56ed-8ad8-4395-a966-9ebee3822656' and c.id='6fcf56ed-8ad8-4395-a966-9ebee3822656';"
ALL_QUERY = "select a.id ID, a.resource SOURCE1, c.resource SOURCE2 \
    from postgresqlk8s.public.patient a, postgresql.public.patient c \
    where a.id = c.id limit 2"

logger = logging.getLogger(__name__)

def readConfig(path):
    try:
        with open(path, 'r') as stream:
            cmReturn = yaml.safe_load(stream)
    except Exception as e:
        raise ValueError('Error reading from file! ' + path)
#    else:    # Basic testing mode
#        config.cmDict = {'REGISTRIES': ['postgresql1.public', 'postgresql.public' ]}
#        return(config.cmDict)
    config.cmDict = cmReturn.get('data', [])
    logger.info(f'cmReturn = ', cmReturn)
    return(config.cmDict)

def main():
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    readConfig(CM_PATH)
    logger.info(f"starting module!")

if __name__ == "__main__":
    main()
    flaskUtils.startServer()