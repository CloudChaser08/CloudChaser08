# Configuration and Constants for DAGs

SLACK_CHANNEL = '#dev'

DECRYPTOR_JAR_LOCATION = 's3://healthverityreleases/vanderbilt/HVDecryptor.jar'
DECRYPTOR_KEY_LOCATION = 's3://salusv/keys/hv_record_private.base64.reformat'

REDSHIFT_HOST_URL_TEMPLATE = '{}.cz8slgfda3sg.us-east-1.redshift.amazonaws.com'
REDSHIFT_USER = 'hvuser'
REDSHIFT_DATABASE = 'dev'
REDSHIFT_PORT = '5439'
