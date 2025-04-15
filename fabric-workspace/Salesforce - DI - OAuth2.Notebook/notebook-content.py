# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# *** Authentication using OAuth 2.0 JWT Bearer - Important Links Below
# https://help.salesforce.com/s/articleView?id=sf.remoteaccess_oauth_jwt_flow.htm&type=5
# https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/quickstart_code.htm
# https://github.com/jhownfs/jwt-oauth--salesforce
# https://gist.github.com/booleangate/30d345ecf0617db0ea19c54c7a44d06f
# ********************************************

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import time
import requests
from Crypto.PublicKey import RSA
from base64 import b64decode
import jwt

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

class SalesforceOAuth2(object):
    _token_url = '/services/oauth2/token'

    def __init__(self, client_id, username, privatekey, sandbox):

        if sandbox:
            self.auth_site = 'test'
        else:
            self.auth_site = 'login'

        self.client_id = client_id
        self.username = username
        self.privatekey = privatekey

    def get_access_token(self):
        """
        Sets the body of the POST request
        :return: POST response
        """
        claim = {
            'iss': self.client_id,
            'exp': int(time.time()) + 300,
            'aud': 'https://{}.salesforce.com'.format(self.auth_site),
            'sub': self.username,
        }

        response = self._request_token(claim)

        return response

    def _request_token(self, data):
        """
        Sends a POST request to Salesforce to authenticate credentials
        :param data: claim of the POST request
        :return: POST response
        """
        privateKeyDecoded = b64decode(self.privatekey)
        rsaSecurityKey = RSA.importKey(privateKeyDecoded)

        assertion = jwt.encode(data, rsaSecurityKey.export_key('PEM'), algorithm='RS256', headers={'alg':'RS256'})

        response = requests.post('https://{}.salesforce.com/services/oauth2/token'.format(self.auth_site), data = {
            'grant_type': 'urn:ietf:params:oauth:grant-type:jwt-bearer',
            'assertion': assertion,
        })

        return response


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
