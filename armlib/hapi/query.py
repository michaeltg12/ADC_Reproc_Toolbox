import logging
import requests
import json

class HAPIError(Exception):
    pass

class HAPIRequest(object):
    def __init__(self, **kwargs):
        for key in kwargs:
            setattr(self, key, kwargs[key])


class HAPI(object):
    def __init__(self, authorization):
        self.logger = logging.getLogger('root.snapi')
        self.authorization = authorization
        self.headers = {"Accept": "application/json", "Authorization": self.authorization}

    def getHapiJson(self, url, query, fields=None):
        if fields:
            parameters = {"sysparm_query": query, "sysparm_fields": fields}
        else:
            parameters = {"sysparm_query": query}

        self.logger.debug("\n***Starting query: ", query)
        response = requests.get(url, headers=self.headers, params=parameters)

        if response.status_code != 200:
            self.logger.critical("Status:", response.status_code)
            self.logger.critical("Headers:", response.headers)
            self.logger.critical("Error Response:", response.json())
            raise HAPIError('Server Responded with error.')
        data = response.json()
        data = data['result']
        return data

    def getHapiObject(self, url, query, fields=None):
        results = self.getHapiJson(url, query, fields)
        snapi_objs = []
        for result in results:
            snapi_objs.append(HAPIRequest(**result))
        return snapi_objs

    def putHapi(self, url, parameters):
        response = requests.put(url, headers=self.headers, data=str(parameters))
        if response.status_code != 200:
            # print("proxyKey =>", proxyKey)
            self.logger.critical("Status:%s", response.status_code)
            self.logger.critical("Headers:%s", response.headers)
            try:
                err_response = response.json()
            except:
                err_response = str(response)
            self.logger.critical("Error Response:%s", err_response)
            self.logger.critical("Error Received in main query response !!!")
            raise HAPIError('Unable to put request to %s.'%(url))
        return response

    def postHapi(self, url, parameters):
        response = requests.post(url, headers=self.headers, data=json.dumps(parameters))
        if response.status_code != 200:
            # print("proxyKey =>", proxyKey)
            self.logger.critical("Status:%s", response.status_code)
            self.logger.critical("Headers:%s", response.headers)
            try:
                err_response = response.json()
            except:
                err_response = str(response)
            self.logger.critical("Error Response:%s", err_response)
            self.logger.critical("Error Received in main query response !!!")
            raise HAPIError('Unable to put request to %s.'%(url))
        return response
