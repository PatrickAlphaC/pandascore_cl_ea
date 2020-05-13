# coding: utf-8
import requests
import json
import os
import logging as log
import time
import sys

log.basicConfig(level=log.INFO)

api_key = os.getenv("PANDASCORE_API_KEY")
_API_URL_PREFIX = "https://api.pandascore.co/"
_RETRIES = 5


def lambda_handler(event, context):
    result = handler(event)
    return result


def gcs_handler(request):
    av_data = request.json
    result = handler(av_data)
    return json.dumps(result)


def handler(av_request_data):
    if 'data' not in av_request_data:
        av_request_data['data'] = {}
    if 'id' not in av_request_data:
        av_request_data['id'] = ""
    query_url = create_api_url(av_request_data['data'])
    log.info("Request data " + str(av_request_data['data']))
    log.info("Resulting query " + query_url)

    response, json_response = handle_api_call(query_url, _RETRIES)
    error_string = None

    if not json_response:
        error_string = 'Error getting data from the api, no return was given.'
        log.error(error_string)
    elif "Error Message" in json_response:
        error_string = json_response["Error Message"]
        log.error(error_string)
    elif "Information" in json_response:
        error_string = json_response["Information"]
        log.info(error_string)
    elif "Note" in json_response:
        error_string = json_response["Note"]
        log.info(error_string)

    adapter_result = {'jobRunID': av_request_data['id'],
                      'data': json_response,
                      'status': str(response.status_code)}

    if error_string is not None:
        adapter_result['error'] = error_string
    return adapter_result


def create_api_url(data):
    url = ""
    url += _API_URL_PREFIX
    if isinstance(data['url'], str):
        url += data['url']
    return url + "token=" + api_key


def handle_api_call(query_url, retries):
    try:
        response = requests.get(query_url)
        json_response = response.json()
    except:
        e = sys.exc_info()[0]
        log.warning(
            "Retring with {} retries left, due to: {}".format(retries, e))
        log.warning("This was run with {}".format(query_url))
        if retries <= 0:
            sys.exit()
            return None
        time.sleep(0.1)
        return handle_api_call(query_url, retries - 1)

    if retries > 0:
        if not json_response:
            log.warning(
                "Retring with {} retries left, due to no response".format(retries))
            time.sleep(0.1)
            return handle_api_call(query_url, retries - 1)
        elif "Error Message" in json_response:
            log.info(
                "Retring with {} retries left, due to Error message".format(retries))
            time.sleep(0.1)
            return handle_api_call(query_url, retries - 1)
    return response, json_response
