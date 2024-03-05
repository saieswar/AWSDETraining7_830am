import requests


def lambda_handler(event, context):
    print('Event Data',event)
    response = requests.get("https://www.google.com")
    return response.text