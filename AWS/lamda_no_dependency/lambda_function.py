import json


def cal_square(n):
    return n**2

def lambda_handler(event, context):
    print("Event Data", event)
    print("Trigger Received!!")
    num = 5
    res_sq = cal_square(num)
    return {
         "statusCode": 200,
         "body": json.dumps({"squareCal": res_sq, "message": "I am from vs code with no dependency"})
         
    } 