
docker pull public.ecr.aws/sam/emulation-python3.8

# hello_world/app.py
import json

def lambda_handler(event, context):
    return {
        'statusCode': 200,
        'body': json.dumps('Hello, World!')
    }





AWSTemplateFormatVersion: '2010-09-09'
Transform: AWS::Serverless-2016-10-31
Resources:
  HelloWorldFunction:
    Type: AWS::Serverless::Function
    Properties:
      Handler: hello_world.app.lambda_handler
      Runtime: python3.8
      CodeUri: hello_world/
      MemorySize: 128
      Timeout: 3
      Policies:
        - AWSLambdaBasicExecutionRole
      Events:
        HelloWorldApi:
          Type: Api
          Properties:
            Path: /hello
            Method: get
