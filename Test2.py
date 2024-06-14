
docker run --rm -v $(pwd):/var/task public.ecr.aws/lambda/python:3.8 pip install -r requirements.txt -t .
docker run --rm -v $(pwd):/var/task lambci/lambda:build-python3.8 pip install -r requirements.txt -t .
