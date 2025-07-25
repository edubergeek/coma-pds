# base image
FROM python:3.9.1-alpine

RUN apk add build-base
RUN apk add postgresql-client
RUN apk add bash

RUN /usr/local/bin/python -m pip install --upgrade pip

# set working directory
RUN mkdir -p /usr/src/build
WORKDIR /usr/src/build

# add requirements (to leverage Docker cache)
ADD ./requirements.txt requirements.txt

# install requirements
RUN pip install -r requirements.txt

