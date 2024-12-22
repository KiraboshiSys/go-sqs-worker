#!/bin/bash

awslocal sqs create-queue \
  --queue-name worker-queue \
  --region ap-northeast-1

awslocal sqs create-queue \
  --queue-name dead-letter-queue \
  --region ap-northeast-1
