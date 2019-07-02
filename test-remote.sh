#!/bin/sh

set -e

export dynamoDbRegion=us-east-2
export dynamoDbTableName=DynamoDbCrud

jest --runInBand "$@"
