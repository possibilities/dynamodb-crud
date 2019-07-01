#!/bin/sh

set -e

export dynamoDbRegion=1
export dynamoDbHost=127.0.0.1:9987
export dynamoDbTableName=AwsDynamoDbAxiosTest

jest --runInBand --verbose "$@"
