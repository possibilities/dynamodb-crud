# DynamoDB CRUD client [![CircleCI](https://circleci.com/gh/possibilities/dynamodb-crud.svg?style=svg)](https://circleci.com/gh/possibilities/dynamodb-crud)

An opinionated CRUD client for [AWS DynamoDB](https://aws.amazon.com/dynamodb/) based on [AWS DynamoDb with Axios](https://github.com/possibilities/aws-dynamodb-axios)

This library is for generating composite key-based queries. See [AWS re:Invent 2018: Amazon DynamoDB Deep Dive: Advanced Design Patterns for DynamoDB (DAT401)](https://www.youtube.com/watch?v=HaEPXoXVf2k) for a primer on this approach to NoSQL database design.

## Usage

For now see [test suite](./__tests__) for usage

## API

### Queries

Provided query builders emit queries that can be executed by the [operation helpers](#operations). You can create your own query builders and/or higher order builders by manipulating existing builder results.

##### `get()`

##### `create()`

##### `patch()`

##### `destroy()`

##### `update()`

##### `list()`

##### `count()`

### Client

#### Configure

##### `dynamodb(obj)`

#### Operations

In addition to the [actions-based API provided by DynamoDb with Axios](https://github.com/possibilities/aws-dynamodb-axios#api) helpers are added for invoking queries generated with the provided query helpers.

##### `invoke(obj|arr)`

##### `batchGet(arr)`

##### `batchWrite(arr)`

##### `transactGet(obj|arr)`

##### `transactWrite(obj|arr)`

#### Interceptors

[Axios inspired](https://github.com/axios/axios#interceptors) interceptors are provided for injecting behavior into database interactions.

##### `dynamodb.interceptors.request.use(fn) => (query)`

##### `dynamodb.interceptors.response.use(fn) => (response, query))`
