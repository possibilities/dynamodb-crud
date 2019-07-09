const dynamodb = require('aws-dynamodb-axios')
const chunk = require('./modules/chunk')
const upperFirst = require('./modules/upperFirst')
const isEmpty = require('./modules/isEmpty')
const compose = require('./modules/compose')
const { unmarshall, marshall } = require('aws-dynamodb-axios')

const failedConditionType = 'ConditionalCheckFailedException'
const failedTranactionConditionType = 'TransactionCanceledException'

const existsOrNullInTransaction = async invoking => {
  try {
    return await invoking
  } catch (error) {
    if (
      error.statusCode === 400 &&
      error.data.__type.endsWith(failedTranactionConditionType)
    ) {
      return null
    }
    throw error
  }
}

const existsOrNull = async invoking => {
  try {
    return await invoking
  } catch (error) {
    if (
      error.statusCode === 400 &&
      error.data.__type.endsWith(failedConditionType)
    ) {
      return null
    }
    throw error
  }
}

const marshallRequest = request => {
  let marshalled = { ...request }
  if (request.Key) {
    marshalled = {
      ...marshalled,
      Key: marshall(request.Key)
    }
  }
  if (request.Item) {
    marshalled = {
      ...marshalled,
      Item: marshall(request.Item)
    }
  }
  if (request.ExpressionAttributeValues) {
    marshalled = {
      ...marshalled,
      ExpressionAttributeValues: marshall(request.ExpressionAttributeValues)
    }
  }
  if (request.ExclusiveStartKey) {
    marshalled = {
      ...marshalled,
      ExclusiveStartKey: marshall(request.ExclusiveStartKey)
    }
  }
  return marshalled
}

const ensureArray = arr => Array.isArray(arr)
  ? arr
  : [arr]

const invoke = (db, config = {}) => async (query, options = {}) => {
  if (Array.isArray(query)) {
    return Promise.all(query.map(q => invoke(db, config)(q, options)))
  }

  const request = {
    ...marshallRequest(query.request),
    TableName: config.tableName
  }

  switch (query.action) {
    case 'get':
      const getResult = await db.get(request)
      return isEmpty(getResult)
        ? null
        : unmarshall(getResult.Item)

    case 'put':
      const putResult = await db.put(request)
      return (putResult && query.body) || null

    case 'delete':
      return await existsOrNull(db.delete(request))
        ? {}
        : null

    case 'query':
      const getItems = await db.query(request)
      // Detect that we're invoking a count()
      if (!getItems.Items && getItems.Count !== undefined) {
        return getItems.Count
      }
      const items = getItems.Items.map(item => unmarshall(item))
      const lastKey = getItems.LastEvaluatedKey
        ? unmarshall(getItems.LastEvaluatedKey)
        : null
      return { items, lastKey }

    case 'update':
      const patchResult = await existsOrNull(db.update(request))
      return isEmpty(patchResult)
        ? null
        : unmarshall(patchResult.Attributes)
  }

  throw new Error(`\`invoke\` does not support ${query.action} action`)
}

const batchWrite = (db, config = {}) => async (queries, options = {}) => {
  for (const queriesChunk of chunk(queries, 25)) {
    await db.batchWrite({
      RequestItems: {
        [config.tableName]: queriesChunk.map(
          ({ request, action }) =>
            ({ [`${upperFirst(action)}Request`]: marshallRequest(request) })
        )
      }
    })
  }
  return queries.map(q => q.body)
}

const batchGet = (db, config = {}) => async (queries, options = {}) => {
  let items = []
  for (const queriesChunk of chunk(queries, 25)) {
    const responses = await db.batchGet({
      RequestItems: {
        [config.tableName]: {
          Keys: queriesChunk.map(
            ({ request, action }) => marshall(request.Key)
          )
        }
      }
    })
    items = [...items, ...responses.Responses[config.tableName]]
  }

  return items.map((item, index) => unmarshall(item))
}

const transactGet = (db, config = {}) => async (queries, options = {}) => {
  let items = []

  const queriesArr = ensureArray(queries)
  for (const queriesChunk of chunk(queriesArr, 25)) {
    const responses = await db.transactGet({
      TransactItems: queriesChunk.map(
        ({ request, action }) => ({
          Get: {
            Key: marshall(request.Key),
            TableName: config.tableName
          }
        })
      )
    })
    items = [...items, ...responses.Responses]
  }

  const preparedItems = items.map((item, index) => unmarshall(item.Item))
  return queries.length > 1
    ? preparedItems
    : preparedItems.pop()
}

const transactWrite = (db, config = {}) => async queries => {
  const queriesArr = ensureArray(queries)

  let hasNulls = false
  for (const queriesChunk of chunk(queriesArr, 25)) {
    const responses = await existsOrNullInTransaction(db.transactWrite({
      TransactItems: queriesChunk.map(query => ({
        [upperFirst(query.action)]: {
          ...marshallRequest(query.request),
          TableName: config.tableName
        }
      }))
    }))

    if (responses === null) {
      hasNulls = true
      break
    }
  }

  if (hasNulls) return null

  if (queriesArr[0].action === 'delete') {
    return {}
  }

  const bodies = queriesArr.map(q => q.body)

  if (queriesArr.length === 1) {
    return bodies.pop()
  }

  return bodies
}

const rest = (config = {}) => {
  const db = dynamodb(config)
  const interceptors = { request: [], response: [] }

  const intercept = handler => async (queries, ...args) => {
    const interceptRequest = compose(...interceptors.request)
    const interceptResponse = compose(...interceptors.response)
    queries = Array.isArray(queries)
      ? await Promise.all(queries.map(interceptRequest))
      : await interceptRequest(queries)
    const responses = await handler(queries, ...args)
    return Array.isArray(responses)
      ? Promise.all(responses.map((response, i) => interceptResponse(response, queries[i])))
      : interceptResponse(responses, queries)
  }

  return {
    invoke: intercept(invoke(db, config)),
    transactGet: intercept(transactGet(db, config)),
    transactWrite: intercept(transactWrite(db, config)),
    batchWrite: intercept(batchWrite(db, config)),
    batchGet: intercept(batchGet(db, config)),
    interceptors: {
      request: { use: handler => interceptors.request.push(handler) },
      response: { use: handler => interceptors.response.push(handler) }
    }
  }
}

module.exports = rest
