const dynamodb = require('aws-dynamodb-axios')
const chunk = require('./modules/chunk')
const { unmarshall } = require('aws-dynamodb-axios')

const isEmpty = obj => !Object.keys(obj).length
const upperFirst = str => str[0].toUpperCase() + str.slice(1)

const itemFor = query => unmarshall(Object.values(query).pop().Item)
const failedConditionType = 'ConditionalCheckFailedException'

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

const invoke = db => async (query, options = {}) => {
  if (Array.isArray(query)) {
    return Promise.all(query.map(q => invoke(db)(q, options)))
  }

  const request = {
    ...query.request,
    TableName: process.env.dynamoDbTableName
  }

  switch (query.action) {
    case 'get':
      const getResult = await db.get(request)
      return isEmpty(getResult)
        ? null
        : unmarshall(getResult.Item)
    case 'put':
      const putResult = await db.put(request)
      if (putResult === null) return null
      return unmarshall(request.Item)
    case 'delete':
      return await existsOrNull(db.delete(request))
        ? {}
        : null
    case 'query':
      const getItems = await db.query(request)
      if (!getItems.Items && getItems.Count !== undefined) return getItems.Count
      return unmarshall(getItems.Items)
    case 'update':
      const updateResult = await existsOrNull(db.update(request))
      if (updateResult === null) return null
      return invoke(db)({
        action: 'get',
        request: { Key: request.Key }
      })
  }

  throw new Error(`query does not support ${query.action} action`)
}

const batch = db => async (queries, options = {}) => {
  for (const queriesChunk of chunk(queries, 25)) {
    await db.batchWrite({
      RequestItems: {
        [process.env.dynamoDbTableName]: queriesChunk.map(
          ({ request, action }) =>
            ({ [`${upperFirst(action)}Request`]: request })
        )
      }
    })
  }
  return queries.map(itemFor)
}

const ensureArray = arr => Array.isArray(arr)
  ? arr
  : [arr]

const transact = db => async queries => {
  const queriesArr = ensureArray(queries)
  for (const queriesChunk of chunk(queriesArr, 25)) {
    await existsOrNull(db.transactWrite({
      TransactItems: queriesChunk.map(query => ({
        [upperFirst(query.action)]: {
          ...query.request,
          TableName: process.env.dynamoDbTableName
        }
      }))
    }))
  }
  return queriesArr.map(itemFor)
}

const crud = (options = {}) => {
  const db = dynamodb(options)
  return {
    batch: batch(db),
    invoke: invoke(db),
    transact: transact(db)
  }
}

module.exports = crud
