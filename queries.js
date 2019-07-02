const omit = require('./modules/omit')
const fromPairs = require('./modules/fromPairs')
const buildKey = require('./modules/buildKey')

const getKey = ({ hash, range }, context) => ({
  [context.hashKeyName]: hash,
  [context.rangeKeyName]: range
})

const getExpressionAttributeValues = ({ hashKeyName, rangeKeyName }, body) => {
  const data = {
    ...body,
    [`${hashKeyName}`]: body[hashKeyName],
    [`${rangeKeyName}`]: body[rangeKeyName]
  }
  return fromPairs(
    Object.keys(data).map(key => ([`:${key}`, data[key]]))
  )
}

const keyMirror = obj => {
  let mirror = {}
  Object.keys(obj).forEach(key => {
    mirror[key] = key
  })
  return mirror
}

const getAttributeNames = ({ hashKeyName, rangeKeyName }, body = {}) => {
  const data = {
    ...keyMirror(body),
    [hashKeyName]: hashKeyName,
    [rangeKeyName]: rangeKeyName
  }
  return fromPairs(Object.keys(data).map(key => ([`#${key}`, data[key]])))
}

const getConditionExpression = (context, comparator) =>
  `#${context.hashKeyName} ${comparator} :${context.hashKeyName} AND ` +
  `#${context.rangeKeyName} ${comparator} :${context.rangeKeyName}`

const getKeyConditionExpression = context =>
  `#${context.hashKeyName} = :${context.hashKeyName} AND ` +
  `begins_with(#${context.rangeKeyName}, :${context.rangeKeyName})`

const create = context =>
  (path, body) => {
    const key = buildKey(path, context.separator)
    return {
      action: 'put',
      request: {
        Item: {
          ...getKey(key, context),
          ...body,
          createdAt: context.stamp,
          updatedAt: context.stamp
        },
        ConditionExpression: getConditionExpression(context, '<>'),
        ExpressionAttributeNames: getAttributeNames(context),
        ExpressionAttributeValues: getExpressionAttributeValues(context, key)
      }
    }
  }

const get = context =>
  (path, options = {}) => {
    const key = buildKey(path, context.separator)

    let request = { Key: getKey(key, context) }
    if (options.keys) {
      request = {
        ...request,
        ProjectionExpression: options.keys.join(', ')
      }
    }

    return { action: 'get', request }
  }

const getUpdateExpression = ({ hashKeyName, rangeKeyName }, body) => {
  const nameExpressions = Object
    .keys(omit(body, hashKeyName, rangeKeyName))
    .map(key => `#${key} = :${key}`).join(', ')
  return `set ${nameExpressions}`
}

const update = context =>
  (path, body) => {
    const key = buildKey(path, context.separator)
    const data = { ...body, updatedAt: context.stamp }
    return {
      action: 'update',
      request: {
        Key: getKey(key, context),
        UpdateExpression: getUpdateExpression(context, data),
        ConditionExpression: getConditionExpression(context, '='),
        ExpressionAttributeNames: getAttributeNames(context, data),
        ExpressionAttributeValues: getExpressionAttributeValues(context, { ...key, ...data })
      }
    }
  }

const remove = context => {
  return path => {
    const key = buildKey(path, context.separator)
    return {
      action: 'delete',
      request: {
        Key: getKey(key, context),
        ConditionExpression: getConditionExpression(context, '='),
        ExpressionAttributeNames: getAttributeNames(context),
        ExpressionAttributeValues: getExpressionAttributeValues(context, key)
      }
    }
  }
}

const list = context =>
  path => {
    const key = buildKey(path, context.separator)
    return {
      action: 'query',
      request: {
        KeyConditionExpression: getKeyConditionExpression(context),
        ExpressionAttributeNames: getAttributeNames(context),
        ExpressionAttributeValues: getExpressionAttributeValues(context, key)
      }
    }
  }

const count = context => {
  const buildListQuery = list(context)
  return path => {
    const listQuery = buildListQuery(path)
    return {
      ...listQuery,
      request: { ...listQuery.request, Select: 'COUNT' }
    }
  }
}

const queries = ({
  separator = '.',
  hashKeyName = 'hash',
  rangeKeyName = 'range'
} = {}) => {
  const stamp = new Date().toISOString()
  const context = { stamp, hashKeyName, rangeKeyName, separator }
  return {
    create: create(context),
    get: get(context),
    update: update(context),
    remove: remove(context),
    list: list(context),
    count: count(context)
  }
}

module.exports = queries
