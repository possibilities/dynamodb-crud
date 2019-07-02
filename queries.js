const omit = require('./modules/omit')
const fromPairs = require('./modules/fromPairs')
const resolveKey = require('./modules/resolveKey')

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
  (key, body) => {
    const resolvedKey = resolveKey(key, context.separator)
    return {
      body,
      context,
      action: 'put',
      request: {
        Item: {
          ...getKey(resolvedKey, context),
          ...body
        },
        ConditionExpression: getConditionExpression(context, '<>'),
        ExpressionAttributeNames: getAttributeNames(context),
        ExpressionAttributeValues: getExpressionAttributeValues(context, resolvedKey)
      }
    }
  }

const get = context =>
  (key, options = {}) => {
    const resolvedKey = resolveKey(key, context.separator)

    let request = { Key: getKey(resolvedKey, context) }
    if (options.keys) {
      request = {
        ...request,
        ProjectionExpression: options.keys.join(', ')
      }
    }

    return { context, action: 'get', request }
  }

const getUpdateExpression = ({ hashKeyName, rangeKeyName }, body) => {
  const nameExpressions = Object
    .keys(omit(body, hashKeyName, rangeKeyName))
    .map(key => `#${key} = :${key}`).join(', ')
  return `set ${nameExpressions}`
}

const update = context =>
  (key, body) => {
    const resolvedKey = resolveKey(key, context.separator)
    return {
      body,
      context,
      action: 'update',
      request: {
        Key: getKey(resolvedKey, context),
        UpdateExpression: getUpdateExpression(context, body),
        ConditionExpression: getConditionExpression(context, '='),
        ExpressionAttributeNames: getAttributeNames(context, body),
        ExpressionAttributeValues: getExpressionAttributeValues(context, { ...resolvedKey, ...body })
      }
    }
  }

const remove = context => {
  return key => {
    const resolvedKey = resolveKey(key, context.separator)
    return {
      context,
      action: 'delete',
      request: {
        Key: getKey(resolvedKey, context),
        ConditionExpression: getConditionExpression(context, '='),
        ExpressionAttributeNames: getAttributeNames(context),
        ExpressionAttributeValues: getExpressionAttributeValues(context, resolvedKey)
      }
    }
  }
}

const list = context =>
  key => {
    const resolvedKey = resolveKey(key, context.separator)
    return {
      context,
      action: 'query',
      request: {
        KeyConditionExpression: getKeyConditionExpression(context),
        ExpressionAttributeNames: getAttributeNames(context),
        ExpressionAttributeValues: getExpressionAttributeValues(context, resolvedKey)
      }
    }
  }

const count = context => {
  const buildListQuery = list(context)
  return key => {
    const listQuery = buildListQuery(key)
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
  const context = { hashKeyName, rangeKeyName, separator }
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
