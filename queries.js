const omit = require('./modules/omit')
const fromPairs = require('./modules/fromPairs')
const resolveKey = require('./modules/resolveKey')

const getKey = (item, context) => ({
  [context.hashKeyName]: item[context.hashKeyName],
  [context.rangeKeyName]: item[context.rangeKeyName]
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
    const resolvedKey = resolveKey(key, context)
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
        ExpressionAttributeValues: getExpressionAttributeValues(
          context,
          resolvedKey
        )
      }
    }
  }

const update = context =>
  (key, body) => {
    const putQuery = create(context)(key, body)
    return {
      ...putQuery,
      request: {
        ...putQuery.request,
        ConditionExpression: getConditionExpression(context, '=')
      }
    }
  }

const get = context =>
  (key, options = {}) => {
    const resolvedKey = resolveKey(key, context)
    return {
      context,
      action: 'get',
      request: { Key: getKey(resolvedKey, context), ...options }
    }
  }

const getUpdateExpression = ({ hashKeyName, rangeKeyName }, body) => {
  const nameExpressions = Object
    .keys(omit(body, hashKeyName, rangeKeyName))
    .map(key => `#${key} = :${key}`).join(', ')
  return `set ${nameExpressions}`
}

const patch = context =>
  (key, body) => {
    const resolvedKey = resolveKey(key, context)
    return {
      body,
      context,
      action: 'update',
      request: {
        Key: getKey(resolvedKey, context),
        UpdateExpression: getUpdateExpression(context, body),
        ConditionExpression: getConditionExpression(context, '='),
        ExpressionAttributeNames: getAttributeNames(context, body),
        ExpressionAttributeValues: getExpressionAttributeValues(
          context,
          { ...resolvedKey, ...body }
        )
      }
    }
  }

const destroy = context => {
  return key => {
    const resolvedKey = resolveKey(key, context)
    return {
      context,
      action: 'delete',
      request: {
        Key: getKey(resolvedKey, context),
        ConditionExpression: getConditionExpression(context, '='),
        ExpressionAttributeNames: getAttributeNames(context),
        ExpressionAttributeValues: getExpressionAttributeValues(
          context,
          resolvedKey
        )
      }
    }
  }
}

const list = context => (key, options = {}) => ({
  context,
  action: 'query',
  request: {
    KeyConditionExpression: getKeyConditionExpression(context),
    ExpressionAttributeNames: getAttributeNames(context),
    ExpressionAttributeValues: getExpressionAttributeValues(
      context,
      resolveKey(key, context)
    ),
    ...options
  }
})

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
    patch: patch(context),
    update: update(context),
    destroy: destroy(context),
    list: list(context),
    count: count(context)
  }
}

module.exports = queries
