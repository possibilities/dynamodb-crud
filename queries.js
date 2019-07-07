const omit = require('./modules/omit')
const fromPairs = require('./modules/fromPairs')
const resolveKey = require('./modules/resolveKey')

const prepareKey = (item, context) => ({
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

const getAttributeNames = ({ hashKeyName, rangeKeyName }, body, options) => {
  const data = {
    ...keyMirror(body),
    [hashKeyName]: hashKeyName,
    [rangeKeyName]: rangeKeyName
  }
  return {
    ...(options.ExpressionAttributeNames || {}),
    ...fromPairs(Object.keys(data).map(key => ([`#${key}`, data[key]])))
  }
}

const getUpdateExpression = ({ hashKeyName, rangeKeyName }, body) => {
  const nameExpressions = Object
    .keys(omit(body, hashKeyName, rangeKeyName))
    .map(key => `#${key} = :${key}`).join(', ')
  return `SET ${nameExpressions}`
}

const getConditionExpression = (context, comparator) =>
  `#${context.hashKeyName} ${comparator} :${context.hashKeyName} AND ` +
  `#${context.rangeKeyName} ${comparator} :${context.rangeKeyName}`

const getKeyConditionExpression = context =>
  `#${context.hashKeyName} = :${context.hashKeyName} AND ` +
  `begins_with(#${context.rangeKeyName}, :${context.rangeKeyName})`

const post = context =>
  (...args) => {
    const [key, body, options = {}] = resolveKey(context, ...args)
    return {
      key,
      body,
      context,
      action: 'put',
      request: {
        Item: {
          ...prepareKey(key, context),
          ...body
        },
        ConditionExpression: getConditionExpression(context, '<>'),
        ExpressionAttributeNames: getAttributeNames(context, {}, options),
        ExpressionAttributeValues: getExpressionAttributeValues(context, key),
        ...options
      }
    }
  }

const update = context =>
  (...args) => {
    const [key, body, options = {}] = resolveKey(context, ...args)
    const putQuery = post(context)(key, body)
    return {
      ...putQuery,
      request: {
        ...putQuery.request,
        ConditionExpression: getConditionExpression(context, '='),
        ...options
      }
    }
  }

const get = context =>
  (...args) => {
    const [key, options = {}] = resolveKey(context, ...args)
    return {
      key,
      context,
      action: 'get',
      request: { Key: prepareKey(key, context), ...options }
    }
  }

const patch = context =>
  (...args) => {
    const [key, body, options = {}] = resolveKey(context, ...args)
    return {
      key,
      body,
      context,
      action: 'update',
      request: {
        Key: prepareKey(key, context),
        UpdateExpression: getUpdateExpression(context, body),
        ConditionExpression: getConditionExpression(context, '='),
        ExpressionAttributeNames: getAttributeNames(context, body, options),
        ExpressionAttributeValues: getExpressionAttributeValues(
          context,
          { ...key, ...body }
        ),
        ...options
      }
    }
  }

const del = context => {
  return (...args) => {
    const [key, options = {}] = resolveKey(context, ...args)
    return {
      key,
      context,
      action: 'delete',
      request: {
        Key: prepareKey(key, context),
        ConditionExpression: getConditionExpression(context, '='),
        ExpressionAttributeNames: getAttributeNames(context, {}, options),
        ExpressionAttributeValues: getExpressionAttributeValues(context, key),
        ...options
      }
    }
  }
}

const list = context => (...args) => {
  const [key, options = {}] = resolveKey(context, ...args)
  return {
    key,
    context,
    action: 'query',
    request: {
      KeyConditionExpression: getKeyConditionExpression(context),
      ExpressionAttributeValues: getExpressionAttributeValues(context, key),
      ...options,
      ExpressionAttributeNames: getAttributeNames(context, {}, options)
    }
  }
}

const count = context => {
  const buildListQuery = list(context)
  return (...args) => {
    const [key, options = {}] = resolveKey(context, ...args)
    const listQuery = buildListQuery(key, options)
    return {
      ...listQuery,
      request: {
        ...listQuery.request,
        Select: 'COUNT',
        ...options
      }
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
    post: post(context),
    get: get(context),
    patch: patch(context),
    update: update(context),
    delete: del(context),
    list: list(context),
    count: count(context)
  }
}

module.exports = queries
