const omit = require('./modules/omit')
const fromPairs = require('./modules/fromPairs')
const resolveKey = require('./modules/resolveKey')

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

const getKeyExistsCondition = context =>
  `#${context.hashKeyName} = :${context.hashKeyName} AND ` +
  `#${context.rangeKeyName} = :${context.rangeKeyName}`

const getKeyNotExistsCondition = context =>
  `#${context.hashKeyName} <> :${context.hashKeyName} AND ` +
  `#${context.rangeKeyName} <> :${context.rangeKeyName}`

const getQueryKeyConditionExpression = context =>
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
        Item: { ...key, ...body },
        ConditionExpression: getKeyNotExistsCondition(context),
        ExpressionAttributeValues: getExpressionAttributeValues(context, key),
        ...options,
        ExpressionAttributeNames: getAttributeNames(context, {}, options),
      }
    }
  }

const put = context =>
  (...args) => {
    const [key, body, options = {}] = resolveKey(context, ...args)
    return {
      key,
      body,
      context,
      action: 'put',
      request: {
        Item: { ...key, ...body },
        ConditionExpression: getKeyExistsCondition(context),
        ExpressionAttributeValues: getExpressionAttributeValues(context, key),
        ...options,
        ExpressionAttributeNames: getAttributeNames(context, {}, options),
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
      request: { Key: key, ...options }
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
        Key: key,
        ReturnValues: 'ALL_NEW',
        UpdateExpression: getUpdateExpression(context, body),
        ConditionExpression: getKeyExistsCondition(context),
        ExpressionAttributeValues: getExpressionAttributeValues(
          context,
          { ...key, ...body }
        ),
        ...options,
        ExpressionAttributeNames: getAttributeNames(context, body, options),
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
        Key: key,
        ConditionExpression: getKeyExistsCondition(context),
        ExpressionAttributeValues: getExpressionAttributeValues(context, key),
        ...options,
        ExpressionAttributeNames: getAttributeNames(context, {}, options),
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
      KeyConditionExpression: getQueryKeyConditionExpression(context),
      ExpressionAttributeValues: getExpressionAttributeValues(context, key),
      ...options,
      ExpressionAttributeNames: getAttributeNames(context, {}, options)
    }
  }
}

const count = context => (...args) => {
  const [key, options = {}] = resolveKey(context, ...args)
  return {
    key,
    context,
    action: 'query',
    request: {
      KeyConditionExpression: getQueryKeyConditionExpression(context),
      ExpressionAttributeValues: getExpressionAttributeValues(context, key),
      ExpressionAttributeNames: getAttributeNames(context, {}, options),
      Select: 'COUNT',
      ...options
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
    put: put(context),
    delete: del(context),
    list: list(context),
    count: count(context)
  }
}

module.exports = queries
