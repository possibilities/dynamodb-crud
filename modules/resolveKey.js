const resolveKey = (key, { separator, hashKeyName, rangeKeyName }) => {
  if (typeof key === 'string') {
    key = key.split(separator)
  } else if (!Array.isArray(key)) {
    return key
  }

  if (!Array.isArray(key)) return key

  const [resourceName, resourceId] = key

  const rangeParts = key.length === 2
    ? key
    : key.slice(2)
  const range = rangeParts.join(separator)

  const hash = [resourceName, resourceId].join(separator)
  return { [hashKeyName]: hash, [rangeKeyName]: range }
}

module.exports = resolveKey
