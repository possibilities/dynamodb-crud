const resolveKey = ({ separator, hashKeyName, rangeKeyName }, ...args) => {
  if (Array.isArray(args[0])) {
    let hashKey
    let rangeKey
    let additionalArgs

    if (Array.isArray(args[1])) {
      const [hash, range] = args
      hashKey = hash.join(separator)
      rangeKey = range.join(separator)
      additionalArgs = args.slice(2)
    } else {
      const [hash] = args
      hashKey = hash.join(separator)
      rangeKey = hash.join(separator)
      additionalArgs = args.slice(1)
    }

    return [
      {
        [hashKeyName]: hashKey,
        [rangeKeyName]: rangeKey
      },
      ...additionalArgs
    ]
  }

  if (typeof args[0] === 'object') {
    const [key, ...additionalArgs] = args
    return [
      {
        [hashKeyName]: Array.isArray(key[hashKeyName])
          ? key[hashKeyName].join(separator)
          : key[hashKeyName],
        [rangeKeyName]: Array.isArray(key[rangeKeyName])
          ? key[rangeKeyName].join(separator)
          : key[rangeKeyName]
      },
      ...additionalArgs
    ]
  }
}

module.exports = resolveKey
