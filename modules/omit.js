const omit = (obj, omitKeys) => Object.keys(obj).reduce((acc, key) => {
  if (omitKeys.includes(key)) return acc
  return { ...acc, [key]: obj[key] }
}, {})

module.exports = omit
