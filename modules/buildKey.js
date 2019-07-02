const buildKey = (path, separator) => {
  const [resource, subjectId] = path
  const hash = [resource, subjectId].join(separator)
  const range = path.join(separator)
  return { hash, range }
}

module.exports = buildKey
