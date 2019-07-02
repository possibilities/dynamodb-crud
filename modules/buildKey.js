const buildKey = (path, separator = '.') => {
  const [resource, subjectId] = path

  const rangeParts = path.length === 2
    ? path
    : path.slice(2)
  const range = rangeParts.join(separator)

  const hash = [resource, subjectId].join(separator)
  return { hash, range }
}

module.exports = buildKey
