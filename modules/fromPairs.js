const fromPairs = pairs => {
  let obj = {}
  pairs.forEach(([key, val]) => {
    obj[key] = val
  })
  return obj
}

module.exports = fromPairs
