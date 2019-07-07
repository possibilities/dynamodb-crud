const compose = (...fns) =>
  (fns.length ? fns : [x => x]).reduce((f, g) => (...args) => f(g(...args)))

module.exports = compose
