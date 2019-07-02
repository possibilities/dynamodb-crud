const buildKey = require('../modules/buildKey')

describe('buildKey', () => {
  test('hash only', () => {
    expect(buildKey(['a', 'b'])).toEqual({
      hash: 'a.b',
      range: 'a.b'
    })
  })

  test('with object', () => {
    expect(buildKey({ hash: 'foo', range: 'bar' })).toEqual({
      hash: 'foo',
      range: 'bar'
    })
  })

  test('with odd length range', () => {
    expect(buildKey(['a', 'b', 'c'])).toEqual({
      hash: 'a.b',
      range: 'c'
    })
  })

  test('with even length range', () => {
    expect(buildKey(['a', 'b', 'c', 'd'])).toEqual({
      hash: 'a.b',
      range: 'c.d'
    })
  })

  test('with custom separator', () => {
    expect(buildKey(['a', 'b', 'c', 'd'], '#')).toEqual({
      hash: 'a#b',
      range: 'c#d'
    })
  })
})
