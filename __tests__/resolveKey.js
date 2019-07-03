const resolveKey = require('../modules/resolveKey')

describe('resolveKey', () => {
  test('hash only', () => {
    expect(resolveKey(['a', 'b'])).toEqual({
      hash: 'a.b',
      range: 'a.b'
    })
  })

  test('with object', () => {
    expect(resolveKey({ hash: 'foo', range: 'bar' })).toEqual({
      hash: 'foo',
      range: 'bar'
    })
  })

  test('with string', () => {
    expect(resolveKey('a.b.c.d')).toEqual({
      hash: 'a.b',
      range: 'c.d'
    })
  })

  test('with odd length range', () => {
    expect(resolveKey(['a', 'b', 'c'])).toEqual({
      hash: 'a.b',
      range: 'c'
    })
  })

  test('with even length range', () => {
    expect(resolveKey(['a', 'b', 'c', 'd'])).toEqual({
      hash: 'a.b',
      range: 'c.d'
    })
  })

  test('with custom separator', () => {
    expect(resolveKey(['a', 'b', 'c', 'd'], '#')).toEqual({
      hash: 'a#b',
      range: 'c#d'
    })
  })
})
