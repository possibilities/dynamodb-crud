const resolveKey = require('../modules/resolveKey')

const context = { hashKeyName: 'hash', rangeKeyName: 'range', separator: '.' }

describe('resolveKey', () => {
  test('hash only', () => {
    expect(resolveKey(['a', 'b'], context)).toEqual({
      hash: 'a.b',
      range: 'a.b'
    })
  })

  test('with object', () => {
    expect(resolveKey({ hash: 'foo', range: 'bar' }, context)).toEqual({
      hash: 'foo',
      range: 'bar'
    })
  })

  test('with string', () => {
    expect(resolveKey('a.b.c.d', context)).toEqual({
      hash: 'a.b',
      range: 'c.d'
    })
  })

  test('with odd length range', () => {
    expect(resolveKey(['a', 'b', 'c'], context)).toEqual({
      hash: 'a.b',
      range: 'c'
    })
  })

  test('with even length range', () => {
    expect(resolveKey(['a', 'b', 'c', 'd'], context)).toEqual({
      hash: 'a.b',
      range: 'c.d'
    })
  })

  test('with custom separator', () => {
    expect(resolveKey(['a', 'b', 'c', 'd'], { ...context, separator: '#' })).toEqual({
      hash: 'a#b',
      range: 'c#d'
    })
  })
})
