const buildKey = require('../modules/buildKey')

describe('buildKey', () => {
  test('hash only', () => {
    expect(buildKey(['a', 'b'], '.')).toEqual({
      hash: 'a.b',
      range: 'a.b'
    })
  })

  test('with range', () => {
    expect(buildKey(['a', 'b', 'c'], '.')).toEqual({
      hash: 'a.b',
      range: 'a.b.c'
    })
  })
})
