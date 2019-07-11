const resolveKey = require('../resolveKey')

const context = { hashKeyName: 'hash', rangeKeyName: 'range', separator: '.' }

describe('resolveKey', () => {
  describe('object key', () => {
    test('basic', () => {
      expect(resolveKey(
        context,
        { hash: 'foo.bar', range: 'foof.doof' }
      )).toEqual([
        { hash: 'foo.bar', range: 'foof.doof' }
      ])
    })

    test('with options', () => {
      expect(resolveKey(
        context,
        { hash: 'foo.bar', range: 'foof.doof' },
        { optionKey: 'optionValue' }
      )).toEqual([
        { hash: 'foo.bar', range: 'foof.doof' },
        { optionKey: 'optionValue' }
      ])
    })

    test('with body', () => {
      expect(resolveKey(
        context,
        { hash: 'foo.bar', range: 'foof.doof' },
        { bodyKey: 'bodyVal' },
        { optionKey: 'optionValue' }
      )).toEqual([
        { hash: 'foo.bar', range: 'foof.doof' },
        { bodyKey: 'bodyVal' },
        { optionKey: 'optionValue' }
      ])
    })
  })

  describe('single array', () => {
    test('basic', () => {
      expect(resolveKey(
        context,
        ['foo', 'bar']
      )).toEqual([
        { hash: 'foo.bar', range: 'foo.bar' }
      ])
    })

    test('with options', () => {
      expect(resolveKey(
        context,
        ['foo', 'bar'],
        { bodyKey: 'bodyVal' },
        { optionKey: 'optionValue' }
      )).toEqual([
        { hash: 'foo.bar', range: 'foo.bar' },
        { bodyKey: 'bodyVal' },
        { optionKey: 'optionValue' }
      ])
    })

    test('with body', () => {
      expect(resolveKey(
        context,
        ['foo', 'bar'],
        { bodyKey: 'bodyVal' },
        { optionKey: 'optionValue' }
      )).toEqual([
        { hash: 'foo.bar', range: 'foo.bar' },
        { bodyKey: 'bodyVal' },
        { optionKey: 'optionValue' }
      ])
    })

    test('with function values', () => {
      const getFoo = () => 'foo'
      const getBar = () => 'bar'
      expect(resolveKey(
        context,
        [getFoo, getBar]
      )).toEqual([
        { hash: 'foo.bar', range: 'foo.bar' }
      ])
    })
  })

  describe('multiple arrays', () => {
    test('basic', () => {
      expect(resolveKey(
        context,
        ['foo', 'bar'],
        ['foof', 'doof']
      )).toEqual([
        { hash: 'foo.bar', range: 'foof.doof' }
      ])
    })

    test('with options', () => {
      expect(resolveKey(
        context,
        ['foo', 'bar'],
        ['foof', 'doof'],
        { optionKey: 'optionValue' }
      )).toEqual([
        { hash: 'foo.bar', range: 'foof.doof' },
        { optionKey: 'optionValue' }
      ])
    })

    test('with body', () => {
      expect(resolveKey(
        context,
        ['foo', 'bar'],
        ['foof', 'doof'],
        { bodyKey: 'bodyVal' },
        { optionKey: 'optionValue' }
      )).toEqual([
        { hash: 'foo.bar', range: 'foof.doof' },
        { bodyKey: 'bodyVal' },
        { optionKey: 'optionValue' }
      ])
    })

    test('with function values', () => {
      const getFoo = () => 'foo'
      const getBar = () => 'bar'
      const getFoof = () => 'foof'
      const getDoof = () => 'doof'
      expect(resolveKey(
        context,
        [getFoo, getBar],
        [getFoof, getDoof]
      )).toEqual([
        { hash: 'foo.bar', range: 'foof.doof' }
      ])
    })
  })

  describe('mapped arrays', () => {
    test('basic', () => {
      expect(resolveKey(
        context,
        { hash: ['foo', 'bar'], range: ['foof', 'doof'] }
      )).toEqual([{
        hash: 'foo.bar',
        range: 'foof.doof'
      }])
    })

    test('with options', () => {
      expect(resolveKey(
        context,
        { hash: ['foo', 'bar'], range: ['foof', 'doof'] },
        { optionKey: 'optionValue' }
      )).toEqual([
        { hash: 'foo.bar', range: 'foof.doof' },
        { optionKey: 'optionValue' }
      ])
    })

    test('with body', () => {
      expect(resolveKey(
        context,
        { hash: ['foo', 'bar'], range: ['foof', 'doof'] },
        { bodyKey: 'bodyVal' },
        { optionKey: 'optionValue' }
      )).toEqual([
        { hash: 'foo.bar', range: 'foof.doof' },
        { bodyKey: 'bodyVal' },
        { optionKey: 'optionValue' }
      ])
    })

    test('with function values', () => {
      const getFoo = () => 'foo'
      const getBar = () => 'bar'
      const getFoof = () => 'foof'
      const getDoof = () => 'doof'
      expect(resolveKey(
        context,
        { hash: [getFoo, getBar], range: [getFoof, getDoof] }
      )).toEqual([
        { hash: 'foo.bar', range: 'foof.doof' }
      ])
    })
  })

  describe('with custom separator', () => {
    test('basic', () => {
      expect(resolveKey(
        { ...context, separator: '#' },
        ['foo', 'bar'],
        ['foof', 'doof']
      )).toEqual([
        { hash: 'foo#bar', range: 'foof#doof' }
      ])
    })
  })
})
