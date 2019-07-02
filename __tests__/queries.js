const queries = require('../queries')

const testContext = {
  hashKeyName: 'hash',
  rangeKeyName: 'range',
  separator: '.'
}

describe('queries', () => {
  describe('operations', () => {
    describe('get', () => {
      test('basic', () => {
        const query = queries()
        expect(query.get(['a', 'b'])).toEqual({
          context: testContext,
          action: 'get',
          request: {
            Key: {
              hash: 'a.b',
              range: 'a.b'
            }
          }
        })
      })

      test('with `keys`', () => {
        const query = queries()
        expect(query.get(['a', 'b'], { keys: ['foo', 'bar'] })).toEqual({
          context: testContext,
          action: 'get',
          request: {
            Key: {
              hash: 'a.b',
              range: 'a.b'
            },
            ProjectionExpression: 'foo, bar'
          }
        })
      })
    })

    describe('create', () => {
      test('basic', () => {
        const query = queries()

        expect(query.create(['a', 'b', 'c', 'd'], { foo: 'bar' })).toEqual({
          body: { foo: 'bar' },
          context: testContext,
          action: 'put',
          request: {
            Item: {
              hash: 'a.b',
              range: 'c.d',
              foo: 'bar'
            },
            ConditionExpression: '#hash <> :hash AND #range <> :range',
            ExpressionAttributeNames: {
              '#hash': 'hash',
              '#range': 'range'
            },
            ExpressionAttributeValues: {
              ':hash': 'a.b',
              ':range': 'c.d'
            }
          }
        })
      })
    })

    describe('update', () => {
      test('basic', () => {
        const query = queries()
        const updateQuery = query.update(['a', 'b'], { foo: 'bar' })

        expect(updateQuery).toEqual({
          body: { foo: 'bar' },
          context: testContext,
          action: 'update',
          request: {
            Key: {
              hash: 'a.b',
              range: 'a.b'
            },
            UpdateExpression: 'set #foo = :foo',
            ConditionExpression: '#hash = :hash AND #range = :range',
            ExpressionAttributeNames: {
              '#hash': 'hash',
              '#range': 'range',
              '#foo': 'foo'
            },
            ExpressionAttributeValues: {
              ':foo': 'bar',
              ':hash': 'a.b',
              ':range': 'a.b'
            }
          }
        })
      })
    })

    describe('remove', () => {
      test('basic', () => {
        const query = queries()
        const removeQuery = query.remove(['a', 'b'])

        expect(removeQuery).toEqual({
          action: 'delete',
          context: testContext,
          request: {
            Key: {
              hash: 'a.b',
              range: 'a.b'
            },
            ConditionExpression: '#hash = :hash AND #range = :range',
            ExpressionAttributeNames: {
              '#hash': 'hash',
              '#range': 'range'
            },
            ExpressionAttributeValues: {
              ':hash': 'a.b',
              ':range': 'a.b'
            }
          }
        })
      })

      test('existing entity', () => {
        const query = queries()
        const removeQuery = query.remove({
          hash: 'a.b',
          range: 'a.b'
        })

        expect(removeQuery).toEqual({
          context: testContext,
          action: 'delete',
          request: {
            Key: {
              hash: 'a.b',
              range: 'a.b'
            },
            ConditionExpression: '#hash = :hash AND #range = :range',
            ExpressionAttributeNames: {
              '#hash': 'hash',
              '#range': 'range'
            },
            ExpressionAttributeValues: {
              ':hash': 'a.b',
              ':range': 'a.b'
            }
          }
        })
      })
    })

    describe('list', () => {
      test('basic', () => {
        const query = queries()
        const listQuery = query.list(['a', 'b'])

        expect(listQuery).toEqual({
          context: testContext,
          action: 'query',
          request: {
            KeyConditionExpression: '#hash = :hash AND begins_with(#range, :range)',
            ExpressionAttributeNames: {
              '#hash': 'hash',
              '#range': 'range'
            },
            ExpressionAttributeValues: {
              ':hash': 'a.b',
              ':range': 'a.b'
            }
          }
        })
      })
    })

    describe('count', () => {
      test('basic', () => {
        const query = queries()
        const countQuery = query.count(['a', 'b'])

        expect(countQuery).toEqual({
          context: testContext,
          action: 'query',
          request: {
            Select: 'COUNT',
            KeyConditionExpression: '#hash = :hash AND begins_with(#range, :range)',
            ExpressionAttributeNames: {
              '#hash': 'hash',
              '#range': 'range'
            },
            ExpressionAttributeValues: {
              ':hash': 'a.b',
              ':range': 'a.b'
            }
          }
        })
      })
    })
  })
})
