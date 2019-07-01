const queries = require('../queries')
const lolex = require('lolex')

describe('queries', () => {
  describe('operations', () => {
    let clock

    beforeEach(() => {
      clock = lolex.install()
    })

    afterEach(() => {
      clock = clock.uninstall()
    })

    describe('get', () => {
      test('basic', () => {
        const query = queries()
        expect(query.get(['a', 'b', 'c', 'd'])).toEqual({
          action: 'get',
          request: {
            Key: {
              hash: { S: 'a.b' },
              range: { S: 'a.b.c.d' }
            }
          }
        })
      })

      test('child entity', () => {
        const query = queries()
        expect(query.get(['a', 'b', 'c'])).toEqual({
          action: 'get',
          request: {
            Key: {
              hash: { S: 'a.b' },
              range: { S: 'a.b.c' }
            }
          }
        })
      })

      test('with `keys`', () => {
        const query = queries()
        expect(query.get(['a', 'b', 'c'], { keys: ['foo', 'bar'] })).toEqual({
          action: 'get',
          request: {
            Key: {
              hash: { S: 'a.b' },
              range: { S: 'a.b.c' }
            },
            ProjectionExpression: 'foo, bar'
          }
        })
      })
    })

    describe('create', () => {
      test('basic', () => {
        const query = queries()
        const stamp = new Date().toISOString()

        expect(query.create(['a', 'b', 'c', 'd'], { foo: 'bar' })).toEqual({
          action: 'put',
          request: {
            Item: {
              hash: { S: 'a.b' },
              range: { S: 'a.b.c.d' },
              foo: { S: 'bar' },
              createdAt: { S: stamp },
              updatedAt: { S: stamp }
            },
            ConditionExpression: '#hash <> :hash AND #range <> :range',
            ExpressionAttributeNames: {
              '#hash': 'hash',
              '#range': 'range'
            },
            ExpressionAttributeValues: {
              ':hash': { S: 'a.b' },
              ':range': { S: 'a.b.c.d' }
            }
          }
        })
      })

      test('timestamps', () => {
        // Here we're just showing that from each query builder a timestamp
        // will be generated
        const query = queries()
        const stamp = new Date().toISOString()

        // Returns initial stamp
        const query1 = query.create(['a'], { foo: 'bar' })
        expect(query1.request.Item.createdAt.S).toEqual(stamp)
        expect(query1.request.Item.updatedAt.S).toEqual(stamp)

        clock.tick(10)

        // Still returns initial stamp after time passed
        const query2 = query.create(['a'], { foo: 'bar' })
        expect(query2.request.Item.createdAt.S).toEqual(stamp)
        expect(query2.request.Item.updatedAt.S).toEqual(stamp)

        clock.tick(10)

        // Retuns new stamp given new query builder
        const queries2 = queries()
        const stamp2 = new Date().toISOString()

        const query3 = queries2.create(['a'], { foo: 'bar' })
        expect(query3.request.Item.createdAt.S).toEqual(stamp2)
        expect(query3.request.Item.updatedAt.S).toEqual(stamp2)
      })
    })

    describe('update', () => {
      test('basic', () => {
        const stamp = new Date().toISOString()
        const query = queries()
        const updateQuery = query.update(['a', 'b'], { foo: 'bar' })

        expect(updateQuery).toEqual({
          action: 'update',
          request: {
            Key: {
              hash: { S: 'a.b' },
              range: { S: 'a.b' }
            },
            UpdateExpression: 'set #foo = :foo, #updatedAt = :updatedAt',
            ConditionExpression: '#hash = :hash AND #range = :range',
            ExpressionAttributeNames: {
              '#hash': 'hash',
              '#range': 'range',
              '#foo': 'foo',
              '#updatedAt': 'updatedAt'
            },
            ExpressionAttributeValues: {
              ':foo': { S: 'bar' },
              ':hash': { S: 'a.b' },
              ':range': { S: 'a.b' },
              ':updatedAt': { S: stamp }
            }
          }
        })
      })

      test('timestamps', () => {
        // Here we're just showing that from each query builder a timestamp
        // will be generated
        const query = queries()
        const stamp = new Date().toISOString()

        // Create and update stamps
        const query1 = query.create(['a', 'b'], { foo: 'bar' })
        expect(query1.request.Item.createdAt.S).toEqual(stamp)
        expect(query1.request.Item.updatedAt.S).toEqual(stamp)

        clock.tick(10)

        // Retuns new stamp given new query builder
        const queries2 = queries()
        const stamp2 = new Date().toISOString()

        // Update after time passed reflects in update stamp
        const query2 = queries2.update(['a', 'b'], { foo: 'bar' })
        expect(query2.request.ExpressionAttributeValues[':updatedAt'].S).not.toEqual(stamp)
        expect(query2.request.ExpressionAttributeValues[':updatedAt'].S).toEqual(stamp2)
      })
    })

    describe('remove', () => {
      test('basic', () => {
        const query = queries()
        const removeQuery = query.remove(['a', 'b'])

        expect(removeQuery).toEqual({
          action: 'delete',
          request: {
            Key: {
              hash: { S: 'a.b' },
              range: { S: 'a.b' }
            },
            ConditionExpression: '#hash = :hash AND #range = :range',
            ExpressionAttributeNames: {
              '#hash': 'hash',
              '#range': 'range'
            },
            ExpressionAttributeValues: {
              ':hash': { S: 'a.b' },
              ':range': { S: 'a.b' }
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
          action: 'delete',
          request: {
            Key: {
              hash: { S: 'a.b' },
              range: { S: 'a.b' }
            },
            ConditionExpression: '#hash = :hash AND #range = :range',
            ExpressionAttributeNames: {
              '#hash': 'hash',
              '#range': 'range'
            },
            ExpressionAttributeValues: {
              ':hash': { S: 'a.b' },
              ':range': { S: 'a.b' }
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
          action: 'query',
          request: {
            KeyConditionExpression: '#hash = :hash AND begins_with(#range, :range)',
            ExpressionAttributeNames: {
              '#hash': 'hash',
              '#range': 'range'
            },
            ExpressionAttributeValues: {
              ':hash': { S: 'a.b' },
              ':range': { S: 'a.b' }
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
          action: 'query',
          request: {
            Select: 'COUNT',
            KeyConditionExpression: '#hash = :hash AND begins_with(#range, :range)',
            ExpressionAttributeNames: {
              '#hash': 'hash',
              '#range': 'range'
            },
            ExpressionAttributeValues: {
              ':hash': { S: 'a.b' },
              ':range': { S: 'a.b' }
            }
          }
        })
      })
    })
  })
})
