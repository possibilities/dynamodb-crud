const dynamodb = require('../index')
const queries = require('../queries')
const clearDatabase = require('../modules/testing/clearDatabase')

describe('dynamodb', () => {
  let db
  beforeEach(async () => {
    db = dynamodb({
      region: process.env.dynamoDbRegion,
      host: process.env.dynamoDbHost,
      tableName: process.env.dynamoDbTableName
    })

    await clearDatabase(process.env.dynamoDbTableName)
  })

  describe('invoke', () => {
    describe('operations', () => {
      describe('post', () => {
        test('basic', async () => {
          const query = queries()
          const posted = await db.invoke(query.post(['a', 'b'], { foo: 123 }))

          // Check that post returns new item
          expect(posted).toEqual({})

          // Check that new item is persisted
          const fetched = await db.invoke(query.get(['a', 'b']))
          expect(fetched).toEqual({ foo: 123, hash: 'a.b', range: 'a.b' })
        })
      })

      describe('get', () => {
        test('basic', async () => {
          const query = queries()
          await db.invoke(query.post(['a', 'b'], { foo: 123 }))
          expect(await db.invoke(query.get(['a', 'b'])))
            .toEqual({ foo: 123, hash: 'a.b', range: 'a.b' })
        })

        test('non-existent item', async () => {
          const query = queries()
          await db.invoke(query.post(['a', 'b'], { foo: 123 }))
          expect(await db.invoke(query.get(['a', 'b']))).not.toBeNull()
          expect(await db.invoke(query.get(['a', 'x']))).toBeNull()
        })
      })

      describe('patch', () => {
        test('basic', async () => {
          const query = queries()
          await db.invoke(query.post(['a', 'b'], { foo: 123 }))
          expect(await db.invoke(query.get(['a', 'b'])))
            .toEqual({ foo: 123, hash: 'a.b', range: 'a.b' })

          // Time passes
          await new Promise(resolve => setTimeout(resolve, 10))
          const query2 = queries()

          // Check that the patch returns the new object
          const postQuery = query2.patch(['a', 'b'], { foo: 124 })
          const posted = await db.invoke(postQuery)
          expect(posted).toEqual({ foo: 124, hash: 'a.b', range: 'a.b' })

          // Check that the new object is persisted
          const fetched = await db.invoke(query2.get(['a', 'b']))
          expect(fetched).toEqual({ foo: 124, hash: 'a.b', range: 'a.b' })
        })

        test('non-existent item', async () => {
          const query = queries()
          await db.invoke(query.post(['a', 'b'], { foo: 123 }))
          expect(await db.invoke(query.patch(['a', 'b'], { foo: 124 }))).not.toBeNull()
          expect(await db.invoke(query.patch(['a', 'x'], { foo: 125 }))).toBeNull()
        })
      })

      describe('delete', () => {
        test('basic', async () => {
          const query = queries()
          await db.invoke(query.post(['a', 'b'], { foo: 123 }))
          expect(await db.invoke(query.get(['a', 'b']))).not.toBeNull()
          await db.invoke(query.delete(['a', 'b']))
          expect(await db.invoke(query.get(['a', 'b']))).toBeNull()
        })

        test('non-existent item', async () => {
          const query = queries()
          await db.invoke(query.post(['a', 'b'], { foo: 123 }))
          expect(await db.invoke(query.get(['a', 'b']))).not.toBeNull()
          expect(await db.invoke(query.delete(['a', 'b']))).toEqual({})
          expect(await db.invoke(query.get(['a', 'b']))).toBeNull()
          expect(await db.invoke(query.delete(['a', 'b']))).toBeNull()
        })
      })

      describe('list', () => {
        test('basic', async () => {
          const query = queries()

          await db.invoke(query.post(['a', 'b'], ['c', 'd'], { foo: 123 }))
          await db.invoke(query.post(['a', 'b'], ['c', 'e'], { foo: 124 }))
          await db.invoke(query.post(['a', 'b'], ['d', 'f'], { foo: 125 }))

          expect(await db.invoke(query.list(['a', 'b'], ['c']))).toEqual({
            items: [
              { foo: 123, hash: 'a.b', range: 'c.d' },
              { foo: 124, hash: 'a.b', range: 'c.e' }
            ],
            lastKey: null
          })

          expect(await db.invoke(query.list(['a', 'b'], ['d']))).toEqual({
            items: [
              { foo: 125, hash: 'a.b', range: 'd.f' }
            ],
            lastKey: null
          })
        })

        test('with lastKey', async () => {
          const query = queries()

          await db.invoke(query.post(['a', 'b'], ['c', 'd'], { foo: 123 }))
          await db.invoke(query.post(['a', 'b'], ['c', 'e'], { foo: 124 }))
          await db.invoke(query.post(['a', 'b'], ['d', 'f'], { foo: 125 }))

          expect(await db.invoke(query.list(['a', 'b'], ['c'], { Limit: 1 }))).toEqual({
            items: [
              { foo: 123, hash: 'a.b', range: 'c.d' }
            ],
            lastKey: { hash: 'a.b', range: 'c.d' }
          })

          expect(await db.invoke(query.list(['a', 'b'], ['d']))).toEqual({
            items: [
              { foo: 125, hash: 'a.b', range: 'd.f' }
            ],
            lastKey: null
          })
        })

        test('with startKey', async () => {
          const query = queries()

          await db.invoke(query.post(['a', 'b'], ['c', 'd'], { foo: 123 }))
          await db.invoke(query.post(['a', 'b'], ['c', 'e'], { foo: 124 }))
          await db.invoke(query.post(['a', 'b'], ['d', 'f'], { foo: 125 }))

          expect(await db.invoke(query.list(['a', 'b'], ['c'], { Limit: 1 }))).toEqual({
            items: [
              { foo: 123, hash: 'a.b', range: 'c.d' }
            ],
            lastKey: { hash: 'a.b', range: 'c.d' }
          })

          expect(await db.invoke(query.list(['a', 'b'], ['d']))).toEqual({
            items: [
              { foo: 125, hash: 'a.b', range: 'd.f' }
            ],
            lastKey: null
          })
        })
      })

      describe('count', () => {
        test('basic', async () => {
          const query = queries()

          await db.invoke(query.post(['a', 'b'], ['c', 'd'], { foo: 123 }))
          await db.invoke(query.post(['a', 'b'], ['c', 'e'], { foo: 124 }))
          await db.invoke(query.post(['a', 'b'], ['d', 'f'], { foo: 125 }))

          expect(await db.invoke(query.count(['a', 'b'], ['c']))).toEqual(2)
          expect(await db.invoke(query.count(['a', 'b'], ['d']))).toEqual(1)
        })
      })
    })

    test('with multiple queries', async () => {
      const query = queries()

      await db.invoke(query.post(['a', 'b'], { foo: 123 }))
      await db.invoke(query.post(['a', 'c'], { foo: 124 }))

      const [one, two] = await db.invoke([
        query.get(['a', 'b']),
        query.get(['a', 'c'])
      ])

      expect(one.foo).toEqual(123)
      expect(two.foo).toEqual(124)
    })
  })

  describe('batchGet', () => {
    test('basic', async () => {
      const query = queries()

      await db.batchWrite([
        query.post(['a', 'b'], { foo: 123 }),
        query.post(['a', 'c'], { foo: 124 })
      ])

      const items = await db.batchGet([
        query.get(['a', 'b']),
        query.get(['a', 'c'])
      ])

      // Order not guaranteed
      expect(items.map(item => item.foo).sort()).toEqual([123, 124])
    })
  })

  describe('batchWrite', () => {
    test('basic', async () => {
      const query = queries()
      await db.batchWrite([
        query.post(['a', 'b'], { foo: 123 }),
        query.post(['a', 'c'], { foo: 124 })
      ])

      expect(await db.invoke(query.get(['a', 'b']))).not.toBeNull()
      expect(await db.invoke(query.get(['a', 'c']))).not.toBeNull()
      // Check false positive
      expect(await db.invoke(query.get(['a', 'x']))).toBeNull()
    })
  })

  describe('transactGet', () => {
    test('basic', async () => {
      const query = queries()

      await db.transactWrite([
        query.post(['a', 'b'], { foo: 123 }),
        query.post(['a', 'c'], { foo: 124 })
      ])

      const items = await db.transactGet([
        query.get(['a', 'b']),
        query.get(['a', 'c'])
      ])

      expect(items).toEqual([
        { foo: 123, hash: 'a.b', range: 'a.b' },
        { foo: 124, hash: 'a.c', range: 'a.c' }
      ])
    })

    test('single query', async () => {
      const query = queries()

      await db.transactWrite([
        query.post(['a', 'b'], { foo: 123 }),
        query.post(['a', 'c'], { foo: 124 })
      ])

      const item = await db.transactGet(query.get(['a', 'b']))

      expect(item).toEqual({ foo: 123, hash: 'a.b', range: 'a.b' })
    })
  })

  describe('transactWrite', () => {
    test('basic', async () => {
      const query = queries()

      const items = await db.transactWrite([
        query.post(['a', 'b'], { foo: 123 }),
        query.post(['a', 'c'], { foo: 124 })
      ])

      expect(items).toEqual([
        { foo: 123 },
        { foo: 124 }
      ])

      expect(await db.invoke(query.get(['a', 'b']))).not.toBeNull()
      expect(await db.invoke(query.get(['a', 'c']))).not.toBeNull()
      // Check false positive
      expect(await db.invoke(query.get(['a', 'x']))).toBeNull()
    })

    test('patch', async () => {
      const query = queries()

      const itemsPosted = await db.transactWrite([
        query.post(['a', 'b'], { foo: 123 }),
        query.post(['a', 'c'], { foo: 124 })
      ])

      expect(itemsPosted).toEqual([
        { foo: 123 },
        { foo: 124 }
      ])

      const itemsFetched = await db.transactWrite([
        query.patch(['a', 'b'], { foo: 125 }),
        query.patch(['a', 'c'], { foo: 126 })
      ])

      expect(itemsFetched).toEqual([
        { foo: 125 },
        { foo: 126 }
      ])
    })

    test('delete', async () => {
      const query = queries()

      const itemsPosted = await db.transactWrite([
        query.post(['a', 'b'], { foo: 123 }),
        query.post(['a', 'c'], { foo: 124 }),
        query.post(['a', 'd'], { foo: 125 })
      ])

      expect(itemsPosted).toEqual([
        { foo: 123 },
        { foo: 124 },
        { foo: 125 }
      ])

      const itemsdeleteed = await db.transactWrite([
        query.delete(['a', 'b'], { foo: 123 }),
        query.delete(['a', 'c'], { foo: 124 })
      ])

      expect(itemsdeleteed).toEqual({})

      expect(await db.transactWrite(query.delete(['a', 'd']))).not.toBeNull()
      // Check false positive
      expect(await db.transactWrite(query.delete(['a', 'x']))).toBeNull()
    })

    test('single query', async () => {
      const query = queries()

      const item =
        await db.transactWrite(query.post(['a', 'b'], { foo: 123 }))
      expect(item).toEqual({ foo: 123 })

      expect(await db.invoke(query.get(['a', 'b']))).not.toBeNull()
      // Check false positive
      expect(await db.invoke(query.get(['a', 'x']))).toBeNull()
    })
  })

  // TODO deeper + better tests
  describe('interceptors', () => {
    test('request', async () => {
      let requests = []

      const query = queries()
      await db.invoke(query.post(['a', 'a'], { foo: 123 }))

      db.interceptors.request.use(query => {
        requests.push(query)
        return query
      })

      let responses = []
      db.interceptors.response.use(response => {
        responses.push(query)
        return response
      })

      await db.invoke(query.post(['a', 'b'], { foo: 123 }))
      await db.invoke(query.post(['a', 'c'], { foo: 123 }))
      await db.invoke(query.post(['a', 'd'], { foo: 123 }))

      expect(requests).toHaveLength(3)
      expect(responses).toHaveLength(3)

      await db.transactWrite(query.post(['a', 'e'], { foo: 123 }))

      expect(requests).toHaveLength(4)
      expect(responses).toHaveLength(4)

      await db.transactWrite([
        query.post(['a', 'f'], { foo: 123 }),
        query.post(['a', 'g'], { foo: 123 })
      ])

      expect(requests).toHaveLength(6)
      expect(responses).toHaveLength(6)

      await db.batchWrite([
        query.post(['a', 'f'], { foo: 123 }),
        query.post(['a', 'g'], { foo: 123 })
      ])

      expect(requests).toHaveLength(8)
      expect(responses).toHaveLength(8)
    })
  })
})
