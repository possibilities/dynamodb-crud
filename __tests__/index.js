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
      describe('create', () => {
        test('basic', async () => {
          const query = queries()
          const created = await db.invoke(query.create(['a', 'b'], { foo: 123 }))

          // Check that create returns new item
          expect(created).toEqual({ foo: 123 })

          // Check that new item is persisted
          const fetched = await db.invoke(query.get(['a', 'b']))
          expect(fetched).toEqual({ foo: 123 })
        })
      })

      describe('get', () => {
        test('basic', async () => {
          const query = queries()
          await db.invoke(query.create(['a', 'b'], { foo: 123 }))
          expect(await db.invoke(query.get(['a', 'b']))).toEqual({ foo: 123 })
        })

        test('non-existent item', async () => {
          const query = queries()
          await db.invoke(query.create(['a', 'b'], { foo: 123 }))
          expect(await db.invoke(query.get(['a', 'b']))).not.toBeNull()
          expect(await db.invoke(query.get(['a', 'x']))).toBeNull()
        })
      })

      describe('update', () => {
        test('basic', async () => {
          const query = queries()
          await db.invoke(query.create(['a', 'b'], { foo: 123 }))
          expect(await db.invoke(query.get(['a', 'b']))).toEqual({ foo: 123 })

          // Time passes
          await new Promise(resolve => setTimeout(resolve, 10))
          const query2 = queries()

          // Check that the update returns the new object
          const createQuery = query2.update(['a', 'b'], { foo: 124 })
          const created = await db.invoke(createQuery)
          expect(created).toEqual({ foo: 124 })

          // Check that the new object is persisted
          const fetched = await db.invoke(query2.get(['a', 'b']))
          expect(fetched).toEqual({ foo: 124 })
        })

        test('non-existent item', async () => {
          const query = queries()
          await db.invoke(query.create(['a', 'b'], { foo: 123 }))
          expect(await db.invoke(query.update(['a', 'b'], { foo: 124 }))).not.toBeNull()
          expect(await db.invoke(query.update(['a', 'x'], { foo: 125 }))).toBeNull()
        })
      })

      describe('delete', () => {
        test('basic', async () => {
          const query = queries()
          await db.invoke(query.create(['a', 'b'], { foo: 123 }))
          expect(await db.invoke(query.get(['a', 'b']))).not.toBeNull()
          await db.invoke(query.remove(['a', 'b']))
          expect(await db.invoke(query.get(['a', 'b']))).toBeNull()
        })

        test('non-existent item', async () => {
          const query = queries()
          await db.invoke(query.create(['a', 'b'], { foo: 123 }))
          expect(await db.invoke(query.get(['a', 'b']))).not.toBeNull()
          expect(await db.invoke(query.remove(['a', 'b']))).toEqual({})
          expect(await db.invoke(query.get(['a', 'b']))).toBeNull()
          expect(await db.invoke(query.remove(['a', 'b']))).toBeNull()
        })
      })

      describe('list', () => {
        test('basic', async () => {
          const query = queries()

          await db.invoke(query.create(['a', 'b', 'c', 'd'], { foo: 123 }))
          await db.invoke(query.create(['a', 'b', 'c', 'e'], { foo: 124 }))
          await db.invoke(query.create(['a', 'b', 'd', 'f'], { foo: 125 }))

          expect(await db.invoke(query.list(['a', 'b', 'c']))).toEqual([
            { foo: 123 },
            { foo: 124 },
          ])

          expect(await db.invoke(query.list(['a', 'b', 'd']))).toEqual([
            { foo: 125 }
          ])
        })
      })

      describe('count', () => {
        test('basic', async () => {
          const query = queries()

          await db.invoke(query.create(['a', 'b', 'c', 'd'], { foo: 123 }))
          await db.invoke(query.create(['a', 'b', 'c', 'e'], { foo: 124 }))
          await db.invoke(query.create(['a', 'b', 'd', 'f'], { foo: 125 }))

          expect(await db.invoke(query.count(['a', 'b', 'c']))).toEqual(2)
          expect(await db.invoke(query.count(['a', 'b', 'd']))).toEqual(1)
        })
      })
    })

    test('with multiple queries', async () => {
      const query = queries()

      await db.invoke(query.create(['a', 'b'], { foo: 123 }))
      await db.invoke(query.create(['a', 'c'], { foo: 124 }))

      const [one, two] = await db.invoke([
        query.get(['a', 'b']),
        query.get(['a', 'c'])
      ])

      expect(one.foo).toEqual(123)
      expect(two.foo).toEqual(124)
    })
  })

  describe('batchWrite', () => {
    test('basic', async () => {
      const query = queries()
      await db.batchWrite([
        query.create(['a', 'b'], { foo: 123 }),
        query.create(['a', 'c'], { foo: 124 })
      ])

      expect(await db.invoke(query.get(['a', 'b']))).not.toBeNull()
      expect(await db.invoke(query.get(['a', 'c']))).not.toBeNull()
      // Check false positive
      expect(await db.invoke(query.get(['a', 'x']))).toBeNull()
    })
  })

  describe('transactWrite', () => {
    test('basic', async () => {
      const query = queries()

      await db.transactWrite([
        query.create(['a', 'b'], { foo: 123 }),
        query.create(['a', 'c'], { foo: 124 })
      ])

      expect(await db.invoke(query.get(['a', 'b']))).not.toBeNull()
      expect(await db.invoke(query.get(['a', 'c']))).not.toBeNull()
      // Check false positive
      expect(await db.invoke(query.get(['a', 'x']))).toBeNull()
    })

    test('single query', async () => {
      const query = queries()

      await db.transactWrite(query.create(['a', 'b'], { foo: 123 }))

      expect(await db.invoke(query.get(['a', 'b']))).not.toBeNull()
      // Check false positive
      expect(await db.invoke(query.get(['a', 'x']))).toBeNull()
    })
  })
})
