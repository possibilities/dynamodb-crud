const dynamodb = require('../index')
const queries = require('../queries')
const clearDatabase = require('../modules/testing/clearDatabase')

describe('dynamodb', () => {
  let db
  beforeEach(async () => {
    db = dynamodb({
      region: process.env.dynamoDbRegion,
      host: process.env.dynamoDbHost
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
          expect(created).toEqual(
            expect.objectContaining({
              foo: 123,
              hash: 'a.b',
              range: 'a.b'
            })
          )

          // Check timestamps created
          expect(created.createdAt).toBeTruthy()
          expect(created.updatedAt).toBeTruthy()

          // Check that new item is persisted
          const fetched = await db.invoke(query.get(['a', 'b']))
          expect(fetched).toEqual(
            expect.objectContaining({
              foo: 123,
              hash: 'a.b',
              range: 'a.b'
            })
          )

          // Check timestamps created
          expect(fetched.createdAt).toBeTruthy()
          expect(fetched.updatedAt).toBeTruthy()
        })
      })

      describe('get', () => {
        test('basic', async () => {
          const query = queries()

          await db.invoke(query.create(['a', 'b'], { foo: 123 }))

          expect(await db.invoke(query.get(['a', 'b']))).toEqual(
            expect.objectContaining({
              foo: 123,
              hash: 'a.b',
              range: 'a.b'
            })
          )
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

          expect(await db.invoke(query.get(['a', 'b']))).toEqual(
            expect.objectContaining({
              foo: 123,
              hash: 'a.b',
              range: 'a.b'
            })
          )

          // Time passes
          await new Promise(resolve => setTimeout(resolve, 10))
          const queryAfterTimepasses = queries()

          // Check that the update returns the new object
          const created = await db.invoke(queryAfterTimepasses.update(['a', 'b'], { foo: 124 }))
          expect(created).toEqual(
            expect.objectContaining({
              foo: 124,
              hash: 'a.b',
              range: 'a.b'
            })
          )

          // Check timestamps created
          expect(created.createdAt).toBeTruthy()
          expect(created.updatedAt).toBeTruthy()

          // Check that the new object is persisted
          const fetched = await db.invoke(queryAfterTimepasses.get(['a', 'b']))
          expect(fetched).toEqual(
            expect.objectContaining({
              foo: 124,
              hash: 'a.b',
              range: 'a.b'
            })
          )

          // Check timestamps created
          expect(fetched.createdAt).toBeTruthy()
          expect(fetched.updatedAt).toBeTruthy()
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

          expect(await db.invoke(query.list(['a', 'b', 'c']))).toHaveLength(2)
          expect(await db.invoke(query.list(['a', 'b', 'd']))).toHaveLength(1)
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

  describe('batch', () => {
    test('basic', async () => {
      const query = queries()
      await db.batch([
        query.create(['a', 'b'], { foo: 123 }),
        query.create(['a', 'c'], { foo: 124 })
      ])

      expect(await db.invoke(query.get(['a', 'b']))).not.toBeNull()
      expect(await db.invoke(query.get(['a', 'c']))).not.toBeNull()
      // Check false positive
      expect(await db.invoke(query.get(['a', 'x']))).toBeNull()
    })
  })

  describe('transact', () => {
    test('basic', async () => {
      const query = queries()

      await db.transact([
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

      await db.transact(query.create(['a', 'b'], { foo: 123 }))

      expect(await db.invoke(query.get(['a', 'b']))).not.toBeNull()
      // Check false positive
      expect(await db.invoke(query.get(['a', 'x']))).toBeNull()
    })
  })
})
