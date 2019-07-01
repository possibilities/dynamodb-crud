const dynamodb = require('aws-dynamodb-axios')

const db = dynamodb({
  region: process.env.dynamoDbRegion,
  host: process.env.dynamoDbHost
})

const testTableName = process.env.dynamoDbTableName

module.exports = async () => {
  const existingTable = await db.describeTable({
    TableName: testTableName
  }).catch(() => null)
  if (!existingTable) {
    console.info(`\nCreating \`${testTableName}\` table...`)
    await db.createTable({
      TableName: testTableName,
      BillingMode: 'PAY_PER_REQUEST',
      AttributeDefinitions: [
        { AttributeName: 'hash', AttributeType: 'S' },
        { AttributeName: 'range', AttributeType: 'S' }
      ],
      KeySchema: [
        { AttributeName: 'hash', KeyType: 'HASH' },
        { AttributeName: 'range', KeyType: 'RANGE' }
      ]
    })
    while (true) {
      const description = await db.describeTable({ TableName: testTableName })
      if (description.Table.TableStatus === 'ACTIVE') {
        break
      }
    }
  }
}
