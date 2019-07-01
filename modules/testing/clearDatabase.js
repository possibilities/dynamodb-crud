const dynamodb = require('aws-dynamodb-axios')

const pick = (obj, pickKeys) => {
  let picked = {}
  pickKeys.forEach(key => {
    picked[key] = obj[key]
  })
  return picked
}

const db = dynamodb({
  region: process.env.dynamoDbRegion,
  host: process.env.dynamoDbHost
})

const clearDatabase = async tableName => {
  while (true) {
    const response = await db.scan({ Limit: 25, TableName: tableName })
    if (!response.Items.length) break
    await db.batchWrite({
      RequestItems: {
        [tableName]: response.Items.map(item => (
          { DeleteRequest: { Key: pick(item, ['hash', 'range']) } }
        ))
      }
    })
  }
}

module.exports = clearDatabase
