const { TableName,dynamodb, dynamoDbDocumentClient } = require('./config');
const redis =  require('redis');
const { connect } = require('./router');
const redisURL = process.env.REDIS_FULL_URL
const redisClient = redis.createClient();
redisClient.on('error', (err) => console.log('Redis Client Error', err));

const connectRedis = ()=>{
  if (!redisClient.isOpen) {
    redisClient.connect().then(()=>{
      console.log("Connected")
    })
  }
}
const scanTable = async(startDate,endDate,limit = 50) => {
  if (!limit) return []

 
  const params = {
    TableName: TableName,
    FilterExpression:'created_at < :endDate and created_at >= :startDate',
    ExpressionAttributeValues: {
      ':endDate': { S: endDate } ,
      ':startDate': { S: startDate } 
    },
    IndexName: '_id-created_at-index',

  };

  const result = await dynamodb.scan(params).promise()
  return result

};
const getItemByDealIdAndTransactionStep = (id,step) => {
  const params = {
    TableName: TableName, 
    IndexName: 'deal_processing_transaction_id-transaction_step-index', 
    KeyConditionExpression: 'deal_processing_transaction_id = :id and transaction_step = :step',
    Limit: 5,
    
    ExpressionAttributeValues: {
      ':id': id,
      ':step': step 
    },
   
    ScanIndexForward: false
  };

  const data  = dynamoDbDocumentClient.query(params).promise()
  return data;
};
const getItemByDealIdAndCreatorID = (id,cid) => {
  const params = {
    TableName: TableName, 
    IndexName: 'deal_processing_transaction_id-transaction_step-index', 
    KeyConditionExpression: 'deal_processing_transaction_id = :id',
    // Limit: 5,
    QueryPageSize:11,
    FilterExpression: 'created_by_id = :createdById',
    ExpressionAttributeValues: {
      ':id': id, 
      ':createdById': cid 
    },
    ScanIndexForward: true
  };

  const data  = dynamoDbDocumentClient.query(params).promise()
  return data;
};

const getItemByDealProcessingTransactionId = async(id,page) => {
  console.log("Page",page)
  connectRedis()
  let params ={
    TableName: TableName, 
    Limit:50,
    IndexName: 'deal_processing_transaction_id-transaction_step-index', 
    KeyConditionExpression: 'deal_processing_transaction_id = :id',
    ExpressionAttributeValues: {
      ':id': id, 
    },
    
  };
  if(page>0){
    const record = await redisClient.get(`Deal:${id}Page:${page-1}`);
    console.log("Record",record)
    if(record === null){
      const DataLength = page*50;
      let data;
      const result = []
      let ExclusiveStartKey = null
    
      do {
        let params ={
          TableName: TableName, 
          Limit:50,
          IndexName: 'deal_processing_transaction_id-transaction_step-index', 
          KeyConditionExpression: 'deal_processing_transaction_id = :id',
          ExclusiveStartKey:ExclusiveStartKey,
          ExpressionAttributeValues: {
            ':id': id, 
          },
          
        };
        data  = await dynamoDbDocumentClient.query(params).promise()
        if(data.LastEvaluatedKey){
          ExclusiveStartKey = data.LastEvaluatedKey
          await redisClient.set(`Deal:${id}Page:${result.length/50 +1}`, JSON.stringify(ExclusiveStartKey));
          result.push(...data.Items)
         }
      } while (result.length<DataLength && data.LastEvaluatedKey);
      // console.log(result.slice(-50))
      return result.slice(-50)
    
    }
   const ExclusiveStartKeyData=  JSON.parse(record)
    params= {
      TableName: TableName, 
      Limit:50,
      IndexName: 'deal_processing_transaction_id-transaction_step-index', 
      KeyConditionExpression: 'deal_processing_transaction_id = :id',
      ExpressionAttributeValues: {
        ':id': id, 
      },
      ExclusiveStartKey:ExclusiveStartKeyData,
    };
  }
  
  const data  = await dynamoDbDocumentClient.query(params).promise()
 if(data.LastEvaluatedKey){
  await redisClient.set(`Deal:${id}Page:${page}`, JSON.stringify(data.LastEvaluatedKey));
 }

  return data;
};

const insertItemIntoActivityLog = async(item) =>{
  const params = {
    TableName: 'Activity_Log_Poc', // Replace with your DynamoDB table name
    Item: item,
  };
  try {
    const response = await dynamoDbDocumentClient.put(params).promise();
    return response;
  } catch (error) {
    console.log(error)
    return error  
  }
 
}
module.exports = {
  scanTable,
  getItemByDealProcessingTransactionId,
  getItemByDealIdAndTransactionStep,
  getItemByDealIdAndCreatorID,
  insertItemIntoActivityLog
};










