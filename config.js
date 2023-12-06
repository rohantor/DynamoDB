const AWS = require('aws-sdk');
require("dotenv").config();

AWS.config.update({
    region: process.env.REGION
});
const dynamodb = new AWS.DynamoDB();
const dynamoDbDocumentClient = new AWS.DynamoDB.DocumentClient();
const TableName = 'Activity_Log_Poc'
module.exports = { dynamodb ,dynamoDbDocumentClient,TableName};
