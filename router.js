const express = require("express");
const { scanTable, getItemByDealProcessingTransactionId, insertItemIntoActivityLog } = require("./helper");
const uuid = require('uuid');
const Router = express.Router();
// const startDate = new Date('2023-11-01').toISOString();
// const endDate = new Date('2023-11-08').toISOString();

Router.get("/",async(req,res)=>{
    const {startDate ,endDate } =  req.query
    console.log(req.query)
    const data = await scanTable(startDate,endDate)
    res.json(data)
})

Router.get("/:deal_id",async(req,res)=>{
    const {deal_id} = req.params
    let {page } =  req.query
   console.log(deal_id)
   const data =  await getItemByDealProcessingTransactionId(parseInt(deal_id),page)
    res.json(data)
})
Router.post("/",async(req,res)=>{

    const {deal_processing_transaction_id,created_at,activity_type,created_by,created_by_id,transaction_step} = req.body
    const Item ={
        "_id": uuid.v4(),
        "deal_processing_transaction_id": deal_processing_transaction_id,
        "created_at": created_at,
        "activity_type": activity_type,
        "created_by": created_by,
        "created_by_id": created_by_id,
        "transaction_step": transaction_step
      }


    console.log(Item);
    const response = await insertItemIntoActivityLog(Item);
    console.log(typeof response,response)
    res.status(200).send("Submitted Successfully")
})

module.exports = Router