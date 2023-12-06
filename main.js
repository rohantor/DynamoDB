const Router =  require("./router")
const express = require("express");
const app = express();
app.use(express.json())
app.use("/",Router)

app.listen(3000, ()=> {
    console.log("Server started on port 3000");
})