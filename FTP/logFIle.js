const dtUtil = require(`${PROJECT_DIR}/utility/dateUtility`);
const path = require('path');
const fs = require('fs');
const csv=require('csvtojson')
const db = require(`${PROJECT_DIR}/utility/selectQueries`);
const createCsvWriter = require('csv-writer');

function dateToMiliseconds(date){
    var dateTobConvert=new Date(date)
    var convertedMs = dateTobConvert.getTime();

   return convertedMs
} 
const createPendingOrderlogFile=async(Arrdata)=>{
    try {
        let realtime = dtUtil.todayDate()
       // let realtime2=dtUtil.todayDateAndTime()
        let realtime2 = dtUtil.todayDatetime()
        let now = dateToMiliseconds(realtime2)
        let records=[]
        console.log('__dir',__dirname)
        //et filename = path.join(__dirname, `./Files/OUT/logs_${realtime}_${now}.csv`)
        for (let i = 0; i < Arrdata.length; i++) {
            const element = Arrdata[i];
            records.push({ S_No: i + 1, salesDocumentNo: element.docNo, status: element.status, data: element.data,message:element.message })
        }
        

        const csvWriter = createCsvWriter.createObjectCsvWriter({
            'path': `FTP/Files/OUT/pending_order/pending_order_${realtime}_${now}.csv`,
            'header': [
                { id: 'S_No', title: 'S No' },
                { id: 'salesDocumentNo', title: 'salesDocumentNo' },
                { id: 'status', title: 'status' },
                { id: 'data', title: 'data' },
                { id: 'message', title: 'message' }
             ]
        });
        let ntg = await csvWriter.writeRecords(records)
        return `pending_order_${realtime}_${now}.csv`   
    
    } catch (error) {
        console.log(error)
    }
 }
 const createPrimarySaleslogFile=async(Arrdata)=>{
    try {
        let realtime = dtUtil.todayDate()
       // let realtime2=dtUtil.todayDateAndTime()
        let realtime2 = dtUtil.todayDatetime()
        let now = dateToMiliseconds(realtime2)
        let records=[]
        console.log('__dir',__dirname)
        //et filename = path.join(__dirname, `./Files/OUT/logs_${realtime}_${now}.csv`)
        for (let i = 0; i < Arrdata.length; i++) {
            const element = Arrdata[i];
            records.push({ S_No: i + 1, InvNo: element.docNo, status: element.status, data: element.data,message:element.message })
        }
        
        const csvWriter = createCsvWriter.createObjectCsvWriter({
            'path': `FTP/Files/OUT/primary_sales/primary_sale_${realtime}_${now}.csv`,
            'header': [
                { id: 'S_No', title: 'S No' },
                { id: 'InvNo', title: 'InvNo' },
                { id: 'status', title: 'status' },
                { id: 'data', title: 'data' },
                { id: 'message', title: 'message' }
             ]
        });
        let ntg = await csvWriter.writeRecords(records)   
        return `primary_sale_${realtime}_${now}.csv`
    
    } catch (error) {
        console.log(error)
    }
 }
 const createoutstandinglogFile=async(Arrdata)=>{
    try {
        let realtime = dtUtil.todayDate()
       // let realtime2=dtUtil.todayDateAndTime()
        let realtime2 = dtUtil.todayDatetime()
        let now = dateToMiliseconds(realtime2)
        let records=[]
        console.log('__dir',__dirname)
        //et filename = path.join(__dirname, `./Files/OUT/logs_${realtime}_${now}.csv`)
        for (let i = 0; i < Arrdata.length; i++) {
            const element = Arrdata[i];
            records.push({ S_No: i + 1, docNo: element.docNo, status: element.status, data: element.data,message:element.message })
        }
        

        const csvWriter = createCsvWriter.createObjectCsvWriter({
            'path': `FTP/Files/OUT/outstanding/outstanding_${realtime}_${now}.csv`,
            'header': [
                { id: 'S_No', title: 'S No' },
                { id: 'docNo', title: 'recordId' },
                { id: 'status', title: 'status' },
                { id: 'data', title: 'data' },
                { id: 'message', title: 'message' }
             ]
        });
        let ntg = await csvWriter.writeRecords(records)   
        return `outstanding_${realtime}_${now}.csv`
    } catch (error) {
        console.log(error)
    }
 }
 const createDealerlogFile=async(Arrdata)=>{
    try {
        let realtime = dtUtil.todayDate()
       // let realtime2=dtUtil.todayDateAndTime()
        let realtime2 = dtUtil.todayDatetime()
        let now = dateToMiliseconds(realtime2)
        let records=[]
        console.log('__dir',__dirname)
        //et filename = path.join(__dirname, `./Files/OUT/logs_${realtime}_${now}.csv`)
        for (let i = 0; i < Arrdata.length; i++) {
            const element = Arrdata[i];
            records.push({ S_No: i + 1, customerId: element.docNo, status: element.status, data: element.data,message:element.message })
        }
        

        const csvWriter = createCsvWriter.createObjectCsvWriter({
            'path': `FTP/Files/OUT/dealer/dealer_${realtime}_${now}.csv`,
            'header': [
                { id: 'S_No', title: 'S No' },
                { id: 'customerId', title: 'customerId' },
                { id: 'status', title: 'status' },
                { id: 'data', title: 'data' },
                { id: 'message', title: 'message' }
             ]
        });
        let ntg = await csvWriter.writeRecords(records)   
        return `dealer_${realtime}_${now}.csv`
    } catch (error) {
        console.log(error)
    }
 }
 const createArealogFile=async(Arrdata,type)=>{
    try {
        let realtime = dtUtil.todayDate()
       // let realtime2=dtUtil.todayDateAndTime()
        let realtime2 = dtUtil.todayDatetime()
        let now = dateToMiliseconds(realtime2)
        let records=[]
        console.log('__dir',__dirname)
        //et filename = path.join(__dirname, `./Files/OUT/logs_${realtime}_${now}.csv`)
        for (let i = 0; i < Arrdata.length; i++) {
            const element = Arrdata[i];
            records.push({ S_No: i + 1, code: element.docNo, status: element.status, data: element.data,message:element.message })
        }

        const csvWriter = createCsvWriter.createObjectCsvWriter({
            'path': `FTP/Files/OUT/area/${type}_${realtime}_${now}.csv`,
            'header': [
                { id: 'S_No', title: 'S No' },
                { id: 'code', title: 'code' },
                { id: 'status', title: 'status' },
                { id: 'data', title: 'data' },
                { id: 'message', title: 'message' }
             ]
        });
        let ntg = await csvWriter.writeRecords(records)   
        return `${type}_${realtime}_${now}.csv`
    } catch (error) {
        console.log(error)
    }
 }
 const createProductlogFile=async(Arrdata)=>{
    try {
        let realtime = dtUtil.todayDate()
       // let realtime2=dtUtil.todayDateAndTime()
        let realtime2 = dtUtil.todayDatetime()
        let now = dateToMiliseconds(realtime2)
        let records=[]
        console.log('__dir',__dirname)
        //et filename = path.join(__dirname, `./Files/OUT/logs_${realtime}_${now}.csv`)
        for (let i = 0; i < Arrdata.length; i++) {
            const element = Arrdata[i];
            records.push({ S_No: i + 1, code: element.docNo, status: element.status, data: element.data,message:element.message })
        }

        const csvWriter = createCsvWriter.createObjectCsvWriter({
            'path': `FTP/Files/OUT/product_item/product_item_${realtime}_${now}.csv`,
            'header': [
                { id: 'S_No', title: 'S No' },
                { id: 'code', title: 'code' },
                { id: 'status', title: 'status' },
                { id: 'data', title: 'data' },
                { id: 'message', title: 'message' }
             ]
        });
        let ntg = await csvWriter.writeRecords(records)   
        return `product_item_${realtime}_${now}.csv`
    } catch (error) {
        console.log(error)
    }
 }
 const createCreditLimitlogFile=async(Arrdata)=>{
    try {
        let realtime = dtUtil.todayDate()
       // let realtime2=dtUtil.todayDateAndTime()
        let realtime2 = dtUtil.todayDatetime()
        let now = dateToMiliseconds(realtime2)
        let records=[]
        console.log('__dir',__dirname)
        //et filename = path.join(__dirname, `./Files/OUT/logs_${realtime}_${now}.csv`)
        for (let i = 0; i < Arrdata.length; i++) {
            const element = Arrdata[i];
            records.push({ S_No: i + 1, code: element.docNo, status: element.status, data: element.data,message:element.message })
        }
        const csvWriter = createCsvWriter.createObjectCsvWriter({
            'path': `FTP/Files/OUT/credit_limit/credit_limit_${realtime}_${now}.csv`,
            'header': [
                { id: 'S_No', title: 'S No' },
                { id: 'code', title: 'code' },
                { id: 'status', title: 'status' },
                { id: 'data', title: 'data' },
                { id: 'message', title: 'message' }
             ]
        });
        let ntg = await csvWriter.writeRecords(records)   
        return `credit_limit_${realtime}_${now}.csv`
    } catch (error) {
        console.log(error)
    }
 }
module.exports={
    createPendingOrderlogFile,
    createPrimarySaleslogFile,
    createDealerlogFile,
    createArealogFile,
    createoutstandinglogFile,
    createProductlogFile,
    createCreditLimitlogFile
}