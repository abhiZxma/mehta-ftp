const cron = require('node-cron');
const qry = require(`${PROJECT_DIR}/utility/selectQueries`);
const dtUtil = require(`${PROJECT_DIR}/utility/dateUtility`);
const moment = require('moment');
const validation = require(`${PROJECT_DIR}/utility/validation`);
const uuidv4 = require('uuid/v4');
const whatsapp = require(`${PROJECT_DIR}/utility/whatsApp`);
const notification=require(`${PROJECT_DIR}/utility/notifications`)

const task_notification_job = cron.schedule(`44 11 * * *`,
async ()=>{
try {
    let today_date=dtUtil.todayDate()
    let tom_date=dtUtil.addDays(today_date,1)
    let get_all_task_for_tom_sql=`SELECT ${SF_TEAM_TABLE_NAME}.sfid,${SF_TEAM_TABLE_NAME}.token__c,${SF_TEAM_TABLE_NAME}.team_member_name__c,${SF_TASK_TABLE_NAME}.sfid as taskSfid,${SF_TASK_TABLE_NAME}.name as taskName,${SF_PICKLIST_TABLE_NAME}.name as taskType FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_TASK_TABLE_NAME} 
    LEFT JOIN ${process.env.TABLE_SCHEMA_NAME}.${SF_TEAM_TABLE_NAME} ON ${SF_TEAM_TABLE_NAME}.sfid=${SF_TASK_TABLE_NAME}.task_owner__c
    LEFT JOIN ${process.env.TABLE_SCHEMA_NAME}.${SF_PICKLIST_TABLE_NAME} ON ${SF_PICKLIST_TABLE_NAME}.sfid=${SF_TASK_TABLE_NAME}.nature_of_task__c
    WHERE ${SF_TASK_TABLE_NAME}.task_date__c='${tom_date}' AND ${SF_TASK_TABLE_NAME}.task_owner__c IS NOT NULL AND ${SF_TEAM_TABLE_NAME}.sfid IS NOT NULL AND ${SF_TEAM_TABLE_NAME}.token__c IS NOT NULL`
   console.log("get_all_task_for_tom_sql",get_all_task_for_tom_sql)
    let get_all_task_for_res=await client.query(get_all_task_for_tom_sql)
    if(get_all_task_for_res.rows.length>0){
        for (let i = 0; i < get_all_task_for_res.rows.length; i++) {
            const element = get_all_task_for_res.rows[i];
            let title=element.tasktype
            let body=`Hi ${element.team_member_name__c} , You have a task ${element.taskname} for ${element.tasktype} for tommorow `
            // let firebase_token=element.token__c
            let notification_msg={
                title,
                body
            }
            let data_to_insert={
                user__c:element.sfid,
                title,
                body,
                firebase_token__c:element.token__c
            }
            console.log("notification_msg",notification_msg)
            console.log("data_to_insert",data_to_insert)
            await notification.sendnotificationToDevice(data_to_insert,notification_msg,{})
        }
    }else{
        console.log("no data found for the sql")
    }
} catch (error) {
console.log("error in task_notification_job---------->",error)    
}

},{ scheduled: true })
module.exports={
    task_notification_job
}