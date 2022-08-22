const cron = require('node-cron');
const qry = require(`${PROJECT_DIR}/utility/selectQueries`);
const dtUtil = require(`${PROJECT_DIR}/utility/dateUtility`);
const moment = require('moment');
const uuidv4 = require('uuid/v4');
const { update } = require('lodash');
const sort = require(`${PROJECT_DIR}/utility/sort`);
const func = require(`${PROJECT_DIR}/utility/functionalUtility`);
// const { sendnotificationToDevice } = require(`${PROJECT_DIR}/utility/notifications`);


const autoEndDay = cron.schedule(
    `30 04 * * *`, //for Everyday At 4:30 Am In Night
    //`40 17 * * *`,
    async () => {
        try{
            console.log(`================= CRON FOR AUTO DAY END HAS STARTED =========================`);
            let date = new Date()
            date = dtUtil.addDays(date, -1);
            console.log(`Current Day -1 Date ----> ${date}`);
            // let date = "2022-05-01";
            //For Testing Sql
            // let day_notend_record_sql = `SELECT * FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_ATTENDANCE_TABLE_NAME} where emp_id__c = 'a0mC700000007zoIAA' and attendance_date__c='${date}'`
            let day_notend_record_sql = `SELECT * FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_ATTENDANCE_TABLE_NAME} where attendance_date__c = '${date}' and end_day__c = false`
            console.log(`All Record For Day Not End On Previous Day ---> ${day_notend_record_sql}`);
            let day_notend_res = await client.query(day_notend_record_sql)
            if(day_notend_res.rows.length > 0){
                for(let i = 0 ; i < day_notend_res.rows.length ; i++){
                    let last_visit_checkout_time;
                    let last_visit_checkout_lat;
                    let last_visit_checkout_long;
                    let team_member_id = day_notend_res.rows[i]['emp_id__c'];
                    let checkin_lat = day_notend_res.rows[i]['checkin_location__latitude__s']
                    let checkin_long = day_notend_res.rows[i]['checkin_location__longitude__s']
                    let attendence_id = day_notend_res.rows[i]['sfid']
                    console.log(`Running Cron For User ----------------------> ${team_member_id}`);

                    // let getVisitWorkingData = await func.getWorkingHourInMinutes(team_member_id,date)
                    //let data = dtUtil.todayDate() - 1 
                    //let visit_sql = `SELECT * FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_ATTENDANCE_TABLE_NAME} where  and visit_date__c = '${date}'`
                    let visit_sql = `select * from ${process.env.TABLE_SCHEMA_NAME}.${SF_VISIT_TABLE_NAME} 
                                        where emp_id__c = '${team_member_id}' 
                                        and visit_date__c = '${date}' 
                                        and check_in_time__c is not null
                                        order by check_in_time__c desc`
                    let visit_res = await client.query(visit_sql)
                    if(visit_res.rows.length > 0){
                        let vr = visit_res.rows
                        //console.log(`CO,CIN ----> ${vr[0]['check_out_time__c']} , ${vr[0]['check_in_time__c']}`);
                        //let temp_date = convertDatePickerTimeToMySQLTime(vr[0]['check_out_time__c']);
                        //console.log(`====TEMP DATE==== ${temp_date}`);

                        if(vr[0]['check_out_time__c'] != null && vr[0]['check_out_time__c'] != undefined){
                            last_visit_checkout_time =await dtUtil.convertDatePickerTimeToMySQLTime(vr[0]['check_out_time__c'])
                        }else{
                            //last_visit_checkout_time = vr[0]['check_in_time__c']
                            last_visit_checkout_time = await dtUtil.convertDatePickerTimeToMySQLTime(vr[0]['check_in_time__c'])
                        }
                        if( (vr[0]['check_out_location__latitude__s'] != null && vr[0]['check_out_location__latitude__s'] != undefined) && ((vr[0]['check_out_location__longitude__s'] != null && vr[0]['check_out_location__longitude__s'] != undefined))){
                            last_visit_checkout_lat = vr[0]['check_out_location__latitude__s']
                            last_visit_checkout_long = vr[0]['check_out_location__longitude__s']
                        }else{
                            last_visit_checkout_lat = vr[0]['check_in_location__latitude__s']
                            last_visit_checkout_long = vr[0]['check_in_location__longitude__s']
                        }
                        
                        let whereClouse = [];
                        let fieldValue=[];
                        const table_name = SF_ATTENDANCE_TABLE_NAME;
                        whereClouse.push({ "field": "sfid", "value": day_notend_res.rows[i]['sfid'] });
                        if(last_visit_checkout_long){
                            fieldValue.push({ "field": "checkout_location__longitude__s", "value": last_visit_checkout_long });
                        }else{
                            fieldValue.push({ "field": "checkout_location__longitude__s", "value": null , "type":'BOOLEAN'});
                        }
                        if(last_visit_checkout_lat){
                            fieldValue.push({ "field": "checkout_location__latitude__s", "value": last_visit_checkout_lat });
                        }else{
                            fieldValue.push({ "field": "checkout_location__latitude__s", "value": null , "type":'BOOLEAN'});
                        }
                        if(last_visit_checkout_time){
                            fieldValue.push({ "field": "end_time__c", "value": last_visit_checkout_time });
                        }else{
                            fieldValue.push({ "field": "end_time__c", "value": null , "type":'BOOLEAN'});

                        }
                        fieldValue.push({ "field": "end_day__c", "value": true , "type":'BOOLEAN'});
                        fieldValue.push({ "field": "batch_end_day__c", "value": true , "type":'BOOLEAN'});

                        let end_day_update_sql = await qry.updateRecord(table_name, fieldValue, whereClouse);
                        console.log(`Update Sql ---> ${JSON.stringify(end_day_update_sql)}`);
                        if(end_day_update_sql.success){
                            console.log(`End Day SuccessFull For Team ID ----> ${team_member_id}`);

                            let visit_related_data = await func.getTotalWorkingData(team_member_id, date);
                            let attendence_related_data = await func.getTotalAttendenceData(team_member_id, date);
                            let atndnce_fieldValue = [];
                            let atndnce_whereClouse = [];
                            if (visit_related_data.working_start_time) {
                                atndnce_fieldValue.push({ "field": "working_hours_start__c", "value": visit_related_data.working_start_time })
                            }
                            if (visit_related_data.working_end_time) {
                                atndnce_fieldValue.push({ "field": "working_hours_end__c", "value": visit_related_data.working_end_time })
                            }
                            if (visit_related_data.visit_working_time) {
                                atndnce_fieldValue.push({ "field": "working_hour__c", "value": Math.round(visit_related_data.visit_working_time) })
                            }
                            if (visit_related_data.visit_working_dist) {
                                atndnce_fieldValue.push({ "field": "total_working_distance__c", "value": visit_related_data.visit_working_dist })
                            }
                            if (attendence_related_data.total_day_time) {
                                atndnce_fieldValue.push({ "field": "total_hours__c", "value": Math.round(attendence_related_data.total_day_time) })
                            }
                            if (attendence_related_data.total_dist) {
                                atndnce_fieldValue.push({ "field": "total_distance_travelled__c", "value": attendence_related_data.total_dist })
                            }
                            atndnce_whereClouse.push({ "field": "sfid", "value": attendence_id });
                            let attndnce_update = await qry.updateRecord(SF_ATTENDANCE_TABLE_NAME, atndnce_fieldValue, atndnce_whereClouse)
                            if (attndnce_update.success) {
                                console.log(`Attendence Data Updated Successfully`);
                            }

                            let update_pjp = await func.updateAttendanceDataInPjp(team_member_id, date)
                            if (update_pjp == 'success') {
                                console.log(`Data Updated In Pjp Table Successfully`);
                            } else {
                                console.log(`Data Updated In Pjp Table Failed`);
                            }
                            // Updating the remarks field of last path
                            console.log("------------------------------------------------------------------------");
                            const collection = db.collection(process.env.MONGODB_COLLECTION);
                            
                            const result5 = await collection.find({
                                team_id: team_member_id,
                                date:date
                                })
                                let test=result5[0]['path'];
                                console.log("last path is======================",JSON.stringify(test[test_length-1]));
                                const update_data_encoded_path = await collection.updateOne(
                                        {
                                            team_id: team_member_id,
                                            date: date,
                                            "path.path_count":test[test_length-1]['path_count']
                                        },
                                        {
                                            $set: {
                                                "path.$.remarks__c":"DE"
                                            }
                                        }
                                    )
                                    console.log("--------------------------------------------------------------------------");
                            //Updating The Encoded Path In Mongo Db
                            // let data = await func.getPathLatLongInArrayFormat(team_member_id, date)
                            // let test = await func.setEncodedLatLongPath(data)
                            // console.log(`Test ====> ${test}`);
                            // const collection = db.collection(process.env.MONGODB_COLLECTION);
                            // const update_data_encoded_path = await collection.updateOne(
                            //     {
                            //         team_id: team_member_id,
                            //         date: date
                            //     },
                            //     {
                            //         $set: {
                            //             encoded_path: test,
                            //             active__c:false
                            //         }
                            //     }
                            // )
                            // //console.log(`update data -----> ${JSON.stringify(update_data)}`);
                            // if (update_data_encoded_path['modifiedCount'] > 0) {
                            //     console.log(`Data Updated For User ---> ${team_member_id} for Date --> ${date}`);
                            // } else {
                            //     console.log(`Data Updation Failed`);
                            // }
                        }else{
                            console.log(`End Day failed For Team ID ----> ${team_member_id}`);
                        }
                    }
                    else{
                        let previous_day_end_time = `${date} 18:29:00`
                        console.log(`Previous Date End Time ----> ${previous_day_end_time}`);
                        let whereClouse = [];
                        let fieldValue=[];
                        const table_name = SF_ATTENDANCE_TABLE_NAME;
                        whereClouse.push({ "field": "sfid", "value": day_notend_res.rows[i]['sfid'] });
                        if(checkin_long){
                            fieldValue.push({ "field": "checkout_location__longitude__s", "value": checkin_long });
                        }else{
                            fieldValue.push({ "field": "checkout_location__longitude__s", "value": null , "type":'BOOLEAN'});
                        }
                        if(checkin_lat){
                            fieldValue.push({ "field": "checkout_location__latitude__s", "value": checkin_lat });
                        }else{
                            fieldValue.push({ "field": "checkout_location__latitude__s", "value": null , "type":'BOOLEAN'});
                        }
                        fieldValue.push({ "field": "end_time__c", "value": previous_day_end_time });
                        fieldValue.push({ "field": "end_day__c", "value": true , "type":'BOOLEAN'});
                        fieldValue.push({ "field": "batch_end_day__c", "value": true , "type":'BOOLEAN'});

                        let end_day_update_sql = await qry.updateRecord(table_name, fieldValue, whereClouse);
                                                    
                        console.log(`Update Sql ---> ${end_day_update_sql}`);
                        if(end_day_update_sql.success){
                            //Updating The Encoded Path In Mongo Db When there is no visit 
                            console.log(`End Day SuccessFull For Team ID ----> ${team_member_id}`);

                            let visit_related_data = await func.getTotalWorkingData(team_member_id, date);
                            let attendence_related_data = await func.getTotalAttendenceData(team_member_id, date);
                            let atndnce_fieldValue = [];
                            let atndnce_whereClouse = [];
                            if (visit_related_data.working_start_time) {
                                atndnce_fieldValue.push({ "field": "working_hours_start__c", "value": visit_related_data.working_start_time })
                            }
                            if (visit_related_data.working_end_time) {
                                atndnce_fieldValue.push({ "field": "working_hours_end__c", "value": visit_related_data.working_end_time })
                            }
                            if (visit_related_data.visit_working_time) {
                                atndnce_fieldValue.push({ "field": "working_hour__c", "value": Math.round(visit_related_data.visit_working_time) })
                            }
                            if (visit_related_data.visit_working_dist) {
                                atndnce_fieldValue.push({ "field": "total_working_distance__c", "value": visit_related_data.visit_working_dist })
                            }
                            if (attendence_related_data.total_day_time) {
                                atndnce_fieldValue.push({ "field": "total_hours__c", "value": Math.round(attendence_related_data.total_day_time) })
                            }
                            if (attendence_related_data.total_dist) {
                                atndnce_fieldValue.push({ "field": "total_distance_travelled__c", "value": attendence_related_data.total_dist })
                            }
                            atndnce_whereClouse.push({ "field": "sfid", "value": attendence_id });
                            let attndnce_update = await qry.updateRecord(SF_ATTENDANCE_TABLE_NAME, atndnce_fieldValue, atndnce_whereClouse)
                            if (attndnce_update.success) {
                                console.log(`Attendence Data Updated Successfully`);
                            }

                            let update_pjp = await func.updateAttendanceDataInPjp(team_member_id, date)
                            if (update_pjp == 'success') {
                                console.log(`Data Updated In Pjp Table Successfully`);
                            } else {
                                console.log(`Data Updated In Pjp Table Failed`);
                            }
                            console.log("------------------------------------------------------------------------");
                            const collection = db.collection(process.env.MONGODB_COLLECTION);
                            
                            const result5 = await collection.find({
                                team_id: team_member_id,
                                date:date
                                }).toArray()
                                console.log("result5=============",result5);
                                let test=result5[0]['path'];
                                let test_length=test.length;
                                console.log("last path is======================",JSON.stringify(test[test_length-1]));
                                const update_data_encoded_path = await collection.updateOne(
                                        {
                                            team_id: team_member_id,
                                            date: date,
                                            "path.path_count":test[test_length-1]['path_count']
                                        },
                                        {
                                            $set: {
                                                "path.$.remarks__c":"DE"
                                            }
                                        }
                                    )
                                    console.log("--------------------------------------------------------------------------");
                            // let data = await func.getPathLatLongInArrayFormat(team_member_id, date)
                            // let test = await func.setEncodedLatLongPath(data)
                            // console.log(`Test ====> ${test}`);
                            // const collection = db.collection(process.env.MONGODB_COLLECTION);
                            // const update_data_encoded_path = await collection.updateOne(
                            //     {
                            //         team_id: team_member_id,
                            //         date: date
                            //     },
                            //     {
                            //         $set: {
                            //             encoded_path: test,
                            //             active__c:false
                            //         }
                            //     }
                            // )
                            // //console.log(`update data -----> ${JSON.stringify(update_data)}`);
                            // if (update_data_encoded_path['modifiedCount'] > 0) {
                            //     console.log(`Data Updated For User ---> ${team_member_id} for Date --> ${date}`);
                            // } else {
                            //     console.log(`Data Updation Failed`);
                            // }
                        }else{
                            console.log(`End Day failed For Team ID ----> ${team_member_id}`);
                        }
                    }
                }
            }
            console.log(`==================CRON FOR AUTO DAY END HAS ENDED ===========================`);
        }catch(e){
            let error_log = `Error In Auto Day End Cron ${e}`
            console.log(error_log);
            func.mailErrorLog(error_log, req.route['path'], req.body, req.query, req.headers.token, req.headers['client-name']);
            response.response = { success: false, message: 'Internal Server error.' };
            response.status = 500;
            return response;
        }
    },
    { scheduled: true }
);

//To Be Always Run Before Auto End Day
const autoEndVisit = cron.schedule(
    `30 03 * * *`, //for Everyday At 3:30 Am In Night
    //`35 17 * * *`, //for testing
    async () => {
        try{
            console.log(`================= CRON FOR AUTO END Visit HAS STARTED =========================`);
            let ongoing_visit_status = await func.getPicklistSfid('Visit__c','Visit_Status__c','Started')
            //let date = dtUtil.todayDate();
            let date = new Date()
            date = dtUtil.addDays(date, -1);
            console.log(`Current Day -1 Date ----> ${date}`);
            //let date = '2022-05-23'
            let visit_end_sql = `SELECT * FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_VISIT_TABLE_NAME} where check_out_time__c is null and visit_status__c = '${ongoing_visit_status}' and visit_date__c ='${date}'`
            //for testing sql
            //let visit_end_sql = `select * from salesforce.visit__c where emp_id__c = 'a0mC700000007zoIAA' and visit_date__c = '${date}'`
            //let visit_end_sql = `select * from salesforce.visit__c where sfid = 'a0rC70000004D5IIAU'`
            //console.log(`sql --> ${visit_end_sql}`);
            let visit_end_res = await client.query(visit_end_sql)
            if(visit_end_res.rows.length > 0){
                for(let i = 0 ; i < visit_end_res.rows.length ; i++){
                    // let lead_genrated = 0;
                    console.log(`Running For visit id ---------> ${visit_end_res.rows[0]['sfid']}`);
                    let completed_visit_status = await func.getPicklistSfid('Visit__c','Visit_Status__c','Completed')
                    let check_in_visit_lat = visit_end_res.rows[0]['check_in_location__latitude__s']
                    let check_in_visit_long = visit_end_res.rows[0]['check_in_location__longitude__s']
                    let check_in_visit_time = await dtUtil.convertDatePickerTimeToMySQLTime(visit_end_res.rows[0]['check_in_time__c'])
                    let visit_sfid = visit_end_res.rows[0]['sfid']

                    // let lead_on_visit_sql = `SELECT * FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_LEAD_TABLE_NAME} where visit__c = '${visit_end_res.rows[i]['sfid']}'`
                    // let lead_res = await client.query(lead_on_visit_sql);
                    // if(lead_res.rows.length > 0){
                    //     lead_genrated = lead_res.rows.length
                    // }
                    let visit_checkout_update_sql = `Update salesforce.visit__c 
                                                SET check_out_time__c = '${check_in_visit_time}' , check_out_location__latitude__s = '${check_in_visit_lat}' , check_out_location__longitude__s = '${check_in_visit_long}' , visit_status__c = '${completed_visit_status}'
                                                where sfid = '${visit_sfid}'`
                    //console.log(`Visit Update ---> ${visit_checkout_update_sql}`);
                    let result = await client.query(visit_checkout_update_sql)
                    if(result.rowCount == 1){
                        console.log(`Visit Ended For User ----> ${visit_sfid}`)
                        let visit_related_data = await func.getVisitData(visit_sfid)
                        let fieldValue = [];
                        let whereClouse = [];
                        fieldValue.push({ "field": "distance_travelled_during_scouting__c", "value": visit_related_data.total_visit_distance })
                        fieldValue.push({ "field": "working_hours_in_grid__c", "value": visit_related_data.total_visit_time })
                        fieldValue.push({ "field": "count_of_lead__c", "value": visit_related_data.leads_genrated })

                        whereClouse.push({ "field": "sfid", "value": visit_sfid });

                        let updateVisit = await qry.updateRecord(SF_VISIT_TABLE_NAME, fieldValue, whereClouse)
                        if (updateVisit.success) {
                            console.log(`Updating Of Visit Record Done`);
                            let update_pjp = await func.updateVisitDataInPjp(visit_related_data.pjp_id, visit_related_data.emp_id__c, visit_related_data.visit_date__c)
                            if (update_pjp == 'success') {
                                console.log(`Data Updated In Pjp Table Successfully`);
                            } else {
                                console.log(`Data Updated In Pjp Table Failed`);
                            }
                        }
                    }else{
                        console.log(`Visit Ended Failed For User ----> ${visit_sfid}`)
                    }
                }
            }
         console.log(`==================CRON FOR AUTO END Visit HAS Ended ===========================`);
        }catch(e){
            let error_log = `Error In Auto End Visit Cron ${e}`
            console.log(error_log);
            func.mailErrorLog(error_log, req.route['path'], req.body, req.query, req.headers.token, req.headers['client-name']);
            response.response = { success: false, message: 'Internal Server error.' };
            response.status = 500;
            return response;
        }
    },
    { scheduled: true }
);

// const autoLogout = cron.schedule(
//     `00 03 * * *`, //for Everyday At 3:00 Am In Night
//     async () => {
//         try{
//             console.log(`================= CRON FOR AUTO LOG OUT HAS STARTED =========================`);
//             let all_team_ids = [];
//             let all_team_member_sql = `SELECT * FROM  ${process.env.TABLE_SCHEMA_NAME}.${SF_TEAM_TABLE_NAME} where logged__c = true`
//             console.log(`All Team Member Who Has Not Logged Out  ---> ${all_team_member_sql}`);
//             let all_team_member_res = await client.query(all_team_member_sql)
//             if(all_team_member_res.rows.length > 0)
//                 all_team_member_res.rows.map((sfid) => {
//                     all_team_ids.push(sfid['sfid'])
//                 })
//                 //for(let i = 0 ; i< all_team_member_res.rows.length ; i++) {
//                 let update_team_sql = `Update ${process.env.TABLE_SCHEMA_NAME}.${SF_TEAM_TABLE_NAME} SET logged__c= false WHERE sfid IN ('${all_team_ids.join("','")}')`
//                 let result = await client.query(update_team_sql)
//                 if(result.success){
//                     console.log(`Updation Succesfull For Auto LogOut `)
//                 }else{
//                     console.log(`Updation Succesfull For Auto LogOut `)
//                 }
//                 //}
//                 console.log(`================== CRON FOR AUTO LOG OUT HAS ENDED ===========================`);
//         }catch(e){
//             let error_log = `Error In autoLogout Cron ${e}`
//             console.log(error_log);
//             func.mailErrorLog(error_log, req.route['path'], req.body, req.query, req.headers.token, req.headers['client-name']);
//             response.response = { success: false, message: 'Internal Server error.' };
//             response.status = 500;
//             return response;
//         }
//     },
//     { scheduled: true }
// );

// const distanceTimeCalculation = cron.schedule(
//     `30 05 * * *`, //for Everyday At 5:30 Am In Morning
//     //`12 10 * * *`, //for Testing
//     async () => {
//         try{
//         // console.log(`<--------------------- Cron Started For Distance Time Calculation -------------------->`);
//         let date = new Date()
//         date = dtUtil.addDays(date, -1);
//         console.log(`Current Day -1 Date ----> ${date}`);
//         let today_date = dtUtil.todayDate();
//         //Need To Fix This SQL To Only Run For New Data
//         //-----------------------------For Calculating Visit Part Multiple Params --------------------------------
//         let completed_visit_status = await func.getPicklistSfid('Visit__c','Visit_Status__c','Completed')
//         let all_visit_sql = `SELECT * FROM salesforce.visit__c where visit_status__c = '${completed_visit_status}' and visit_date__c = '${date}' and check_out_time__c is not null`
//         //For Testing Sql
//         //let all_visit_sql = `SELECT * FROM salesforce.visit__c where sfid = 'a0rC70000004CfKIAU'`
//         let all_visit_res = await client.query(all_visit_sql);
//         if(all_visit_res.rows.length > 0){
//             for(let i = 0 ; i< all_visit_res.rows.length ; i++){
//                 let fieldValue = [];
//                 let whereClouse = [];
//                 let distance = 0;
//                 let visit_sfid = all_visit_res.rows[i]['sfid']
//                 //For Visit Part --- Calculating Time FOr All Visit
//                 let start_time = await dtUtil.convertDatePickerTimeToMySQLTime(all_visit_res.rows[i]['check_in_time__c'])
//                 let end_time = await dtUtil.convertDatePickerTimeToMySQLTime(all_visit_res.rows[i]['check_out_time__c'])
//                 let pause_in_time = 'no_time';
//                 let pause_out_time = 'no_time';
//                 if(all_visit_res.rows[i]['pause_in_time__c'] != null && all_visit_res.rows[i]['pause_out_time__c'] != null){
//                     pause_in_time = await dtUtil.convertDatePickerTimeToMySQLTime(all_visit_res.rows[i]['pause_in_time__c'])
//                     pause_out_time = await dtUtil.convertDatePickerTimeToMySQLTime(all_visit_res.rows[i]['pause_out_time__c'])
//                 }
//                 console.log(`Start ----> ${start_time} End time ----> ${end_time} pause_in_time ---> ${pause_in_time} pause_Out_time ---> ${pause_out_time}`);
//                 let total_grid_working_hour = await dtUtil.getTotalHourMinuteV2(start_time,end_time)
//                 console.log(`After`);
//                 if(pause_in_time != 'no_time' && pause_out_time != 'no_time'){
//                     let total_pause_time = await dtUtil.getTotalHourMinuteV2(pause_in_time,pause_out_time)
//                     total_grid_working_hour = await dtUtil.getTotalHourMinuteV2(total_grid_working_hour,total_pause_time)
//                     fieldValue.push({ "field": "working_hours_in_grid__c", "value": total_grid_working_hour[0] });
//                 }else{
//                     //console.log(`Total Grid Working Hour ----> ${total_grid_working_hour}`);
//                     fieldValue.push({ "field": "working_hours_in_grid__c", "value": total_grid_working_hour[0] });
//                 }

//                 //For Visit Part --- Calculating Distance For All Visit
//                 //if(all_visit_res.rows[i]['grid__c'] != null){
//                     //need to correct
//                 let dummy = await func.getPathLatLongForSpecificVisitGrid(all_visit_res.rows[i]['sfid'],'visit','no_date')
//                 distance = await func.getDistanceOnWaypoints(dummy)
//                 if(distance > 0){
//                     fieldValue.push({ "field": "distance_travelled_during_scouting__c", "value": distance/1000 });
//                 }else{
//                     fieldValue.push({ "field": "distance_travelled_during_scouting__c", "value": distance });
//                 }
//                 //}else{
//                 //}
                


//                 //Now Updating All The Values 
//                 whereClouse.push({ 'field': 'sfid', 'value': visit_sfid }); 
//                 let update_visit_record_sql = await qry.updateRecord(SF_VISIT_TABLE_NAME, fieldValue, whereClouse)
//                 if(update_visit_record_sql.success){
//                     console.log(`Update Successfull For Visit Id ---> ${visit_sfid}`);
//                 }else{
//                     console.log(`Update Failed For Visit Id ---> ${visit_sfid}`);
//                 }

//             }
//         }

//         //--------------------------------For Lead Part -- For Calculation Customer Facing Time ------------------------------------------
//         let all_lead_sql = `SELECT * From salesforce.lead__c where visit_start_date__c is not null and visit_end_date__c is not null and lead_customer_facing_time__c is null and sfid is not null`
//         console.log(`All Lead SQl ----> ${all_lead_sql}`);
//         //let all_lead_sql = `SELECT * From salesforce.lead__c where sfid = 'a0TC7000000DyTOMA0'`
//         let all_lead_res = await client.query(all_lead_sql)
//         if(all_lead_res.rows.length > 0){
//             console.log(`InsideIf`);
//             for(let i = 0 ; i<all_lead_res.rows.length ; i++){
//                 let start_time = await dtUtil.convertDatePickerTimeToMySQLTime(all_lead_res.rows[i]['visit_start_date__c'])
//                 let end_time = await dtUtil.convertDatePickerTimeToMySQLTime(all_lead_res.rows[i]['visit_end_date__c'])
//                 console.log(`Start Time ----> ${start_time}  End Time -----> ${end_time}`);
//                 let total_grid_working_hour = await dtUtil.getTotalHourMinuteV2(start_time,end_time)
//                 if(total_grid_working_hour){
//                     let lead_update_sql = `Update salesforce.lead__c set lead_customer_facing_time__c = '${total_grid_working_hour[0]}'  where sfid = '${all_lead_res.rows[i]['sfid']}' `
//                     let lead_update_res = await client.query(lead_update_sql)
//                     console.log(`Lead Update Sql ----> ${lead_update_sql}`);
//                     console.log(`Lead Update Res ----> ${lead_update_res}`);
//                 }else{
//                     let lead_update_sql = `Update salesforce.lead__c set lead_customer_facing_time__c = '0'  where sfid = '${all_lead_res.rows[i]['sfid']}' `
//                     let lead_update_res = await client.query(lead_update_sql)
//                     console.log(`Lead Update Sql ----> ${lead_update_sql}`);
//                     console.log(`Lead Update Res ----> ${lead_update_res}`);
//                 }
//             }
//         }

//         //--------------------------------For PJP Part --- For Calculation Of Pjp Related Data ------------------------------------
//         //let pjp_sql = `Select * From salesforce.pjp__c where total_hours__c is null and total_working_hours__c is null and pjp_date__c = '${date}'`
//         //let pjp_sql = `Select * From salesforce.pjp__c where total_hours__c is null and total_working_hours__c is null and pjp_date__c = '2022-05-14'`
//         let pjp_sql = `Select * From salesforce.pjp__c where pjp_date__c = '${date}'`
//         //console.log(`PJP SQL ---> ${pjp_sql}`);
//         //let pjp_sql = `Select * From salesforce.pjp__c where sfid = 'a0ZC70000008VLqMAM'`
//         let pjp_res = await client.query(pjp_sql)
//         if(pjp_res.rows.length > 0){
//             for(let i = 0 ; i<pjp_res.rows.length ; i++){
//                 let fieldValue = [];
//                 let whereClouse = [];
//                 let team_id = pjp_res.rows[i]['emp_id__c']
//                 let pjp_date = dtUtil.ISOtoLocal(pjp_res.rows[i]['pjp_date__c'])
//                 pjp_date = await dtUtil.dateZeroFix(pjp_date)
//                 let pjp_sfid = pjp_res.rows[i]['sfid']
//                 console.log(`Pjp Date ----> ${pjp_date}`);
//                 let attendence_sql = `select * from salesforce.attendence__c where emp_id__c = '${team_id}' and attendance_date__c= '${pjp_date}'`
//                 console.log(`Attendnece SQL ---> ${attendence_sql}`);
//                 let attendence_res = await client.query(attendence_sql)
//                 if(attendence_res.rows.length > 0){
//                     if(attendence_res.rows[0]['start_time__c'] != null && attendence_res.rows[0]['end_time__c'] != null){
//                         let start_time = await dtUtil.convertDatePickerTimeToMySQLTime(attendence_res.rows[0]['start_time__c'])
//                         let end_time = await dtUtil.convertDatePickerTimeToMySQLTime(attendence_res.rows[0]['end_time__c'])
//                         //console.log(`Start Time ----> ${start_time}   End Time -------> ${end_time}`);
//                         let pause_in_time = 'no_time';
//                         let pause_out_time = 'no_time';
//                         if(attendence_res.rows[0]['pause_start__c'] != null && attendence_res.rows[0]['pause_end__c'] != null){
//                             pause_in_time = await dtUtil.convertDatePickerTimeToMySQLTime(attendence_res.rows[0]['pause_start__c'])
//                             pause_out_time = await dtUtil.convertDatePickerTimeToMySQLTime(attendence_res.rows[0]['pause_end__c'])
//                         }
//                         let total_grid_working_hour = await dtUtil.getTotalHourMinuteV2(start_time,end_time)
//                         if(pause_in_time != 'no_time' && pause_out_time != 'no_time'){
//                             let total_pause_time = await dtUtil.getTotalHourMinuteV2(pause_in_time,pause_out_time)
//                             total_grid_working_hour = total_grid_working_hour[0] - total_pause_time[0]
//                             fieldValue.push({ "field": "total_hours__c", "value": total_grid_working_hour });
//                         }else{
//                             fieldValue.push({ "field": "total_hours__c", "value": total_grid_working_hour[0] });
//                         }
//                     }else{
//                         fieldValue.push({ "field": "total_hours__c", "value": 0 });
//                     }
                    

//                     //Pause Start Pause End Missing
//                     let total_day_distance = await func.getPathLatLongForSpecificVisitGrid(team_id,'day',pjp_date)
//                     let total_day_distance_value = await func.getDistanceOnWaypoints(total_day_distance)
//                     console.log(`Total Distance ---> ${total_day_distance_value/1000}`);
//                     if(total_day_distance_value > 0){
//                         fieldValue.push({ "field": "total_distance__c", "value": total_day_distance_value/1000 });                    
//                     }else{
//                         fieldValue.push({ "field": "total_distance__c", "value": 0 });                    
//                     }
                    
//                 }else{
//                     fieldValue.push({ "field": "total_hours__c", "value": 0 });
//                     fieldValue.push({ "field": "total_distance__c", "value": 0 });  
//                 }

//                 let completed_visit_status = await func.getPicklistSfid('Visit__c','Visit_Status__c','Completed')
//                 let total_visit_time = 0
//                 let visit_sql = `SELECT * FROM salesforce.visit__c where pjp_header__c = '${pjp_sfid}' and visit_status__c = '${completed_visit_status}' order by check_in_time__c asc`
//                 console.log(`Visit SQL ---> ${visit_sql}`);
//                 let visit_res =  await client.query(visit_sql)
//                 if(visit_res.rows.length > 0){
//                     // let total_cft = 0;
//                     let pause_time = 0
//                     let start_visit_start_time = await dtUtil.convertDatePickerTimeToMySQLTime(visit_res.rows[0]['check_in_time__c'])
//                     let end_visit_end_time = await dtUtil.convertDatePickerTimeToMySQLTime(visit_res.rows[visit_res.rows.length - 1]['check_out_time__c'])
//                     let pause_in_time = 'no_time';
//                     let pause_out_time = 'no_time';
//                     let all_visit_distance = 0;
//                     //Pause In Logic Missing
//                     for(let i = 0 ; i<visit_res.rows.length ; i++ ){
//                         if(visit_res.rows[i]['pause_in_time__c'] != null && visit_res.rows[i]['pause_out_time__c'] != null){
//                             pause_in_time = await dtUtil.convertDatePickerTimeToMySQLTime(visit_res.rows[i]['pause_in_time__c'])
//                             pause_out_time = await dtUtil.convertDatePickerTimeToMySQLTime(visit_res.rows[i]['pause_out_time__c'])
//                             let pause_time__c = await dtUtil.getTotalHourMinuteV2(pause_in_time,pause_out_time)
//                             pause_time += pause_time__c[0]
//                         }
//                         let total_visit_distance = await func.getPathLatLongForSpecificVisitGrid(visit_res.rows[i]['sfid'],'visit','no_date')
//                         let total_visit_distance_value = await func.getDistanceOnWaypoints(total_visit_distance)/1000
//                         all_visit_distance += total_visit_distance_value

//                         total_visit_time += visit_res.rows[i]['working_hours_in_grid__c']
//                         if(pause_time > 0){
//                             total_visit_time -= pause_time
//                         }
//                     }
                    
//                     //let total_grid_working_hour = await dtUtil.getTotalHourMinuteV2(start_visit_start_time,end_visit_end_time)
//                     //let total_visit_time = total_grid_working_hour[0] - pause_time
//                     fieldValue.push({ "field": "visit_working_hour__c", "value": total_visit_time });   
//                     fieldValue.push({ "field": "total_working_distance__c", "value": all_visit_distance });                    
                    
//                 }else{
//                     fieldValue.push({ "field": "visit_working_hour__c", "value": 0 });   
//                     fieldValue.push({ "field": "total_working_distance__c", "value": 0 });  
//                 }
//                 whereClouse.push({ 'field': 'sfid', 'value': pjp_sfid }); 
//                 if(fieldValue.length > 0){
//                     let update_pjp = await qry.updateRecord(SF_PJP_TABLE_NAME, fieldValue, whereClouse)
//                     if(update_pjp.success){
//                         console.log(`Pjp Data Updated For Pjp Id ----> ${pjp_sfid}`);
//                     }else{
//                     console.log(`Pjp Data Updated Failed For Pjp Id ----> ${pjp_sfid}`);
//                     }
//                 }else{
//                     console.log(`No Data To Upload For selected pjp ----> ${pjp_sfid}`);
//                 }
//             }
//         }
//         console.log(`<--------------------- Cron Ended For Distance Time Calculation -------------------->`);
//     }catch(e){
//         console.log(`Error in Distance And Time Calculation ----> ${e}`);
//     }

//     },
//     { scheduled: true }
// );

// const attendenceDataUpdate = cron.schedule(
//     `30 06 * * *`, //for Everyday At 6:30 Am In Morning
//     //For Testing
//     //`31 10 * * *`, //for testing
//     async () => {
//         try{
//             console.log(`-----------------------------Cron Started For Attendence Data Update ------------------------------------------`);
//             let date = new Date()
//             date = dtUtil.addDays(date, -1);
//             //let date = '2022-05-16'
//             console.log(`Current Day -1 Date ----> ${date}`);
//             let completed_visit_status = await func.getPicklistSfid('Visit__c','Visit_Status__c','Completed')
//             //For Testing Sql
//             //let attendence_sql = `SELECT * FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_ATTENDANCE_TABLE_NAME} where emp_id__c = 'a0mC700000007z5IAA' and attendance_date__c = '2022-05-14'`
//             //let attendence_sql = `SELECT * FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_ATTENDANCE_TABLE_NAME} where attendance_date__c = '${date}' and end_time__c is not null`
//             let attendence_sql = `SELECT * FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_ATTENDANCE_TABLE_NAME} where attendance_date__c = '${date}'`
//             console.log(`All Record For Day End On Previous Day ---> ${attendence_sql}`);
//             let attendence_res = await client.query(attendence_sql)
//             if(attendence_res.rows.length > 0){
//                 for(let i = 0 ; i< attendence_res.rows.length ; i++){
//                     let all_visit_distance = 0
//                     let team_id = attendence_res.rows[i]['emp_id__c']
//                     //let end_time__c = attendence_res.rows[i]['end_time__c']
//                     //let working_data = await func.getWorkingHourInMinutes(team_id,date,end_time__c)
//                     // let day_data = await func.getDayWorkingHourInMinutes(team_id,date)
//                     //console.log(`Working Data ----> ${JSON.stringify(day_data)}`);

//                     //For Visit Related Data
//                     let visit_data = {
//                         'working_hour_start_at':null,
//                         'working_hour_end_at':null,
//                         'total_working_hour':0
//                     }
//                     //let visit_data_sql = `SELECT * FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_VISIT_TABLE_NAME} where emp_id__c = '${team_id}' and visit_date__c = '${date}' and visit_status__c = '${completed_visit_status}' and sfid is not null order by check_in_time__c asc`
//                     let visit_data_sql = `SELECT * FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_VISIT_TABLE_NAME} where emp_id__c = '${team_id}' and visit_date__c = '${date}' and visit_status__c = '${completed_visit_status}' and sfid is not null order by check_in_time__c asc`

//                     console.log(`Visit Data Sql -----> ${visit_data_sql}`);
//                     let first_visit_res = await client.query(visit_data_sql);
//                     if(first_visit_res.rows.length > 0){
//                         if(first_visit_res.rows[0]['check_in_time__c']){
//                             visit_data.working_hour_start_at = await dtUtil.convertDatePickerTimeToMySQLTime(first_visit_res.rows[0]['check_in_time__c'])
//                         }
//                         if(first_visit_res.rows[first_visit_res.rows.length - 1]['check_out_time__c']){
//                             visit_data.working_hour_end_at = await dtUtil.convertDatePickerTimeToMySQLTime(first_visit_res.rows[first_visit_res.rows.length - 1]['check_out_time__c'])
//                         }
//                         first_visit_res.rows.map((data) => {
//                             visit_data.total_working_hour += data['working_hours_in_grid__c']
//                         })
//                     }

//                     //For Attedence Related Data
//                     let start_time = await dtUtil.convertDatePickerTimeToMySQLTime(attendence_res.rows[i]['start_time__c'])
//                     let end_time = await dtUtil.convertDatePickerTimeToMySQLTime(attendence_res.rows[i]['end_time__c'])
//                     //console.log(`Start Time ----> ${start_time}   End Time -------> ${end_time}`);
//                     let pause_in_time = 'no_time';
//                     let pause_out_time = 'no_time';
//                     if (attendence_res.rows[0]['pause_start__c'] != null && attendence_res.rows[0]['pause_end__c'] != null) {
//                         pause_in_time = await dtUtil.convertDatePickerTimeToMySQLTime(attendence_res.rows[i]['pause_start__c'])
//                         pause_out_time = await dtUtil.convertDatePickerTimeToMySQLTime(attendence_res.rows[i]['pause_end__c'])
//                     }
//                     let total_grid_working_hour = await dtUtil.getTotalHourMinuteV2(start_time, end_time)
//                     console.log(`Data -------> ${total_grid_working_hour}`);
//                     let total_working_hour = total_grid_working_hour[0]
//                     if (pause_in_time != 'no_time' && pause_out_time != 'no_time') {
//                         let total_pause_time = await dtUtil.getTotalHourMinuteV2(pause_in_time, pause_out_time)
//                         total_working_hour = total_grid_working_hour[0] - total_pause_time[0]
//                     }
//                     console.log(`Total_grid_working_hour -----> ${total_grid_working_hour[0]}`);

//                     let total_day_distance = await func.getPathLatLongForSpecificVisitGrid(team_id,'day',date)
//                     let total_day_distance_value = await func.getDistanceOnWaypoints(total_day_distance)
//                     console.log(`Total Distance ---> ${total_day_distance_value/1000}`);

//                     //console.log(`HERE`);
//                     //let visit_sql = `SELECT * FROM salesforce.visit__c where emp_id__c = '${team_id}' and visit_date__c = '${date}' and visit_status__c = '${completed_visit_status}' and sfid is not null`
//                     let visit_sql = `SELECT * FROM salesforce.visit__c where emp_id__c = '${team_id}' and visit_date__c = '${date}' and visit_status__c = '${completed_visit_status}' and sfid is not null`
//                     //console.log(`Visit SQl ------> ${visit_sql}`);
//                     let visit_res = await client.query(visit_sql)
//                     for(let i = 0 ; i < visit_res.rows.length ; i++){
//                         let total_visit_distance = await func.getPathLatLongForSpecificVisitGrid(visit_res.rows[i]['sfid'],'visit','no_date')
//                         let total_visit_distance_value = await func.getDistanceOnWaypoints(total_visit_distance)
//                         if(total_visit_distance_value){
//                             all_visit_distance += total_visit_distance_value
//                         }
//                     }

//                     let whereClouse = [];
//                     let fieldValue=[];
//                     const table_name = SF_ATTENDANCE_TABLE_NAME;
//                     if(all_visit_distance > 0){
//                         fieldValue.push({ "field": "total_working_distance__c", "value": all_visit_distance/1000 });
//                     }else{
//                         fieldValue.push({ "field": "total_working_distance__c", "value": 0 });
//                     }
//                     if(total_day_distance_value > 0){
//                         fieldValue.push({ "field": "total_distance_travelled__c", "value": total_day_distance_value/1000 });
//                     }else{
//                         fieldValue.push({ "field": "total_distance_travelled__c", "value": 0 });
//                     }   
//                     if(total_working_hour){
//                         fieldValue.push({ "field": "total_hours__c", "value": total_working_hour });
//                     }
//                     if(visit_data.total_working_hour){
//                         fieldValue.push({ "field": "working_hour__c", "value": Math.round(visit_data.total_working_hour) });
//                     }
//                     if(visit_data.working_hour_end_at){
//                         fieldValue.push({ "field": "working_hours_end__c", "value": visit_data.working_hour_end_at });
//                     }else{
//                         fieldValue.push({ "field": "working_hours_end__c", "value": null , "type":'BOOLEAN'});
//                     }
//                     if(visit_data.working_hour_start_at){
//                         fieldValue.push({ "field": "working_hours_start__c", "value": visit_data.working_hour_start_at });
//                     }else{
//                         fieldValue.push({ "field": "working_hours_start__c", "value": null , "type":'BOOLEAN'});
//                     }
//                     whereClouse.push({ "field": "sfid", "value": attendence_res.rows[i]['sfid'] });
//                     let attendence_update_sql = await qry.updateRecord(table_name, fieldValue, whereClouse);

//                     // let attendence_update_sql = `Update salesforce.attendence__c 
//                     //                             set total_working_distance__c = '${all_visit_distance}',
//                     //                             total_distance_travelled__c = '${total_day_distance_value}',
//                     //                             total_hours__c = '${Math.round(total_grid_working_hour[0])}', 
//                     //                             working_hour__c = '${Math.round(visit_data.total_working_hour)}',
//                     //                             working_hours_end__c = '${visit_data.working_hour_end_at}',
//                     //                             working_hours_start__c = '${visit_data.working_hour_start_at}'
//                     //                             where sfid = '${attendence_res.rows[i]['sfid']}' `
//                     // console.log(`Update sql ----> ${attendence_update_sql}`);
//                     // let attendence_update_res = await client.query(attendence_update_sql)
//                     if(attendence_update_sql.success){
//                         console.log(`Data Updated For User -----> ${team_id} and date -----> ${date}`);
//                     }else{
//                         console.log(`Data Updated Failed For User -----> ${team_id} and date -----> ${date}`);
//                     }
//                 }
//             }
//             console.log(`-----------------------------Cron Ended For Attendence Data Update ------------------------------------------`);
//         }catch(e){
//             let error_log = `Error In attendenceDataUpdate Cron ${e}`
//             console.log(error_log);
//             func.mailErrorLog(error_log, req.route['path'], req.body, req.query, req.headers.token, req.headers['client-name']);
//             response.response = { success: false, message: 'Internal Server error.' };
//             response.status = 500;
//             return response;
//         }
//     },
//     { scheduled: true }
// );

// const attendenceDataUpdate = cron.schedule(
//     //`00 18 * * *`, //for Everyday At 6:00 Am In Evening
//     //For Testing
//     `57 17 * * *`, //for testing
//     async () => {
//         try{
             
            
//         }catch(e){
//             let error_log = `Error In attendenceDataUpdate Cron ${e}`
//             console.log(error_log);
//             func.mailErrorLog(error_log, req.route['path'], req.body, req.query, req.headers.token, req.headers['client-name']);
//             response.response = { success: false, message: 'Internal Server error.' };
//             response.status = 500;
//             return response;
//         }
//     },
//     { scheduled: true }
// );

module.exports = {
    autoEndDay,
    autoEndVisit
  };