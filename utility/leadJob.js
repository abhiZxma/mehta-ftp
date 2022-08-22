const cron = require('node-cron');
const qry = require(`${PROJECT_DIR}/utility/selectQueries`);
const dtUtil = require(`${PROJECT_DIR}/utility/dateUtility`);
const moment = require('moment');
const validation = require(`${PROJECT_DIR}/utility/validation`);
const uuidv4 = require('uuid/v4');
const whatsapp = require(`${PROJECT_DIR}/utility/whatsApp`);
const notification=require(`${PROJECT_DIR}/utility/notifications`);
const func = require(`${PROJECT_DIR}/utility/functionalUtility`);

// const { sendnotificationToDevice } = require(`${PROJECT_DIR}/utility/notifications`);

const leadStatusOnDateJob = cron.schedule(
    `00 01 * * *`,  //for Everyday At 1 Am In Night
    //`40 15 * * *`, //For testing purpose
    async () => {
        try{

            let hot_hot_min_value;
            let hot_hot_max_value;
            let hot_min_value;
            let hot_max_value;
            let warm_min_value;
            let warm_max_value;
            let cold_min_value;
            let cold_max_value;
            let info_min_value;
            
            //Hot_hot picklist
            let hot_hot_sfid = `Select sfid from ${process.env.TABLE_SCHEMA_NAME}.${SF_PICKLIST_TABLE_NAME} WHERE ${SF_PICKLIST_TABLE_NAME}.object_name__c='Site_Status_master__c' AND ${SF_PICKLIST_TABLE_NAME}.field_name__c= 'Site_Status__c'AND ${SF_PICKLIST_TABLE_NAME}.name = 'Hot Hot'`
            let hot_hot_sfid_res = await client.query(hot_hot_sfid);
            hot_hot_sfid_res = hot_hot_sfid_res.rows[0]['sfid'];

            //Hot picklist
            let hot_sfid = `Select sfid from ${process.env.TABLE_SCHEMA_NAME}.${SF_PICKLIST_TABLE_NAME} WHERE ${SF_PICKLIST_TABLE_NAME}.object_name__c='Site_Status_master__c' AND ${SF_PICKLIST_TABLE_NAME}.field_name__c= 'Site_Status__c'AND ${SF_PICKLIST_TABLE_NAME}.name = 'Hot'`
            let hot_sfid_res = await client.query(hot_sfid);
            hot_sfid_res = hot_sfid_res.rows[0]['sfid'];

            //Warm Picklist
            let warm_sfid = `Select sfid from ${process.env.TABLE_SCHEMA_NAME}.${SF_PICKLIST_TABLE_NAME} WHERE ${SF_PICKLIST_TABLE_NAME}.object_name__c='Site_Status_master__c' AND ${SF_PICKLIST_TABLE_NAME}.field_name__c= 'Site_Status__c'AND ${SF_PICKLIST_TABLE_NAME}.name = 'Warm'`
            let warm_sfid_res = await client.query(warm_sfid);
            warm_sfid_res = warm_sfid_res.rows[0]['sfid'];

            //Cold Picklist
            let cold_sfid = `Select sfid from ${process.env.TABLE_SCHEMA_NAME}.${SF_PICKLIST_TABLE_NAME} WHERE ${SF_PICKLIST_TABLE_NAME}.object_name__c='Site_Status_master__c' AND ${SF_PICKLIST_TABLE_NAME}.field_name__c= 'Site_Status__c'AND ${SF_PICKLIST_TABLE_NAME}.name = 'Cold'`
            let cold_sfid_res = await client.query(cold_sfid);
            cold_sfid_res = cold_sfid_res.rows[0]['sfid'];

            //Information Picklist
            let Information_sfid = `Select sfid from ${process.env.TABLE_SCHEMA_NAME}.${SF_PICKLIST_TABLE_NAME} WHERE ${SF_PICKLIST_TABLE_NAME}.object_name__c='Site_Status_master__c' AND ${SF_PICKLIST_TABLE_NAME}.field_name__c= 'Site_Status__c'AND ${SF_PICKLIST_TABLE_NAME}.name = 'Information'`
            let Information_sfid_res = await client.query(Information_sfid);
            Information_sfid_res = Information_sfid_res.rows[0]['sfid'];

            //Site fir Picklist
            let site_fir = `Select sfid from ${process.env.TABLE_SCHEMA_NAME}.${SF_PICKLIST_TABLE_NAME} WHERE ${SF_PICKLIST_TABLE_NAME}.object_name__c='Lead__c' AND ${SF_PICKLIST_TABLE_NAME}.field_name__c= 'Site_FIR__c'AND ${SF_PICKLIST_TABLE_NAME}.name IN('Lead','Cold Lead') order by name desc`
            let site_fir_res = await client.query(site_fir);
            let lead_pkl = site_fir_res.rows[0]['sfid'];
            let cold_lead_pkl = site_fir_res.rows[1]['sfid'];
            console.log('lead::::::::',lead_pkl,'cold_lead_pkl:::::::::::::::',cold_lead_pkl)
            let all_lead_sql = `SELECT * FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_LEAD_TABLE_NAME} where sfid is NOT NULL AND site_fir__c IN ('${lead_pkl}','${cold_lead_pkl}')`
            let all_lead_sql_res = await client.query(all_lead_sql);
            console.log('for all lead sql:::::::::::::::::::',all_lead_sql)
            //For HOT-HOT
            let hot_hot_sql = `SELECT * FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_VALUE_PARAMETERIZATION_TABLE_NAME} where output_value__c = 'Hot-hot'`
            let hot_hot_sql_res = await client.query(hot_hot_sql)
            if(hot_hot_sql_res.rows.length > 0){
                hot_hot_min_value = hot_hot_sql_res.rows[0]['parameterized_min_value__c']
                hot_hot_max_value = hot_hot_sql_res.rows[0]['parameterized_max_value__c']
            }else{
                hot_hot_min_value = 0
                hot_hot_max_value = 15
            }
            //FOR HOT
            let hot_sql = `SELECT * FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_VALUE_PARAMETERIZATION_TABLE_NAME} where output_value__c = 'Hot'`
            let hot_sql_res = await client.query(hot_sql)
            if(hot_sql_res.rows.length > 0){
                hot_min_value = hot_sql_res.rows[0]['parameterized_min_value__c']
                hot_max_value = hot_sql_res.rows[0]['parameterized_max_value__c']
            }else{
                hot_min_value = 15
                hot_max_value = 45
            }
            //FOR WARM
            let warm_sql = `SELECT * FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_VALUE_PARAMETERIZATION_TABLE_NAME} where output_value__c = 'Warm'`
            let warm_sql_res = await client.query(warm_sql)
            if(warm_sql_res.rows.length > 0){
                warm_min_value = warm_sql_res.rows[0]['parameterized_min_value__c']
                warm_max_value = warm_sql_res.rows[0]['parameterized_max_value__c']
            }else{
                warm_min_value = 45
                warm_max_value = 90
            }
            //FOR COLD
            let cold_sql = `SELECT * FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_VALUE_PARAMETERIZATION_TABLE_NAME} where output_value__c = 'Cold'`
            let cold_sql_res = await client.query(cold_sql)
            if(cold_sql_res.rows.length > 0){
                cold_min_value = cold_sql_res.rows[0]['parameterized_min_value__c']
                cold_max_value = cold_sql_res.rows[0]['parameterized_max_value__c']
            }else{
                cold_min_value = 90
                cold_max_value = 150
            }
            //FOR INFORMATION
            let info_sql = `SELECT * FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_VALUE_PARAMETERIZATION_TABLE_NAME} where output_value__c = 'Information'`
            let info_sql_res = await client.query(info_sql)
            if(info_sql_res.rows.length > 0){
                info_min_value = info_sql_res.rows[0]['parameterized_min_value__c']
            }else{
                info_min_value = 150
            }


            let today_date = new Date();
            today_date = moment(today_date).format('YYYY-MM-DD');
            //console.log('Today Date ---->',today_date);
    
            if(all_lead_sql_res.rows.length > 0){
                for(let i = 0 ; i < all_lead_sql_res.rows.length ; i++){
    
                    let lead_sfid = all_lead_sql_res.rows[i]['sfid'];
                    //console.log('Sfid ---->',lead_sfid);
    
                    let lead_expected_maturity_date = dtUtil.ISOtoLocal(all_lead_sql_res.rows[i]['expected_maturity_date__c']);
                    //console.log('Expected Maturity Date ----> ',lead_expected_maturity_date);
    
                    const date1 = new Date(`'${today_date}'`);
                    const date2 = new Date(`'${lead_expected_maturity_date}'`);
                    //console.log('--->',date1,date2);
                    const diffTime = Math.abs(date2 - date1);
                    const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24));
                    //console.log('Diff Days ---->',diffDays,diffTime);
    
                    if(diffDays <= hot_hot_max_value){
                        //Case For Hot Hot
                        console.log('Case 1');
                        let open_close_validation = await validation.taskStatusOpenCloseLiveLeadValidation(hot_hot_sfid_res)
                        let lead_value = open_close_validation.lead_value;
                        let live_lead_value = open_close_validation.live_lead_value;
                        let update_sql = `update ${process.env.TABLE_SCHEMA_NAME}.${SF_LEAD_TABLE_NAME} set site_status__c= '${hot_hot_sfid_res}',live_lead__c = ${live_lead_value},lead_open_closed__c = '${lead_value}' where sfid = '${lead_sfid}'`
                        let update_res = await client.query(update_sql)
                        console.log('Update sql ---->',update_sql);
                        console.log('Update Sql Res ---->',update_res);
                        // if(update_res.success){
                        //     console.log('Updated Successfully In Case For Hot Hot');
                        // }else{
                        //     console.log('Update Failed !!!!!');
                        // }
    
                    }
                    if(diffDays > hot_min_value && diffDays <= hot_max_value){
                        //Case For Hot
                        console.log('Case 2');
                        let open_close_validation = await validation.taskStatusOpenCloseLiveLeadValidation(hot_sfid_res)
                        let lead_value = open_close_validation.lead_value;
                        let live_lead_value = open_close_validation.live_lead_value;
                        let update_sql = `update ${process.env.TABLE_SCHEMA_NAME}.${SF_LEAD_TABLE_NAME} set site_status__c= '${hot_sfid_res}',live_lead__c = ${live_lead_value},lead_open_closed__c = '${lead_value}' where sfid = '${lead_sfid}'`
                        let update_res = await client.query(update_sql)
                        console.log('Update sql ---->',update_sql);
                        console.log('Update Sql Res ---->',update_res);
                        // if(update_res.success){
                        //     console.log('Updated Successfully In Case For Hot');
                        // }else{
                        //     console.log('Update Failed !!!!!');
                        // }
                    }
                    if(diffDays > warm_min_value && diffDays <= warm_max_value){
                        //Case For Warm
                        console.log('Case 3');
                        let open_close_validation = await validation.taskStatusOpenCloseLiveLeadValidation(warm_sfid_res)
                        let lead_value = open_close_validation.lead_value;
                        let live_lead_value = open_close_validation.live_lead_value;
                        let update_sql = `update ${process.env.TABLE_SCHEMA_NAME}.${SF_LEAD_TABLE_NAME} set site_status__c= '${warm_sfid_res}',live_lead__c = ${live_lead_value},lead_open_closed__c = '${lead_value}' where sfid = '${lead_sfid}'`
                        let update_res = await client.query(update_sql)
                        console.log('Update sql ---->',update_sql);
                        console.log('Update Sql Res ---->',update_res);
                        // if(update_res.success){
                        //     console.log('Updated Successfully In Case For Warm');
                        // }else{
                        //     console.log('Update Failed !!!!!');
                        // }
                    }
                    if(diffDays > cold_min_value && diffDays <= cold_max_value){
                        //Case For Cold
                        console.log('Case 4');
                        let open_close_validation = await validation.taskStatusOpenCloseLiveLeadValidation(cold_sfid_res)
                        let lead_value = open_close_validation.lead_value;
                        let live_lead_value = open_close_validation.live_lead_value;
                        let update_sql = `update ${process.env.TABLE_SCHEMA_NAME}.${SF_LEAD_TABLE_NAME} set site_status__c= '${cold_sfid_res}',live_lead__c = ${live_lead_value},lead_open_closed__c = '${lead_value}' where sfid = '${lead_sfid}'`
                        let update_res = await client.query(update_sql)
                        console.log('Update sql ---->',update_sql);
                        console.log('Update Sql Res ---->',update_res);
                        // if(update_res.success){
                        //     console.log('Updated Successfully In Case For Cold');
                        // }else{
                        //     console.log('Update Failed !!!!!');
                        // }   
                    }
                    if(diffDays > info_min_value && (all_lead_sql_res.rows[i]['owner_number__c'] != undefined && all_lead_sql_res.rows[i]['owner_number__c'].length > 0)){
                        //Case For Informaton
                        console.log('Case 5');
                        let open_close_validation = await validation.taskStatusOpenCloseLiveLeadValidation(Information_sfid_res)
                        let lead_value = open_close_validation.lead_value;
                        let live_lead_value = open_close_validation.live_lead_value;
                        let update_sql = `update ${process.env.TABLE_SCHEMA_NAME}.${SF_LEAD_TABLE_NAME} set site_status__c= '${Information_sfid_res}',live_lead__c = ${live_lead_value},lead_open_closed__c = '${lead_value}' where sfid = '${lead_sfid}'`
                        let update_res = await client.query(update_sql)
                        console.log('Update sql ---->',update_sql);
                        console.log('Update Sql Res ---->',update_res);
                        // if(update_res.success){
                        //     console.log('Updated Successfully In Case For Information');
                        // }else{
                        //     console.log('Update Failed !!!!!');
                        // }
                    }
                }
                console.log('Cron For All Lead Updated Its Status In site_status__c');
            }else{
                console.log('Cron Started But No Lead Record Found');
            }
        }catch(e){
          let error_log = `Error In Lead Cron Job leadStatusOnDateJob ----->${e}`
          console.log(error_log);
          func.mailErrorLog(error_log, req.route['path'], req.body, req.query, req.headers.token, req.headers['client-name']);
          response.response = { success: false, message: 'Internal Server error.' };
          response.status = 500;
          return response;
        }
    },
    { scheduled: true }
)
// TODO-  NEED TO CHANGE QUERY TO GET ALL THE RECORDS WHICH WE HAVE OWNER NUMBER AND OWNER NAME NOT BLANK & COMMENT IF CONDITION

const leadStatusOnCreationDate = cron.schedule(
    `00 01 * * *`,  //for Everyday At 1 Am In Night
    async () => {
        try{
            let date_to_be_checked_for_inactive ;

            //Owner Picklist
            let owner_sfid = `Select sfid from ${process.env.TABLE_SCHEMA_NAME}.${SF_PICKLIST_TABLE_NAME} WHERE ${SF_PICKLIST_TABLE_NAME}.object_name__c='Contact' AND ${SF_PICKLIST_TABLE_NAME}.field_name__c= 'Contact_Type__c'AND ${SF_PICKLIST_TABLE_NAME}.name = 'Owner'`
            let owner_sfid_res = await client.query(owner_sfid);
            owner_sfid_res = owner_sfid_res.rows[0]['sfid'];

             //Inactive Picklist
             let inactive_sfid = `Select sfid from ${process.env.TABLE_SCHEMA_NAME}.${SF_PICKLIST_TABLE_NAME} WHERE ${SF_PICKLIST_TABLE_NAME}.object_name__c='Site_Status_master__c' AND ${SF_PICKLIST_TABLE_NAME}.field_name__c= 'Site_Status__c'AND ${SF_PICKLIST_TABLE_NAME}.name = 'Inactive'`
             let inactive_sfid_res = await client.query(inactive_sfid);
             inactive_sfid_res = inactive_sfid_res.rows[0]['sfid'];
            
             //Site fir Picklist
            let site_fir = `Select sfid from ${process.env.TABLE_SCHEMA_NAME}.${SF_PICKLIST_TABLE_NAME} WHERE ${SF_PICKLIST_TABLE_NAME}.object_name__c='Lead__c' AND ${SF_PICKLIST_TABLE_NAME}.field_name__c= 'Site_FIR__c'AND ${SF_PICKLIST_TABLE_NAME}.name IN('Lead','Cold Lead') order by name desc`
            let site_fir_res = await client.query(site_fir);
            let lead_pkl = site_fir_res.rows[0]['sfid'];
            let cold_lead_pkl = site_fir_res.rows[1]['sfid'];
            console.log('lead::::::::',lead_pkl,'cold_lead_pkl:::::::::::::::',cold_lead_pkl)

            let all_lead_sql = `SELECT ${SF_LEAD_TABLE_NAME}.*, contact.mobilephone, contact.name as contact_name FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_LEAD_TABLE_NAME}
            LEFT JOIN salesforce.contact ON lead__c.sfid=contact.lead__c
             where ${SF_LEAD_TABLE_NAME}.sfid is NOT NULL AND ${SF_LEAD_TABLE_NAME}.site_fir__c IN ('${lead_pkl}','${cold_lead_pkl}') AND contact.name is NOT NULL AND contact.mobilephone is NOT NULL AND contact.contact_type__c ='${owner_sfid_res}'`
            //let all_lead_sql = `SELECT * FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_LEAD_TABLE_NAME} where sfid = 'a0F0w0000014wEyEAI'`
            let all_lead_sql_res = await client.query(all_lead_sql);
            console.log('all_lead_sql_res:::::::::::::::::::',all_lead_sql)
    
            let value_parameterization_sql = `SELECT * FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_VALUE_PARAMETERIZATION_TABLE_NAME} where name='30'`
            let value_res = await client.query(value_parameterization_sql);
    
            if(value_res.rows.length > 0){
                date_to_be_checked_for_inactive = value_res.rows[0]['parameterized_value__c']
            }else{
                date_to_be_checked_for_inactive = 30;
            }
            console.log("date_to_be_checked_for_inactive::::::::",date_to_be_checked_for_inactive)
            let today_date = new Date();
            today_date = moment(today_date).format('YYYY-MM-DD');
            //console.log('Today Date ---->',today_date);
        
            if(all_lead_sql_res.rows.length > 0){
                for(let i = 0 ; i < all_lead_sql_res.rows.length ; i++){
        
                    let lead_sfid = all_lead_sql_res.rows[i]['sfid'];
                    console.log('Sfid ---->',lead_sfid);
                    let Inactive_status = all_lead_sql_res.rows[i]['inactive__c'];
                    console.log('Sfid ---->',Inactive_status);
    
                    let lead_created_date = dtUtil.ISOtoLocal(all_lead_sql_res.rows[i]['lead_created_date__c']);
                    console.log('Expected Maturity Date ----> ',lead_created_date);
    
                    const date1 = new Date(`'${today_date}'`);
                    const date2 = new Date(`'${lead_created_date}'`);
                    console.log('--->',date1,date2);
                    const diffTime = Math.abs(date2 - date1);
                    const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24));
                    console.log('Diff Days ---->',diffDays,diffTime);
    
                    //if((all_lead_sql_res.rows[i]['contact_name'] != undefined && all_lead_sql_res.rows[i]['contact_name'].length > 0) && (all_lead_sql_res.rows[i]['mobilephone'] != undefined && all_lead_sql_res.rows[i]['mobilephone'].length > 0)){
                        
                        // if(diffDays == 15){
                        // console.log('Case 1');
                        // let update_sql = `update ${process.env.TABLE_SCHEMA_NAME}.${SF_LEAD_TABLE_NAME} set site_status__c='a050w000003QLntAAG' where sfid = '${lead_sfid}'`
                        // let update_res = await client.query(update_sql)
                        // console.log('Update sql ---->',update_sql);
                        // console.log('Update Sql Res ---->',update_res);
                        // // if(update_res.success){
                        // //     console.log('Updated Successfully In Case For Information');
                        // // }else{
                        // //     console.log('Update Failed !!!!!');
                        // // }
                        // }
                  //Inactive Picklist
                  let Inactive_sfid_pic = `Select sfid from ${process.env.TABLE_SCHEMA_NAME}.${SF_PICKLIST_TABLE_NAME} WHERE ${SF_PICKLIST_TABLE_NAME}.object_name__c='Site_Status_master__c' AND ${SF_PICKLIST_TABLE_NAME}.field_name__c= 'Site_Status__c'AND ${SF_PICKLIST_TABLE_NAME}.name = 'Inactive'`
                  let Inactive_sfid_pic_res = await client.query(Inactive_sfid_pic);
                  Inactive_sfid_pic_res = Inactive_sfid_pic_res.rows[0]['sfid'];
                  console.log('Inactive_sfid_res:::::::::', Inactive_sfid_pic_res)
                        if(diffDays == date_to_be_checked_for_inactive){
                            console.log('Case 2 For setting status as inactive');
                            let open_close_validation = await validation.taskStatusOpenCloseLiveLeadValidation(inactive_sfid_res)
                            let lead_value = open_close_validation.lead_value;
                            let live_lead_value = open_close_validation.live_lead_value;
                            let update_sql = `update ${process.env.TABLE_SCHEMA_NAME}.${SF_LEAD_TABLE_NAME} set site_status__c= '${inactive_sfid_res}',active_status__c= '${Inactive_sfid_pic_res}',live_lead__c = ${live_lead_value},lead_open_closed__c = '${lead_value}' where sfid = '${lead_sfid}'`
                            //let update_sql = `update ${process.env.TABLE_SCHEMA_NAME}.${SF_LEAD_TABLE_NAME} set site_status__c='a050w000003QLoHAAW' where sfid = '${lead_sfid}'`
                            let update_res = await client.query(update_sql)
                            console.log('Update sql ---->',update_sql);
                            console.log('Update Sql Res ---->',update_res);
                          let task_close_sfid = `Select sfid from ${process.env.TABLE_SCHEMA_NAME}.${SF_PICKLIST_TABLE_NAME} WHERE ${SF_PICKLIST_TABLE_NAME}.object_name__c='Task__c' AND ${SF_PICKLIST_TABLE_NAME}.field_name__c= 'Task_Status__c'AND ${SF_PICKLIST_TABLE_NAME}.name = 'Closed'`
                          let task_close_sfid_res = await client.query(task_close_sfid);
                          task_close_sfid_res = task_close_sfid_res.rows[0]['sfid'];
                          console.log('close task picklist sql:::::::',task_close_sfid_res)
                          if (lead_sfid.length > 20) {
                            let task_of_lead_sql = `SELECT sfid FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_TASK_TABLE_NAME} Where lead__c = '${lead_sfid}'`
                            let result = await client.query(task_of_lead_sql)
                            if (result.rows.length > 0) {
                              result.rows.map((id) => {
                                task_sfids_arr.push(id['sfid'])
                              })
                              let update_task = `update ${process.env.TABLE_SCHEMA_NAME}.${SF_TASK_TABLE_NAME} 
                                                      set task_status__c= '${task_close_sfid_res}',task_outcome__c = null 
                                                      where sfid IN ('${task_sfids_arr.join("','")}')`
                              let response = await client.query(update_task)
                              console.log(`Update Task In Inactive ----> ${response}`);
                            }
                          }
                        }
                        
                    //}
                    
                }
            }
        }catch(e){
          let error_log = `Error In Lead Cron Job leadStatusOnCreationDate ----->${e}`
          console.log(error_log);
          func.mailErrorLog(error_log, req.route['path'], req.body, req.query, req.headers.token, req.headers['client-name']);
          response.response = { success: false, message: 'Internal Server error.' };
          response.status = 500;
          return response;
        }
    },
    { scheduled: true }
)
//TODO - NEED TO TEST ONE MORE TIME , WHEN A LEAD IS INACTIVE THEN LEAD__C SHOULD BE INACTIVE
const leadStatusOnInformation = cron.schedule(
    `00 02 * * *`,  //Everyday At 2 Am 
    //`28 18 * * *`,  //Everyday At 2 Am for testing
    async () => {
        try{
          let owner_sfid = `Select sfid from ${process.env.TABLE_SCHEMA_NAME}.${SF_PICKLIST_TABLE_NAME} WHERE ${SF_PICKLIST_TABLE_NAME}.object_name__c='Contact' AND ${SF_PICKLIST_TABLE_NAME}.field_name__c= 'Contact_Type__c'AND ${SF_PICKLIST_TABLE_NAME}.name = 'Owner'`
          let owner_sfid_res = await client.query(owner_sfid);
          owner_sfid_res = owner_sfid_res.rows[0]['sfid'];
            //Information Picklist
            let Information_sfid = `Select sfid from ${process.env.TABLE_SCHEMA_NAME}.${SF_PICKLIST_TABLE_NAME} WHERE ${SF_PICKLIST_TABLE_NAME}.object_name__c='Site_Status_master__c' AND ${SF_PICKLIST_TABLE_NAME}.field_name__c= 'Site_Status__c'AND ${SF_PICKLIST_TABLE_NAME}.name = 'Information'`
            let Information_sfid_res = await client.query(Information_sfid);
            Information_sfid_res = Information_sfid_res.rows[0]['sfid'];

            //Site fir Picklist
            let site_fir = `Select sfid from ${process.env.TABLE_SCHEMA_NAME}.${SF_PICKLIST_TABLE_NAME} WHERE ${SF_PICKLIST_TABLE_NAME}.object_name__c='Lead__c' AND ${SF_PICKLIST_TABLE_NAME}.field_name__c= 'Site_FIR__c'AND ${SF_PICKLIST_TABLE_NAME}.name IN('Lead','Cold Lead') order by name desc`
            let site_fir_res = await client.query(site_fir);
            let lead_pkl = site_fir_res.rows[0]['sfid'];
            let cold_lead_pkl = site_fir_res.rows[1]['sfid'];
            console.log('lead::::::::',lead_pkl,'cold_lead_pkl:::::::::::::::',cold_lead_pkl)
           
            let all_lead_sql = `SELECT ${SF_LEAD_TABLE_NAME}.*, contact.mobilephone FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_LEAD_TABLE_NAME}
            LEFT JOIN salesforce.contact ON lead__c.sfid=contact.lead__c
             where ${SF_LEAD_TABLE_NAME}.sfid is NOT NULL AND ${SF_LEAD_TABLE_NAME}.site_fir__c IN ('${lead_pkl}','${cold_lead_pkl}') AND contact.mobilephone is Null AND contact.contact_type__c ='${owner_sfid_res}'`
            let all_lead_sql_res = await client.query(all_lead_sql);
            console.log('for all lead sql:::::::::::::',all_lead_sql)
            if(all_lead_sql_res.rows.length > 0){
                for(let i = 0 ; i < all_lead_sql_res.rows.length ; i++){
                    let lead_created_date = dtUtil.ISOtoLocal(all_lead_sql_res.rows[i]['lead_created_date__c']);
                    //let lead_created_date = dtUtil.ISOtoLocal('lead_created_date__c');
                    console.log('Expected Maturity Date ----> ',lead_created_date);
                    let lead_sfid = all_lead_sql_res.rows[i]['sfid'];

                    console.log('Sfid ---->',lead_sfid);
                        let today_date = new Date();
                        today_date = moment(today_date).format('YYYY-MM-DD');
                        let date1 = new Date(`'${today_date}'`);
                        let date2 = new Date(`'${lead_created_date}'`);
                        console.log('--->',date1,date2);
                        let diffTime = Math.abs(date2 - date1);
                        let diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24));
                        console.log('Diff Days ---->',diffDays,diffTime);

                  let one_fifty_days = `SELECT * FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_VALUE_PARAMETERIZATION_TABLE_NAME} where parameterized_value__c IN ('150','30') order by parameterized_value__c desc`
                  let one_fifty_days_res = await client.query(one_fifty_days);

                  if (one_fifty_days_res.rows.length > 0) {
                    one_fifty = one_fifty_days_res.rows[0]['parameterized_value__c']
                    thirty_days = one_fifty_days_res.rows[1]['parameterized_value__c']
                  } else {
                    one_fifty = 150;
                    one_fifty = 30;
                  }
                  console.log("one_fifty_days_res::::::::::::",one_fifty,'30DAYS::::::',thirty_days)
                  //Inactive Picklist
                  let Inactive_sfid_pic = `Select sfid from ${process.env.TABLE_SCHEMA_NAME}.${SF_PICKLIST_TABLE_NAME} WHERE ${SF_PICKLIST_TABLE_NAME}.object_name__c='Lead__c' AND ${SF_PICKLIST_TABLE_NAME}.field_name__c= 'Active_Status__c'AND ${SF_PICKLIST_TABLE_NAME}.name = 'InActive'`
                  let Inactive_sfid_pic_res = await client.query(Inactive_sfid_pic);
                  Inactive_sfid_pic_res = Inactive_sfid_pic_res.rows[0]['sfid'];
                  console.log('Inactive_sfid_res:::::::::', Inactive_sfid_pic_res)
                        if(diffDays > one_fifty ){
                            console.log('Case 1 set lead as information');
                            let update_sql = `update ${process.env.TABLE_SCHEMA_NAME}.${SF_LEAD_TABLE_NAME} set lead__c='${Information_sfid_res}' where sfid='${lead_sfid}'`
                            let update_res = await client.query(update_sql)
                            console.log('Update sql ---->',update_sql);
                            console.log('Update Sql Res ---->',update_res);
                        }
                  //close Picklist
                  let close_sfid = `Select sfid from ${process.env.TABLE_SCHEMA_NAME}.${SF_PICKLIST_TABLE_NAME} WHERE ${SF_PICKLIST_TABLE_NAME}.object_name__c='Lead__c' AND ${SF_PICKLIST_TABLE_NAME}.field_name__c= 'Lead_Open_Closed__c'AND ${SF_PICKLIST_TABLE_NAME}.name = 'Closed'`
                  let close_sfid_res = await client.query(close_sfid);
                  close_sfid_res = close_sfid_res.rows[0]['sfid'];
                  console.log('close_sfid_res::::::::', close_sfid_res)
                  //site_status__c Picklist
                  let site_inactive_sfid = `Select sfid from ${process.env.TABLE_SCHEMA_NAME}.${SF_PICKLIST_TABLE_NAME} WHERE ${SF_PICKLIST_TABLE_NAME}.object_name__c='Site_Status_master__c' AND ${SF_PICKLIST_TABLE_NAME}.field_name__c= 'Site_Status__c'AND ${SF_PICKLIST_TABLE_NAME}.name IN ('Inactive','Won and confirmation pending','Hot','Warm','Cold','Won and Ongoing','Won and completed','Information','Pending for phone no','Hot Hot','Lost') order by name desc`
                  let site_inactive_sfid_res = await client.query(site_inactive_sfid);
                  let inactive = site_inactive_sfid_res.rows[7]['sfid'];
                  console.log('site_inactive_sfid_res::::::::', inactive)
                        if(diffDays > thirty_days){
                          console.log('Case 2 For checking lead as inactive');
                          let update_sql = `update ${process.env.TABLE_SCHEMA_NAME}.${SF_LEAD_TABLE_NAME} set site_status__c='${inactive}',inactive_date__c='${today_date}', active_status__c='${Inactive_sfid_pic_res}', lead_open_closed__c='${close_sfid_res}', live_lead__c='${false}', expected_maturity_date__c=null where sfid = '${lead_sfid}' AND inactive__c='true'`
                          let update_res = await client.query(update_sql)
                          console.log('Update sql ---->',update_sql);
                          console.log('Update Sql Res ---->',update_res);
                          let task_close_sfid = `Select sfid from ${process.env.TABLE_SCHEMA_NAME}.${SF_PICKLIST_TABLE_NAME} WHERE ${SF_PICKLIST_TABLE_NAME}.object_name__c='Task__c' AND ${SF_PICKLIST_TABLE_NAME}.field_name__c= 'Task_Status__c'AND ${SF_PICKLIST_TABLE_NAME}.name = 'Closed'`
                          let task_close_sfid_res = await client.query(task_close_sfid);
                          task_close_sfid_res = task_close_sfid_res.rows[0]['sfid'];
                          console.log('close task picklist sql:::::::',task_close_sfid_res)
                            let task_of_lead_sql = `SELECT sfid FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_TASK_TABLE_NAME} Where lead__c = '${lead_sfid}'`
                            let result = await client.query(task_of_lead_sql)
                            let task_sfids_arr = []
                            if (result.rows.length > 0) {
                              result.rows.map((id) => {
                                task_sfids_arr.push(id['sfid'])
                              })
                              let update_task = `update ${process.env.TABLE_SCHEMA_NAME}.${SF_TASK_TABLE_NAME} 
                                                      set task_status__c= '${task_close_sfid_res}',task_outcome__c = null 
                                                      where sfid IN ('${task_sfids_arr.join("','")}')`
                              let response = await client.query(update_task)
                              console.log(`update_task ----> ${update_task}`);
                              console.log('response sql:::::::',response)

                            }
                      }
                }
                console.log('Cron For All Lead Updated Its information In lead__c');
            }
            else{
                console.log('Cron Started But No Lead Record Found');
            }
        }catch(e){
          let error_log = `Error In Lead Cron Job leadStatusOnInformation ----->${e}`
          console.log(error_log);
          func.mailErrorLog(error_log, req.route['path'], req.body, req.query, req.headers.token, req.headers['client-name']);
          response.response = { success: false, message: 'Internal Server error.' };
          response.status = 500;
          return response;
        }
    },
    { scheduled: true }
)
//TODO - NEED TO TEST ONE MORE TIME , WHEN A LEAD IS INACTIVE THEN LEAD__C SHOULD BE INACTIVE, 2ND POINT NEED TO CHECK FOR CHANGE OF LCD 
// TO TODAY_DATE && NNED TO REMOVE CONDITIONS(SUPPLY FIRST AND ON -GOING)

const leadStatusOnDateAndStage = cron.schedule(
    `34 17 * * *`,
    //`46 00 * * *`,   //Everyday At 12:30 AM
    async () => {
        try{
            console.log('.......');

            //Won and Ongoing Picklist
            let Won_and_Ongoing_sfid = `Select sfid from ${process.env.TABLE_SCHEMA_NAME}.${SF_PICKLIST_TABLE_NAME} WHERE ${SF_PICKLIST_TABLE_NAME}.object_name__c='Site_Status_master__c' AND ${SF_PICKLIST_TABLE_NAME}.field_name__c= 'Site_Status__c'AND ${SF_PICKLIST_TABLE_NAME}.name IN ('Won and Ongoing','Won and completed','Inactive') order by name desc`
            let Won_and_Ongoing_sfid_res = await client.query(Won_and_Ongoing_sfid);
            let won_ongoing_picklist = Won_and_Ongoing_sfid_res.rows[0]['sfid'];
            let won_completed_picklist = Won_and_Ongoing_sfid_res.rows[1]['sfid'];
            let inactive_picklist = Won_and_Ongoing_sfid_res.rows[2]['sfid'];


            //Open lead
            let Open_sfid = `Select sfid from ${process.env.TABLE_SCHEMA_NAME}.${SF_PICKLIST_TABLE_NAME} WHERE ${SF_PICKLIST_TABLE_NAME}.object_name__c='Lead__c' AND ${SF_PICKLIST_TABLE_NAME}.field_name__c= 'Lead_Open_Closed__c'AND ${SF_PICKLIST_TABLE_NAME}.name = 'Open'`
            let Open_sfid_res = await client.query(Open_sfid);
            Open_sfid_res = Open_sfid_res.rows[0]['sfid'];

            //Owner Picklist
            let owner_sfid = `Select sfid from ${process.env.TABLE_SCHEMA_NAME}.${SF_PICKLIST_TABLE_NAME} WHERE ${SF_PICKLIST_TABLE_NAME}.object_name__c='Contact' AND ${SF_PICKLIST_TABLE_NAME}.field_name__c= 'Contact_Type__c'AND ${SF_PICKLIST_TABLE_NAME}.name = 'Owner'`
            let owner_sfid_res = await client.query(owner_sfid);
            owner_sfid_res = owner_sfid_res.rows[0]['sfid'];

            //Stage-1 Picklist
            let stage1_sfid = `Select sfid from ${process.env.TABLE_SCHEMA_NAME}.${SF_PICKLIST_TABLE_NAME} WHERE ${SF_PICKLIST_TABLE_NAME}.object_name__c='Lead__c' AND ${SF_PICKLIST_TABLE_NAME}.field_name__c= 'Stage__c'AND ${SF_PICKLIST_TABLE_NAME}.name IN ('Stage-1','Stage-2') order by name desc`
            let stage1_sfid_res = await client.query(stage1_sfid);
            let stage_1 = stage1_sfid_res.rows[0]['sfid'];
            let stage_2 = stage1_sfid_res.rows[1]['sfid'];


            //Inactive Reason (Wrong and Invalid number) Picklist
            let inactivereaon_sfid = `Select sfid from ${process.env.TABLE_SCHEMA_NAME}.${SF_PICKLIST_TABLE_NAME} WHERE ${SF_PICKLIST_TABLE_NAME}.object_name__c='Lead__c' AND ${SF_PICKLIST_TABLE_NAME}.field_name__c= 'Inactive_Reason__c'AND ${SF_PICKLIST_TABLE_NAME}.name IN ('Wrong number','Invalid number') order by name desc`
            let inactivereaon_sfid_res = await client.query(inactivereaon_sfid);
            let inactive_reason_wrong_no = inactivereaon_sfid_res.rows[0]['sfid'];
            let inactive_reason_invalid_no = inactivereaon_sfid_res.rows[1]['sfid'];

            //Site fir Picklist
            let site_fir = `Select sfid from ${process.env.TABLE_SCHEMA_NAME}.${SF_PICKLIST_TABLE_NAME} WHERE ${SF_PICKLIST_TABLE_NAME}.object_name__c='Lead__c' AND ${SF_PICKLIST_TABLE_NAME}.field_name__c= 'Site_FIR__c'AND ${SF_PICKLIST_TABLE_NAME}.name IN('Lead','Cold Lead') order by name desc`
            let site_fir_res = await client.query(site_fir);
            let lead_pkl = site_fir_res.rows[0]['sfid'];
            let cold_lead_pkl = site_fir_res.rows[1]['sfid'];
            console.log('lead::::::::',lead_pkl,'cold_lead_pkl:::::::::::::::',cold_lead_pkl)

            let lead_sql = `SELECT * FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_LEAD_TABLE_NAME} where site_status__c != '${won_ongoing_picklist}' and lead_open_closed__c = '${Open_sfid_res}' and sfid is not null AND site_fir__c IN ('${lead_pkl}','${cold_lead_pkl}')`
            //let lead_sql = `SELECT * FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_LEAD_TABLE_NAME} where sfid = 'a0F0w000001QMqUEAW'`
            console.log('Lead SQL -----> ',lead_sql);
            let lead_sql_res = await client.query(lead_sql);
    
            let today_date = new Date();
            today_date = moment(today_date).format('YYYY-MM-DD');
    
            if(lead_sql_res.rows.length > 0){
                for(let i = 0 ; i < lead_sql_res.rows.length ; i++){   
                    let statement_to_run = 0;

                    let contact_on_lead_sql = `SELECT * FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_CONTACT_TABLE_NAME} where lead__c = '${lead_sql_res.rows[i]['sfid']}' AND contact_type__c = '${owner_sfid_res}'`
                    let contact_on_lead_sql_res = await client.query(contact_on_lead_sql);
                    if(contact_on_lead_sql_res.rows.length > 0){ 
                        statement_to_run = 1;
                    }
                    console.log(`Iteration :::::::  ${i}`);
                    let lead_sfid = lead_sql_res.rows[i]['sfid'];
                    console.log('Lead Sfid -------> ',lead_sfid);
                    
                    let lead_stage = lead_sql_res.rows[i]['stage__c'];
                    console.log('Lead Stage --------> ',lead_stage);
    
                    let owner_number__c = lead_sql_res.rows[i]['owner_number__c'];
                    console.log('Owner Number --------> ',owner_number__c);
                    
                    let lead_created_date = dtUtil.ISOtoLocal(lead_sql_res.rows[i]['lead_created_date__c']);
                    console.log('Lead Created Date ----> ',lead_created_date);
    
                    const date1 = new Date(`'${today_date}'`);
                    const date2 = new Date(`'${lead_created_date}'`);
                    console.log('--->',date1,date2);
                    const diffTime = Math.abs(date2 - date1);
                    const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24));
                    console.log('Diff Days ---->',diffDays,diffTime); 
                  let value_parameterization_sql = `SELECT * FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_VALUE_PARAMETERIZATION_TABLE_NAME} where parameterized_value__c IN ('365',180,'30','90') order by parameterized_value__c desc`
                  let value_parameterization_sql_res = await client.query(value_parameterization_sql);

                  if (value_parameterization_sql_res.rows.length > 0) {
                    three_sixty_five = value_parameterization_sql_res.rows[0]['parameterized_value__c']
                    one_eighty = value_parameterization_sql_res.rows[1]['parameterized_value__c']
                    thirty_days = value_parameterization_sql_res.rows[3]['parameterized_value__c']
                    ninety = value_parameterization_sql_res.rows[2]['parameterized_value__c']
                  } else {
                    three_sixty_five = 365;
                    one_eighty=180;
                    thirty_days=30;
                    ninety=90;

                  }
                  console.log('three_sixty_five:::',three_sixty_five,'one_eighty:::::',one_eighty,'thirty_days::::',thirty_days,'ninety::::::',ninety)
                  //Inactive Picklist
                  let Inactive_sfid_pic = `Select sfid from ${process.env.TABLE_SCHEMA_NAME}.${SF_PICKLIST_TABLE_NAME} WHERE ${SF_PICKLIST_TABLE_NAME}.object_name__c='Lead__c' AND ${SF_PICKLIST_TABLE_NAME}.field_name__c= 'Active_Status__c'AND ${SF_PICKLIST_TABLE_NAME}.name = 'InActive'`
                  let Inactive_sfid_pic_res = await client.query(Inactive_sfid_pic);
                  Inactive_sfid_pic_res = Inactive_sfid_pic_res.rows[0]['sfid'];
                  console.log('Inactive_sfid_res:::::::::', Inactive_sfid_pic_res)
                    if(diffDays > three_sixty_five){
                        console.log('Case ::: Diff Days greater than 360');
                        let update_sql = `update ${process.env.TABLE_SCHEMA_NAME}.${SF_LEAD_TABLE_NAME} set site_status__c='${inactive_picklist}',inactive__c='true',active_status__c='${Inactive_sfid_pic_res}',live_lead__c = 'false' where sfid = '${lead_sfid}'`
                        let update_res = await client.query(update_sql)
                        console.log('Update sql ---->',update_sql);
                        //console.log('Update Sql Res ---->',update_res);
                        let task_close_sfid = `Select sfid from ${process.env.TABLE_SCHEMA_NAME}.${SF_PICKLIST_TABLE_NAME} WHERE ${SF_PICKLIST_TABLE_NAME}.object_name__c='Task__c' AND ${SF_PICKLIST_TABLE_NAME}.field_name__c= 'Task_Status__c'AND ${SF_PICKLIST_TABLE_NAME}.name = 'Closed'`
                        let task_close_sfid_res = await client.query(task_close_sfid);
                        task_close_sfid_res = task_close_sfid_res.rows[0]['sfid'];
                        console.log('close task picklist sql:::::::', task_close_sfid_res)
                        let task_of_lead_sql = `SELECT sfid FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_TASK_TABLE_NAME} Where lead__c = '${lead_sfid}'`
                        let result = await client.query(task_of_lead_sql)
                        let task_sfids_arr = []
                        if (result.rows.length > 0) {
                          result.rows.map((id) => {
                            task_sfids_arr.push(id['sfid'])
                          })
                          let update_task = `update ${process.env.TABLE_SCHEMA_NAME}.${SF_TASK_TABLE_NAME} 
                                                      set task_status__c= '${task_close_sfid_res}',task_outcome__c = null 
                                                      where sfid IN ('${task_sfids_arr.join("','")}')`
                          let response = await client.query(update_task)
                          console.log(`update_task ----> ${update_task}`);
                          console.log('response sql:::::::',response)
                        }
                    }
                    
                    if(diffDays > one_eighty && (lead_stage == stage_1 || lead_stage == stage_2)){   //Stage 1 , Stage 2
                        console.log('Case ::: Diff Days Greater Than !80 And Stage is 1,2');
                        let update_sql = `update ${process.env.TABLE_SCHEMA_NAME}.${SF_LEAD_TABLE_NAME} set site_status__c='${inactive_picklist}',inactive__c='true',live_lead__c = 'false', active_status__c='${Inactive_sfid_pic_res}' where sfid = '${lead_sfid}'`
                        let update_res = await client.query(update_sql)
                        console.log('Update sql ---->',update_sql);
                        //console.log('Update Sql Res ---->',update_res);
                        let task_close_sfid = `Select sfid from ${process.env.TABLE_SCHEMA_NAME}.${SF_PICKLIST_TABLE_NAME} WHERE ${SF_PICKLIST_TABLE_NAME}.object_name__c='Task__c' AND ${SF_PICKLIST_TABLE_NAME}.field_name__c= 'Task_Status__c'AND ${SF_PICKLIST_TABLE_NAME}.name = 'Closed'`
                        let task_close_sfid_res = await client.query(task_close_sfid);
                        task_close_sfid_res = task_close_sfid_res.rows[0]['sfid'];
                        console.log('close task picklist sql:::::::', task_close_sfid_res)
                        let task_of_lead_sql = `SELECT sfid FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_TASK_TABLE_NAME} Where lead__c = '${lead_sfid}'`
                        let result = await client.query(task_of_lead_sql)
                        let task_sfids_arr = []
                        if (result.rows.length > 0) {
                          result.rows.map((id) => {
                            task_sfids_arr.push(id['sfid'])
                          })
                          let update_task = `update ${process.env.TABLE_SCHEMA_NAME}.${SF_TASK_TABLE_NAME} 
                                                      set task_status__c= '${task_close_sfid_res}',task_outcome__c = null 
                                                      where sfid IN ('${task_sfids_arr.join("','")}')`
                          let response = await client.query(update_task)
                          console.log(`Update Task In Inactive ----> ${response}`);
                        }
                    }
                    
                    if(statement_to_run == 1){
                      if(diffDays >= thirty_days && contact_on_lead_sql_res.rows[0]['mobilephone'] == null ){
                        console.log('Case ::: Diff Days Is greater Than 30 and Owner Number Is Null');
                        let update_sql = `update ${process.env.TABLE_SCHEMA_NAME}.${SF_LEAD_TABLE_NAME} set site_status__c='${inactive_picklist}',inactive__c='true',live_lead__c = 'false',inactive_reason__c = '${inactive_reason_wrong_no}' where sfid = '${lead_sfid}'`
                        let update_res = await client.query(update_sql)
                        console.log('Update sql ---->',update_sql);
                        //console.log('Update Sql Res ---->',update_res);
                        let task_close_sfid = `Select sfid from ${process.env.TABLE_SCHEMA_NAME}.${SF_PICKLIST_TABLE_NAME} WHERE ${SF_PICKLIST_TABLE_NAME}.object_name__c='Task__c' AND ${SF_PICKLIST_TABLE_NAME}.field_name__c= 'Task_Status__c'AND ${SF_PICKLIST_TABLE_NAME}.name = 'Closed'`
                        let task_close_sfid_res = await client.query(task_close_sfid);
                        task_close_sfid_res = task_close_sfid_res.rows[0]['sfid'];
                        console.log('close task picklist sql:::::::', task_close_sfid_res)
                        let task_of_lead_sql = `SELECT sfid FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_TASK_TABLE_NAME} Where lead__c = '${lead_sfid}'`
                        let result = await client.query(task_of_lead_sql)
                        let task_sfids_arr = []
                        if (result.rows.length > 0) {
                          result.rows.map((id) => {
                            task_sfids_arr.push(id['sfid'])
                          })
                          let update_task = `update ${process.env.TABLE_SCHEMA_NAME}.${SF_TASK_TABLE_NAME} 
                                                      set task_status__c= '${task_close_sfid_res}',task_outcome__c = null 
                                                      where sfid IN ('${task_sfids_arr.join("','")}')`
                          let response = await client.query(update_task)
                          console.log(`update_task ----> ${update_task}`);
                          console.log('response sql:::::::',response)
                        }
                      }
                      if(diffDays >= thirty_days && contact_on_lead_sql_res.rows[0]['mobilephone'] != null && lead_sql_res.rows[i]['invalid_phone_number__c'] == true ){
                        console.log('Case ::: Diff Days Is greater Than 30 and Owner Number Is Not Null And The No. Is Invalid');
                        let update_sql = `update ${process.env.TABLE_SCHEMA_NAME}.${SF_LEAD_TABLE_NAME} set site_status__c='${inactive_picklist}',inactive__c='true',active_status__c='${Inactive_sfid_pic_res}',live_lead__c = 'false',inactive_reason__c = '${inactive_reason_invalid_no}' where sfid = '${lead_sfid}'`
                        let update_res = await client.query(update_sql)
                        console.log('Update sql ---->',update_sql);
                        //console.log('Update Sql Res ---->',update_res);
                        let task_close_sfid = `Select sfid from ${process.env.TABLE_SCHEMA_NAME}.${SF_PICKLIST_TABLE_NAME} WHERE ${SF_PICKLIST_TABLE_NAME}.object_name__c='Task__c' AND ${SF_PICKLIST_TABLE_NAME}.field_name__c= 'Task_Status__c'AND ${SF_PICKLIST_TABLE_NAME}.name = 'Closed'`
                        let task_close_sfid_res = await client.query(task_close_sfid);
                        task_close_sfid_res = task_close_sfid_res.rows[0]['sfid'];
                        console.log('close task picklist sql:::::::', task_close_sfid_res)
                        let task_of_lead_sql = `SELECT sfid FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_TASK_TABLE_NAME} Where lead__c = '${lead_sfid}'`
                        let result = await client.query(task_of_lead_sql)
                        let task_sfids_arr = []
                        if (result.rows.length > 0) {
                          result.rows.map((id) => {
                            task_sfids_arr.push(id['sfid'])
                          })
                          let update_task = `update ${process.env.TABLE_SCHEMA_NAME}.${SF_TASK_TABLE_NAME} 
                                                      set task_status__c= '${task_close_sfid_res}',task_outcome__c = null 
                                                      where sfid IN ('${task_sfids_arr.join("','")}')`
                          let response = await client.query(update_task)
                          console.log(`update_task ----> ${update_task}`);
                          console.log('response sql:::::::',response)
                        } 
                      }
                    }
                }

                console.log('::::::::::: Cron Ended For Open Lead Other Than Won And Ongoing ::::::::::::::');
            }
            console.log('::::::::::: Cron Ended For Open Lead Other Than Won And Ongoing (No record) ::::::::::::::');

    
            //2nd Point For Won And Ongoing Lead
            console.log('2nd Point For Won And Ongoing Lead     .......................');
            let lead_sql_second_case= `SELECT * FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_LEAD_TABLE_NAME} where site_status__c = '${won_ongoing_picklist}'`
            console.log('Lead SQL 2nd Point For Won And Ongoing Lead -----> ',lead_sql_second_case);
            let lead_sql_second_case_res = await client.query(lead_sql_second_case);
    
            if(lead_sql_second_case_res.rows.length > 0){
                for(let i = 0 ; i < lead_sql_second_case_res.rows.length ; i++ ){
                    let lead_sfid = lead_sql_second_case_res.rows[i]['sfid'];
                    console.log('Lead Sfid -------> ',lead_sfid);
                    
                    let lead_created_date = dtUtil.ISOtoLocal(lead_sql_second_case_res.rows[i]['lead_created_date__c']);
                    console.log('Lead Created Date ----> ',lead_created_date);
    
                    //let supply_false_count = await suppleDetailsCount(lead_sfid);
                    //console.log('supply_false_count Values --->',supply_false_count.last_supply_false_count);
    
                    let supply_detail_sql = `SELECT * FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_SUPPLY_TABLE_NAME} where lead__c = '${lead_sfid}' order by supply_date__c desc`;
                    let supply_detail_sql_res = await client.query(supply_detail_sql)
    
                    if(supply_detail_sql_res.rows.length > 0){
                        let supply_date = dtUtil.ISOtoLocal(supply_detail_sql_res.rows[0]['supply_date__c']);
                        let first_supply = supply_detail_sql_res.rows[0]['first_supply__c'];
                        let last_supply = supply_detail_sql_res.rows[0]['last_supply__c'];
                        let going_on_supply = supply_detail_sql_res.rows[0]['on_going_supply__c'];
    
                        const supply_date1 = new Date(`'${supply_date}'`);
                        const lead_created_date2 = new Date(`'${lead_created_date}'`);
                        const today_date=new Date();
                        //today_date = moment(today_date).format("YYYY-MM-DD");
                        console.log('--->',supply_date1,today_date);
                        const diffTime = Math.abs(today_date - supply_date1);
                        const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24));
                        console.log('Diff Days ---->',diffDays,diffTime); 
    
                        if(diffDays > ninety ){  //For Won And Completed
                          //if(diffDays > 90 && (first_supply == true || going_on_supply == true)){  //For Won And Completed
                            let update_sql = `update ${process.env.TABLE_SCHEMA_NAME}.${SF_LEAD_TABLE_NAME} set site_status__c='${won_completed_picklist}' where sfid = '${lead_sfid}'`
                            let update_sql_res = await client.query(update_sql);
                            console.log('Update sql In Case 2 -------> ',update_sql);
                        }   
                    }
    
                }
                console.log('::::::::::: Cron Ended For Won And Ongoing And No Last Supply In 90 Days ::::::::::::::');
            }
            console.log('::::::::::: Cron Ended For Won And Ongoing And No Last Supply In 90 Days (No record) ::::::::::::::');
    
        }catch(e){
          let error_log = `Error In Lead Cron Job leadStatusOnDateAndStage ----->${e}`
          console.log(error_log);
          func.mailErrorLog(error_log, req.route['path'], req.body, req.query, req.headers.token, req.headers['client-name']);
          response.response = { success: false, message: 'Internal Server error.' };
          response.status = 500;
          return response;
        }
    },
    { scheduled: true }
)

const autoTask_and_NotificationForOwnerNo=cron.schedule(`46 11 * * *`,  //Everyday At 12:30 AM
async ()=>{
    try{    
        let today = dtUtil.todayDate();
        let checkDate=dtUtil.addDays(today,-15)
        const taskDate=dtUtil.addDays(today,1)
        let datentime=dtUtil.todayDatetime()
        // get all the lead where lcd is 14 days before
        let findEMDSql=`SELECT lead__c.sfid,lead__c.expected_maturity_date__c,lead__c.name as leadname,lead__c.lead_assignment__c,team_territory_mapping__c.team_member_id__c,contact.Contact_Type__c FROM salesforce.lead__c
        LEFT JOIN salesforce.team_territory_mapping__c ON team_territory_mapping__c.territory_code__c=lead__c.territory_name__c
        LEFT JOIN  salesforce.team__c ON team__c.sfid=team_territory_mapping__c.team_member_id__c
        LEFT JOIN salesforce.contact ON contact.lead__c=lead__c.sfid 
        WHERE lead__c.lead_created_date__c < '${checkDate}' AND lead__c.sfid is not NUll AND team_territory_mapping__c.sfid is not NULL and contact.contact_type__c is not null and lead__c.expected_maturity_date__c is not null`
        //AND contact.Contact_Type__c!='a050w000003RBksAAG'

        console.log("findEMDSql",findEMDSql)
        let findEMDSqlResult=await client.query(findEMDSql)
        let mapObj={}
        let mapObj2={}
        // to sort the data
        // map to store all sfid
        findEMDSqlResult.rows.map((obj)=>{
          mapObj[obj.sfid]=obj
          mapObj2[`'${obj.sfid}'`]=obj.sfid
        })
        // get the picklist of contact type owner
        let contact_picklist_sql = `SELECT * FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_PICKLIST_TABLE_NAME} 
        WHERE ${SF_PICKLIST_TABLE_NAME}.object_name__c='Contact' AND ${SF_PICKLIST_TABLE_NAME}.field_name__c= 'Contact_Type__c' AND ${SF_PICKLIST_TABLE_NAME}.name= 'Owner'`;
        let contact_picklist = await client.query(contact_picklist_sql);
        contact_picklist = contact_picklist.rows[0]['sfid'];
        //console.log('contact_picklist:::::::::::',contact_picklist)
        //sql to get which lead have owner number
        let findWithoutNumberSql=`SELECT lead__c FROM salesforce.contact WHERE Contact_Type__c='${contact_picklist}' AND lead__c IN (${Object.keys(mapObj2)})`
        console.log("findWithoutNumberSql",findWithoutNumberSql)
        
        let findWithoutNumberResult=await client.query(findWithoutNumberSql)
        // removed sfid who have owner number
        for (let j = 0; j < findWithoutNumberResult.rows.length; j++) {
          const element = await findWithoutNumberResult.rows[j];
          if(element.lead__c in mapObj){
            delete mapObj[element.lead__c]
           }
        }
        console.log("mapObj_____________________________________________________>",mapObj)
        if(Object.values(mapObj).length>0){

            for (let i = 0; i < Object.values(mapObj).length; i++) {
                const element = Object.values(mapObj)[i];
                
                console.log("element",element)
                let token_sql=`SELECT team_member_name__c,token__c FROM salesforce.team__c WHERE sfid='${element.lead_assignment__c}'`
                let token_res=await client.query(token_sql)
                console.log("token_res",token_res.rows[0])
                if(token_res.rows.length>0 && token_res.rows[0].token__c!=null){
                  let title=`Didn't get lead owner number yet`
                  let body=`Hi ${token_res.rows[0].team_member_name__c},still we din't get onwer number of LEAD ${element.leadname}`
                  let firebase_token__c=token_res.rows[0].token__c
                  let user__c=element.lead_assignment__c
                  let notificationLog=await notification.sendnotificationToDevice({user__c, firebase_token__c,title,body},{title,body})
                  console.log(notificationLog)
                }
                let lead_status_on_date = await validation.leadStatusOnDate(element.expected_maturity_date__c) // check status and below validate
                console.log("lead_status_on_date",lead_status_on_date)
                let site_picklist_sql = `SELECT * FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_PICKLIST_TABLE_NAME} 
                  WHERE ${SF_PICKLIST_TABLE_NAME}.object_name__c='Site_Status_master__c' AND ${SF_PICKLIST_TABLE_NAME}.field_name__c ='Site_Status__c' AND  ${SF_PICKLIST_TABLE_NAME}.name IN ('Hot Hot','Hot','Warm','Cold')  order by name desc`;
                let site_picklist = await client.query(site_picklist_sql);
                let hot_hot = site_picklist.rows[0]['sfid'];
                let hot = site_picklist.rows[1]['sfid'];
                let warm = site_picklist.rows[2]['sfid'];
                let cold = site_picklist.rows[3]['sfid'];
                    //console.log('site_picklist:::::::::::::::::::::::',site_picklist1,site_picklist2,site_picklist3,site_picklist4)
                if(lead_status_on_date.status_id == hot_hot|| lead_status_on_date.status_id == hot || lead_status_on_date.status_id == warm || lead_status_on_date.status_id== cold){
                 console.log("i came here")
                  let pg_id__cAT=uuidv4()
                  let naturetask_picklist_sql = `SELECT * FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_PICKLIST_TABLE_NAME} 
                    WHERE ${SF_PICKLIST_TABLE_NAME}.object_name__c='Task__c' AND ${SF_PICKLIST_TABLE_NAME}.field_name__c ='Mode_Of_Clousre__c' AND  ${SF_PICKLIST_TABLE_NAME}.name='Phone Call'`;
                  let naturetask_picklist = await client.query(naturetask_picklist_sql);
                      naturetask_picklist = naturetask_picklist.rows[0]['sfid'];
                      //console.log('naturetask_picklist:::::::::::::::::::::',naturetask_picklist)
                    const natureOfTask = naturetask_picklist;
                    let task_type_sql = `SELECT * FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_PICKLIST_TABLE_NAME} 
                       WHERE ${SF_PICKLIST_TABLE_NAME}.object_name__c='Task__c' AND ${SF_PICKLIST_TABLE_NAME}.field_name__c ='Task_Type__c' AND  ${SF_PICKLIST_TABLE_NAME}.name='Get owner phone no'`;
                    let task_type = await client.query(task_type_sql);
                        task_type = task_type.rows[0]['sfid'];
                      //console.log('task_type:::::::::::::::::::::',task_type)
                    let task_status_sql = `SELECT * FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_PICKLIST_TABLE_NAME} 
                      WHERE ${SF_PICKLIST_TABLE_NAME}.object_name__c='Task__c' AND ${SF_PICKLIST_TABLE_NAME}.field_name__c ='Task_Status__c' AND  ${SF_PICKLIST_TABLE_NAME}.name='Open'`;
                    let task_status = await client.query(task_status_sql);
                       task_status = task_status.rows[0]['sfid'];
                      console.log('task_status:::::::::::::::::::::',task_status)

                    const fieldtobeinsertedForAutoTask = [
                        `pg_id__c, 
                        createddate,
                        task_date__c,
                        task_owner__c,
                        task_type__c,
                        task_assigned_to__c,
                        task_creation_remarks__c,
                        lead__c,
                        task_status__c,
                        nature_of_task__c`
                    ];
                    const fieldValueForAutoTask = [
                        pg_id__cAT,
                        datentime,
                        taskDate,
                        element.team_member_id__c,
                        task_type,
                        element.team_member_id__c,
                        'Auto Task get owner number',
                        element.sfid,
                        task_status,
                        natureOfTask,            
                    ];
                    let natureTaskSql=await qry.insertRecord(fieldtobeinsertedForAutoTask, fieldValueForAutoTask, SF_TASK_TABLE_NAME);
                    if(natureTaskSql.success){
                        console.log("1 auto task created")
                    }

                }
                                
            }
        }
    
    }catch(e){
      let error_log = `Error In Lead Cron Job autoTask_and_NotificationForOwnerNo ----->${e}`
      console.log(error_log);
      func.mailErrorLog(error_log, req.route['path'], req.body, req.query, req.headers.token, req.headers['client-name']);
      response.response = { success: false, message: 'Internal Server error.' };
      response.status = 500;
      return response;
    }
    },{ scheduled: true })

const AutoTaskFirstSupply = cron.schedule(
        `30 01 * * *`, //for Everyday At 1:30 Am In Night
        async () => {
          try {
            //All leads with site_status Hot_hot

            let  callcentereSfid;
            let hot_hot_sfid = `Select sfid from ${process.env.TABLE_SCHEMA_NAME}.${SF_PICKLIST_TABLE_NAME} WHERE ${SF_PICKLIST_TABLE_NAME}.object_name__c='Site_Status_master__c' AND ${SF_PICKLIST_TABLE_NAME}.field_name__c= 'Site_Status__c'AND ${SF_PICKLIST_TABLE_NAME}.name = 'Hot Hot'`
            let hot_hot_sfid_res = await client.query(hot_hot_sfid);
            hot_hot_sfid_res = hot_hot_sfid_res.rows[0]['sfid'];

            //Task_type picklist
            let task_sfid = `Select sfid from ${process.env.TABLE_SCHEMA_NAME}.${SF_PICKLIST_TABLE_NAME} WHERE ${SF_PICKLIST_TABLE_NAME}.object_name__c='Task__c' AND ${SF_PICKLIST_TABLE_NAME}.field_name__c= 'Task_Type__c'AND ${SF_PICKLIST_TABLE_NAME}.name = 'Confirm Delivery ( First Supply)'`
            let task_sfid_res = await client.query(task_sfid);
            task_sfid_res = task_sfid_res.rows[0]['sfid'];

            //Nature Task picklist
            let nature_task_sfid = `Select sfid from ${process.env.TABLE_SCHEMA_NAME}.${SF_PICKLIST_TABLE_NAME} WHERE ${SF_PICKLIST_TABLE_NAME}.object_name__c='Task__c' AND ${SF_PICKLIST_TABLE_NAME}.field_name__c= 'Mode_Of_Clousre__c'AND ${SF_PICKLIST_TABLE_NAME}.name = 'Phone Call'`
            let nature_task_sfid_res = await client.query(nature_task_sfid);
            nature_task_sfid_res = nature_task_sfid_res.rows[0]['sfid'];

            //task status picklist
            let task_status_sfid = `Select sfid from ${process.env.TABLE_SCHEMA_NAME}.${SF_PICKLIST_TABLE_NAME} WHERE ${SF_PICKLIST_TABLE_NAME}.object_name__c='Task__c' AND ${SF_PICKLIST_TABLE_NAME}.field_name__c= 'Task_Status__c'AND ${SF_PICKLIST_TABLE_NAME}.name = 'Open'`
            let task_status_sfid_res = await client.query(task_status_sfid);
            task_status_sfid_res = task_status_sfid_res.rows[0]['sfid'];

            // task_owner = 'a030w0000082Z09AAE';
            let all_lead_sql = `SELECT * FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_LEAD_TABLE_NAME} where sfid is NOT NULL and site_status__c = '${hot_hot_sfid_res}' and conversion_verified__c = 'true'`;
      
            // console.log("selecting all leads for Hot_hot", all_lead_sql);
            let all_lead_sql_res = await client.query(all_lead_sql);

            if (all_lead_sql_res.rows.length > 0) {
              for (let i = 0; i < all_lead_sql_res.rows.length; i++) {
                let lead_sfid = all_lead_sql_res.rows[i]["sfid"];
                // console.log("lead sfid========", lead_sfid);
      
                if(all_lead_sql_res.rows[i]['town_name__c'] != null){
                  console.log("INSIDE")
                  callcentereSfid=await func.getCCFromTown(all_lead_sql_res.rows[i]['town_name__c'])
                }
                let supply = `SELECT * FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_SUPPLY_TABLE_NAME} where lead__c = '${all_lead_sql_res.rows[i]["sfid"]}' and first_supply__c = true  order by id`;
                let value_res = await client.query(supply);
      
                if (value_res.rows.length > 0) {
                  if (
                    all_lead_sql_res.rows[i]["conversion_verified__c"] == true &&
                    value_res.rows[0]["supplying_dealer_verified__c"] == true &&
                    value_res.rows[0]["supply_validated__c"] == true
                  ) {
                    let task_sql = `Select * from salesforce.task__c where lead__c = '${all_lead_sql_res.rows[i]["sfid"]}' and task_creation_remarks__c = 'Auto Task for Confirm Delivery(First Supply)'`;
                    let task_sql_res = await client.query(task_sql);
                    if (task_sql_res.rows.length > 0) {
                      console.log("Task is already there");
                    } else {
                      let pg_id__c = uuidv4();
                      let createddate = dtUtil.todayDatetime();
                      let today = dtUtil.todayDate();
                      const table_name = SF_TASK_TABLE_NAME;
      
                      const fieldtobeinserted = [
                        `pg_id__c, 
                                  createddate,
                                  task_date__c,
                                  task_owner__c,
                                  task_type__c,
                                  task_assigned_to__c,
                                  task_creation_remarks__c,
                                  lead__c,
                                  nature_of_task__c,
                                  task_status__c`,
                      ];
                      const fieldValue = [
                        pg_id__c,
                        createddate,
                        today,
                        callcentereSfid,
                        task_sfid_res,
                        callcentereSfid,
                        "Auto Task for Confirm Delivery(First Supply)",
                        lead_sfid,
                        nature_task_sfid_res,
                        task_status_sfid_res,
                      ];
      
                      let create_task_batch_sql = await qry.insertRecord(
                        fieldtobeinserted,
                        fieldValue,
                        table_name
                      );
                    }
                  }
                }
              }
              console.log("Cronjob ended for autoTask Confirm DElivery First Supply");
            }
          } catch (e) {
            let error_log = `Error In Lead Cron Job AutoTaskFirstSupply ----->${e}`
            console.log(error_log);
            func.mailErrorLog(error_log, req.route['path'], req.body, req.query, req.headers.token, req.headers['client-name']);
            response.response = { success: false, message: 'Internal Server error.' };
            response.status = 500;
            return response;
          }
        },
        { scheduled: true }
);
  
const AutoTaskFurtherSupply = cron.schedule(
        `40 01 * * *`, //for Everyday At 1:35 Am In Night
        async () => {
          try {
            //All leads with site_status Hot_hot

            let callcentereSfid;
            let hot_hot_sfid = `Select sfid from ${process.env.TABLE_SCHEMA_NAME}.${SF_PICKLIST_TABLE_NAME} WHERE ${SF_PICKLIST_TABLE_NAME}.object_name__c='Site_Status_master__c' AND ${SF_PICKLIST_TABLE_NAME}.field_name__c= 'Site_Status__c'AND ${SF_PICKLIST_TABLE_NAME}.name = 'Hot Hot'`
            let hot_hot_sfid_res = await client.query(hot_hot_sfid);
            hot_hot_sfid_res = hot_hot_sfid_res.rows[0]['sfid'];

            //Task_type picklist
            let task_sfid = `Select sfid from ${process.env.TABLE_SCHEMA_NAME}.${SF_PICKLIST_TABLE_NAME} WHERE ${SF_PICKLIST_TABLE_NAME}.object_name__c='Task__c' AND ${SF_PICKLIST_TABLE_NAME}.field_name__c= 'Task_Type__c'AND ${SF_PICKLIST_TABLE_NAME}.name = 'Confirm Delivery ( Further Supply)'`
            let task_sfid_res = await client.query(task_sfid);
            task_sfid_res = task_sfid_res.rows[0]['sfid'];

            //Nature Task picklist
            let nature_task_sfid = `Select sfid from ${process.env.TABLE_SCHEMA_NAME}.${SF_PICKLIST_TABLE_NAME} WHERE ${SF_PICKLIST_TABLE_NAME}.object_name__c='Task__c' AND ${SF_PICKLIST_TABLE_NAME}.field_name__c= 'Mode_Of_Clousre__c'AND ${SF_PICKLIST_TABLE_NAME}.name = 'Phone Call'`
            let nature_task_sfid_res = await client.query(nature_task_sfid);
            nature_task_sfid_res = nature_task_sfid_res.rows[0]['sfid'];

            //task status picklist
            let task_status_sfid = `Select sfid from ${process.env.TABLE_SCHEMA_NAME}.${SF_PICKLIST_TABLE_NAME} WHERE ${SF_PICKLIST_TABLE_NAME}.object_name__c='Task__c' AND ${SF_PICKLIST_TABLE_NAME}.field_name__c= 'Task_Status__c'AND ${SF_PICKLIST_TABLE_NAME}.name = 'Open'`
            let task_status_sfid_res = await client.query(task_status_sfid);
            task_status_sfid_res = task_status_sfid_res.rows[0]['sfid'];

            // task_owner = 'a030w0000082Z09AAE';

            let all_lead_sql = `SELECT * FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_LEAD_TABLE_NAME} where sfid is NOT NULL and site_status__c = '${hot_hot_sfid_res}'`;
      
            // console.log("selecting all leads for Hot_hot");
            let all_lead_sql_res = await client.query(all_lead_sql);
      
            if (all_lead_sql_res.rows.length > 0) {
              for (let i = 0; i < all_lead_sql_res.rows.length; i++) {
                // let town_sql = `Select * from salesforce.area1__c where sfid = '${all_lead_sql_res.rows[i]['town_name__c']}'`
                // // console.log("town_sql++++++", town_sql)
                // let town_sql_res = await client.query(town_sql);
      
                // let ter_sql = `Select * from salesforce.area1__c where sfid = '${town_sql_res.rows[0]['parent_code__c']}'`
                // // console.log("ter_sql++++++", ter_sql)
                // let ter_sql_res = await client.query(ter_sql);
      
                // let se_sql = `Select * from salesforce.area1__c where sfid = '${ter_sql_res.rows[0]['parent_code__c']}'`
                // // console.log("se_sql++++++", se_sql)
                // let se_sql_res = await client.query(se_sql);
      
                // let asm_sql = `Select * from salesforce.area1__c where sfid = '${se_sql_res.rows[0]['parent_code__c']}'`
                // // console.log("se_sql++++++", se_sql)
                // let asm_sql_res = await client.query(asm_sql);
      
                // let branch_sql = `Select * from salesforce.area1__c where sfid = '${asm_sql_res.rows[0]['parent_code__c']}'`
                // let branch_sql_res = await client.query(branch_sql);
                // console.log("Branch_res",branch_sql_res.rows)
      
                //*************************************  IF Branch is PV Branch OR Non-PV ************************************************************ */
                // if((branch_sql_res.rows[0]['branch_type__c'] == 'a050w000002jNn7AAE' || branch_sql_res.rows[0]['branch_type__c'] == 'a050w000002jNn8AAE') && branch_sql_res.rows[0]['area_type__c'] == 'a050w000002jNn3AAE'){
                
                if(all_lead_sql_res.rows[i]['town_name__c'] != null){
                  console.log("INSIDE")
                  callcentereSfid=await func.getCCFromTown(all_lead_sql_res.rows[i]['town_name__c'])
                }
      
                let supply = `SELECT * FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_SUPPLY_TABLE_NAME} where lead__c = '${all_lead_sql_res.rows[i]["sfid"]}'`;
                let value_res = await client.query(supply);
      
                let lead_sfid = all_lead_sql_res.rows[i]["sfid"];
                // console.log("++++++++++++++++++++", lead_sfid);
      
                if (value_res.rows.length > 0) {
                  if (
                    all_lead_sql_res.rows[i]["conversion_verified__c"] == true &&
                    value_res.rows[0]["supplying_dealer_verified__c"] == true &&
                    value_res.rows[0]["supply_validated__c"] == true &&
                    value_res.rows[0]["on_going_supply__c"] == true
                  ) {
                    let task_sql = `Select * from salesforce.task__c where lead__c = '${all_lead_sql_res.rows[i]["sfid"]}' and task_creation_remarks__c = 'Auto Task for Confirm Delivery(Further Supply)'`;
                    let task_sql_res = await client.query(task_sql);
      
                    if (task_sql_res.rows.length > 0) {
                      console.log("Task is already there");
                    } else {
                      let pg_id__c = uuidv4();
                      let createddate = dtUtil.todayDatetime();
                      let today = dtUtil.todayDate();
                      const table_name = SF_TASK_TABLE_NAME;
      
                      const fieldtobeinserted = [
                        `pg_id__c, 
                                  createddate,
                                  task_date__c,
                                  task_owner__c,
                                  task_type__c,
                                  task_assigned_to__c,
                                  task_creation_remarks__c,
                                  lead__c,
                                  nature_of_task__c,
                                  task_status__c`,
                      ];
                      const fieldValue = [
                        pg_id__c,
                        createddate,
                        today,
                        callcentereSfid,
                        task_sfid_res,
                        callcentereSfid,
                        "Auto Task for Confirm Delivery(Further Supply)",
                        lead_sfid,
                        nature_task_sfid_res,
                        task_status_sfid_res,
                      ];
      
                      let create_task_batch_sql = await qry.insertRecord(
                        fieldtobeinserted,
                        fieldValue,
                        table_name
                      );
      
                    }
                  }
                }
      
              }
              console.log("Cronjob ended for autoTask Confirm DElivery First Supply");
            }
          } catch (e) {
            let error_log = `Error In Lead Cron Job AutoTaskFurtherSupply ----->${e}`
            console.log(error_log);
            func.mailErrorLog(error_log, req.route['path'], req.body, req.query, req.headers.token, req.headers['client-name']);
            response.response = { success: false, message: 'Internal Server error.' };
            response.status = 500;
            return response;
          }
        },
        { scheduled: true }
);
const AutoTaskValidateEMD = cron.schedule(
        `50 01 * * *`, //for Everyday At 1 Am In Night
        async () => {
          try {

            let callcentereSfid;
            let hot_hot_sfid = `Select sfid from ${process.env.TABLE_SCHEMA_NAME}.${SF_PICKLIST_TABLE_NAME} WHERE ${SF_PICKLIST_TABLE_NAME}.object_name__c='Site_Status_master__c' AND ${SF_PICKLIST_TABLE_NAME}.field_name__c= 'Site_Status__c'AND ${SF_PICKLIST_TABLE_NAME}.name = 'Hot Hot'`
            let hot_hot_sfid_res = await client.query(hot_hot_sfid);
            hot_hot_sfid_res = hot_hot_sfid_res.rows[0]['sfid'];

              //Task_type picklist
              let task_sfid = `Select sfid from ${process.env.TABLE_SCHEMA_NAME}.${SF_PICKLIST_TABLE_NAME} WHERE ${SF_PICKLIST_TABLE_NAME}.object_name__c='Task__c' AND ${SF_PICKLIST_TABLE_NAME}.field_name__c= 'Task_Type__c'AND ${SF_PICKLIST_TABLE_NAME}.name = 'Validate/ Change EMD'`
              let task_sfid_res = await client.query(task_sfid);
              task_sfid_res = task_sfid_res.rows[0]['sfid'];
  
              //Nature Task picklist
              let nature_task_sfid = `Select sfid from ${process.env.TABLE_SCHEMA_NAME}.${SF_PICKLIST_TABLE_NAME} WHERE ${SF_PICKLIST_TABLE_NAME}.object_name__c='Task__c' AND ${SF_PICKLIST_TABLE_NAME}.field_name__c= 'Mode_Of_Clousre__c'AND ${SF_PICKLIST_TABLE_NAME}.name = 'Phone Call'`
              let nature_task_sfid_res = await client.query(nature_task_sfid);
              nature_task_sfid_res = nature_task_sfid_res.rows[0]['sfid'];
  
              //task status picklist
              let task_status_sfid = `Select sfid from ${process.env.TABLE_SCHEMA_NAME}.${SF_PICKLIST_TABLE_NAME} WHERE ${SF_PICKLIST_TABLE_NAME}.object_name__c='Task__c' AND ${SF_PICKLIST_TABLE_NAME}.field_name__c= 'Task_Status__c'AND ${SF_PICKLIST_TABLE_NAME}.name = 'Open'`
              let task_status_sfid_res = await client.query(task_status_sfid);
              task_status_sfid_res = task_status_sfid_res.rows[0]['sfid'];
  
              // task_owner = 'a030w0000082Z09AAE';

            //All leads with site_status Hot_hot
            let all_lead_sql = `SELECT * FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_LEAD_TABLE_NAME} where sfid is NOT NULL and site_status__c = '${hot_hot_sfid_res}'`;
      
            console.log("selecting all leads for Hot_hot");
            let all_lead_sql_res = await client.query(all_lead_sql);
      
            let today_date = new Date();
            today_date = moment(today_date).format("YYYY-MM-DD");
      
            if (all_lead_sql_res.rows.length > 0) {
              for (let i = 0; i < all_lead_sql_res.rows.length; i++) {

                if(all_lead_sql_res.rows[i]['town_name__c'] != null){
                  console.log("INSIDE")
                  callcentereSfid=await func.getCCFromTown(all_lead_sql_res.rows[i]['town_name__c'])
                }
               
                let lead_sfid = all_lead_sql_res.rows[i]["sfid"];
      
                let lead_expected_maturity_date = dtUtil.ISOtoLocal(
                  all_lead_sql_res.rows[i]["expected_maturity_date__c"]
                );
                // console.log(
                //   "Expected Maturity Date ----> ",
                //   lead_expected_maturity_date
                // );
      
                const date1 = new Date(`'${today_date}'`);
                const date2 = new Date(`'${lead_expected_maturity_date}'`);
                // console.log("--->", date1, date2);
                const diffTime = Math.ceil(date2 - date1);
                const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24));
                // console.log("Diff Days ---->", diffDays, diffTime);
      
                if (diffDays <= 0) {
                  let task_sql = `Select * from salesforce.task__c where lead__c = '${all_lead_sql_res.rows[i]["sfid"]}' and task_creation_remarks__c = 'Auto Task for Validate EMD/Change EMD'`;
                  let task_sql_res = await client.query(task_sql);
      
                  if (task_sql_res.rows.length > 0) {
                    console.log("Task is already there");
                  } else {
                    let pg_id__c = uuidv4();
                    let createddate = dtUtil.todayDatetime();
                    let today = dtUtil.todayDate();
                    const table_name = SF_TASK_TABLE_NAME;
      
                    const fieldtobeinserted = [
                      `pg_id__c, 
                                  createddate,
                                  task_date__c,
                                  task_owner__c,
                                  task_type__c,
                                  task_assigned_to__c,
                                  task_creation_remarks__c,
                                  lead__c,
                                  nature_of_task__c,
                                  task_status__c`,
                    ];
                    const fieldValue = [
                      pg_id__c,
                      createddate,
                      today,
                      callcentereSfid,
                      task_sfid_res,
                      callcentereSfid,
                      "Auto Task for Validate EMD/Change EMD",
                      lead_sfid,
                      nature_task_sfid_res,
                      task_status_sfid_res,
                    ];
      
                    let create_task_batch_sql = await qry.insertRecord(
                      fieldtobeinserted,
                      fieldValue,
                      table_name
                    );
                  }
                }
      
                console.log("++++++++++++++++++++", lead_sfid);
      
                // }
              }
              console.log("Cronjob ended for autoTask Vailidate EMD / Change EMD");
            }
          } catch (e) {
            let error_log = `Error In Lead Cron Job AutoTaskValidateEMD ----->${e}`
            console.log(error_log);
            func.mailErrorLog(error_log, req.route['path'], req.body, req.query, req.headers.token, req.headers['client-name']);
            response.response = { success: false, message: 'Internal Server error.' };
            response.status = 500;
            return response;
          }
        },
        { scheduled: true }
);
  
const AutoTaskTransferofLeadsToDealer = cron.schedule(
        `10 02 * * *`, //for Everyday At 1 Am In Night
        async () => {
          try {

            let callcentereSfid;
            //All leads with site_status Hot_hot
            let hot_hot_sfid = `Select sfid from ${process.env.TABLE_SCHEMA_NAME}.${SF_PICKLIST_TABLE_NAME} WHERE ${SF_PICKLIST_TABLE_NAME}.object_name__c='Site_Status_master__c' AND ${SF_PICKLIST_TABLE_NAME}.field_name__c= 'Site_Status__c'AND ${SF_PICKLIST_TABLE_NAME}.name = 'Hot Hot'`
            let hot_hot_sfid_res = await client.query(hot_hot_sfid);
            hot_hot_sfid_res = hot_hot_sfid_res.rows[0]['sfid'];

            //PV Branch picklist
            let pv_sfid = `Select sfid from ${process.env.TABLE_SCHEMA_NAME}.${SF_PICKLIST_TABLE_NAME} WHERE ${SF_PICKLIST_TABLE_NAME}.object_name__c='Area1__c' AND ${SF_PICKLIST_TABLE_NAME}.field_name__c= 'Branch_Type__c'AND ${SF_PICKLIST_TABLE_NAME}.name = 'PV Branch'`
            let pv_sfid_res = await client.query(pv_sfid);
            pv_sfid_res = pv_sfid_res.rows[0]['sfid'];

            //Task type picklist
            let task_sfid = `Select sfid from ${process.env.TABLE_SCHEMA_NAME}.${SF_PICKLIST_TABLE_NAME} WHERE ${SF_PICKLIST_TABLE_NAME}.object_name__c='Task__c' AND ${SF_PICKLIST_TABLE_NAME}.field_name__c= 'Task_Type__c'AND ${SF_PICKLIST_TABLE_NAME}.name = 'Transfer of leads to dealers'`
            let task_sfid_res = await client.query(task_sfid);
            task_sfid_res = task_sfid_res.rows[0]['sfid'];

            //Nature Task picklist
            let nature_task_sfid = `Select sfid from ${process.env.TABLE_SCHEMA_NAME}.${SF_PICKLIST_TABLE_NAME} WHERE ${SF_PICKLIST_TABLE_NAME}.object_name__c='Task__c' AND ${SF_PICKLIST_TABLE_NAME}.field_name__c= 'Mode_Of_Clousre__c'AND ${SF_PICKLIST_TABLE_NAME}.name = 'Phone Call'`
            let nature_task_sfid_res = await client.query(nature_task_sfid);
            nature_task_sfid_res = nature_task_sfid_res.rows[0]['sfid'];

            //task status picklist
            let task_status_sfid = `Select sfid from ${process.env.TABLE_SCHEMA_NAME}.${SF_PICKLIST_TABLE_NAME} WHERE ${SF_PICKLIST_TABLE_NAME}.object_name__c='Task__c' AND ${SF_PICKLIST_TABLE_NAME}.field_name__c= 'Task_Status__c'AND ${SF_PICKLIST_TABLE_NAME}.name = 'Open'`
            let task_status_sfid_res = await client.query(task_status_sfid);
            task_status_sfid_res = task_status_sfid_res.rows[0]['sfid'];

            //Site Quality picklist
            let premium_sfid = `Select sfid from ${process.env.TABLE_SCHEMA_NAME}.${SF_PICKLIST_TABLE_NAME} WHERE ${SF_PICKLIST_TABLE_NAME}.object_name__c='Lead__c' AND ${SF_PICKLIST_TABLE_NAME}.field_name__c= 'Site_Quality__c'AND ${SF_PICKLIST_TABLE_NAME}.name IN ('Premium','Semi Premium','Normal') order by name desc`
            let premium_sfid_res = await client.query(premium_sfid);
            let premium_picklist = premium_sfid_res.rows[0]['sfid'];
            let semipremium_picklist = premium_sfid_res.rows[1]['sfid'];
            let normal_picklist = premium_sfid_res.rows[2]['sfid'];

            // task_owner = 'a030w0000082Z09AAE';

            let parameter_value = `SELECT * FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_VALUE_PARAMETERIZATION_TABLE_NAME} where parameter_details__c = 'Today- EMD=  8 days'`
            let parameter_value_res = await client.query(parameter_value)
            if(parameter_value_res.rows.length > 0){
              parameter_8 = parameter_value_res.rows[0]['output_value__c']
            }else{
              parameter_8 = 8
            }

            let all_lead_sql = `SELECT lead__c.*,team_territory_mapping__c.team_member_id__c FROM salesforce.lead__c
            LEFT JOIN salesforce.team_territory_mapping__c ON team_territory_mapping__c.territory_code__c=lead__c.territory_name__c where lead__c.sfid is NOT NULL and lead__c.site_status__c = '${hot_hot_sfid_res}' and lead__c.town_name__c is not null`;

            console.log("selecting all leads for Hot_hot");
            let all_lead_sql_res = await client.query(all_lead_sql);
      
            let today_date = new Date();
            today_date = moment(today_date).format("YYYY-MM-DD");
      
            if (all_lead_sql_res.rows.length > 0) {
              for (let i = 0; i < all_lead_sql_res.rows.length; i++) {

                if(all_lead_sql_res.rows[i]['town_name__c'] != null){
                  console.log("INSIDE")
                  callcentereSfid=await func.getCCFromTown(all_lead_sql_res.rows[i]['town_name__c'])
                }

                let town_sql = `Select * from salesforce.area1__c where sfid = '${all_lead_sql_res.rows[i]["town_name__c"]}'`;
                // console.log("town_sql++++++", town_sql)
                let town_sql_res = await client.query(town_sql);
      
                let ter_sql = `Select * from salesforce.area1__c where sfid = '${town_sql_res.rows[0]["parent_code__c"]}'`;
                // console.log("ter_sql++++++", ter_sql)
                let ter_sql_res = await client.query(ter_sql);
      
                let se_sql = `Select * from salesforce.area1__c where sfid = '${ter_sql_res.rows[0]["parent_code__c"]}'`;
                // console.log("se_sql++++++", se_sql)
                let se_sql_res = await client.query(se_sql);
      
                let asm_sql = `Select * from salesforce.area1__c where sfid = '${se_sql_res.rows[0]["parent_code__c"]}'`;
                // console.log("se_sql++++++", se_sql)
                let asm_sql_res = await client.query(asm_sql);
      
                let branch_sql = `Select * from salesforce.area1__c where sfid = '${asm_sql_res.rows[0]["parent_code__c"]}'`;
                let branch_sql_res = await client.query(branch_sql);
                // console.log("Branch_res",branch_sql_res.rows)
      
                if (branch_sql_res.rows[0]["branch_type__c"] == pv_sfid_res) {//PV branch
      
                  if (
                    all_lead_sql_res.rows[i]["site_quality__c"] == premium_picklist || all_lead_sql_res.rows[i]["site_quality__c"] == semipremium_picklist || all_lead_sql_res.rows[i]["site_quality__c"] == normal_picklist
                  ) { //Premium/SemiPremium/Normal
      
                    let lead_sfid = all_lead_sql_res.rows[i]["sfid"];
                    let team_mem_id = all_lead_sql_res.rows[i]["team_member_id__c"];
      
                    let lead_expected_maturity_date = dtUtil.ISOtoLocal(
                      all_lead_sql_res.rows[i]["expected_maturity_date__c"]
                    );
                    // console.log(
                    //   "Expected Maturity Date ----> ",
                    //   lead_expected_maturity_date
                    // );
      
                    const date1 = new Date(`'${today_date}'`);
                    const date2 = new Date(`'${lead_expected_maturity_date}'`);
                    console.log("--->", date1, date2);
                    const diffTime = Math.ceil(date1 - date2);
                    const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24));
                    console.log("Diff Days ---->", diffDays, diffTime);
      
                    if (diffDays == parameter_8) {
                      let task_sql = `Select * from salesforce.task__c where lead__c = '${all_lead_sql_res.rows[i]["sfid"]}' and task_creation_remarks__c = 'Pass on the lead information (Lead owner name- Phone number) to dealer'`;
                      let task_sql_res = await client.query(task_sql);
      
                      if (task_sql_res.rows.length > 0) {
                        console.log("Task is already there");
                      } else {
                        let pg_id__c = uuidv4();
                        let createddate = dtUtil.todayDatetime();
                        let today = dtUtil.todayDate();
                        const table_name = SF_TASK_TABLE_NAME;
      
                        const fieldtobeinserted = [
                          `pg_id__c, 
                                  createddate,
                                  task_date__c,
                                  task_owner__c,
                                  task_type__c,
                                  task_assigned_to__c,
                                  task_creation_remarks__c,
                                  lead__c,
                                  nature_of_task__c,
                                  task_status__c`,
                        ];
                        const fieldValue = [
                          pg_id__c,
                          createddate,
                          today,
                          callcentereSfid,
                          task_sfid_res,
                          team_mem_id,
                          "Pass on the lead information (Lead owner name- Phone number) to dealer",
                          lead_sfid,
                          nature_task_sfid_res,
                          task_status_sfid_res,
                        ];
      
                        let create_task_batch_sql = await qry.insertRecord(
                          fieldtobeinserted,
                          fieldValue,
                          table_name
                        );
                      }
                    }
      
                    console.log("++++++++++++++++++++", lead_sfid);
                  }
                }
              }
              console.log("Cronjob ended for  AutoTask Transfer of Leads To Dealer");
            }
          } catch (e) {
            let error_log = `Error In Lead Cron Job AutoTaskTransferofLeadsToDealer ----->${e}`
            console.log(error_log);
            func.mailErrorLog(error_log, req.route['path'], req.body, req.query, req.headers.token, req.headers['client-name']);
            response.response = { success: false, message: 'Internal Server error.' };
            response.status = 500;
            return response;
          }
        },
        { scheduled: true }
);
  
const AutoTaskForFollowUp = cron.schedule(
        `20 02 * * *`, //for Everyday At 1 Am In Night
        async () => {
          try {
            //All leads with site_status Hot and is_verified_by_cc
            let hot_sfid = `Select sfid from ${process.env.TABLE_SCHEMA_NAME}.${SF_PICKLIST_TABLE_NAME} WHERE ${SF_PICKLIST_TABLE_NAME}.object_name__c='Site_Status_master__c' AND ${SF_PICKLIST_TABLE_NAME}.field_name__c= 'Site_Status__c'AND ${SF_PICKLIST_TABLE_NAME}.name = 'Hot'`
            let hot_sfid_res = await client.query(hot_sfid);
            hot_sfid_res = hot_sfid_res.rows[0]['sfid'];

            //Non-PV Branch picklist
            let nonpv_sfid = `Select sfid from ${process.env.TABLE_SCHEMA_NAME}.${SF_PICKLIST_TABLE_NAME} WHERE ${SF_PICKLIST_TABLE_NAME}.object_name__c='Area1__c' AND ${SF_PICKLIST_TABLE_NAME}.field_name__c= 'Branch_Type__c'AND ${SF_PICKLIST_TABLE_NAME}.name = 'Non PV Branch'`
            let nonpv_sfid_res = await client.query(nonpv_sfid);
            nonpv_sfid_res = nonpv_sfid_res.rows[0]['sfid'];

            //Task type picklist
            let task_sfid = `Select sfid from ${process.env.TABLE_SCHEMA_NAME}.${SF_PICKLIST_TABLE_NAME} WHERE ${SF_PICKLIST_TABLE_NAME}.object_name__c='Task__c' AND ${SF_PICKLIST_TABLE_NAME}.field_name__c= 'Task_Type__c'AND ${SF_PICKLIST_TABLE_NAME}.name = 'Call owner (follow up)'`
            let task_sfid_res = await client.query(task_sfid);
            task_sfid_res = task_sfid_res.rows[0]['sfid'];

            //Nature Task picklist
            let nature_task_sfid = `Select sfid from ${process.env.TABLE_SCHEMA_NAME}.${SF_PICKLIST_TABLE_NAME} WHERE ${SF_PICKLIST_TABLE_NAME}.object_name__c='Task__c' AND ${SF_PICKLIST_TABLE_NAME}.field_name__c= 'Mode_Of_Clousre__c'AND ${SF_PICKLIST_TABLE_NAME}.name = 'Phone Call'`
            let nature_task_sfid_res = await client.query(nature_task_sfid);
            nature_task_sfid_res = nature_task_sfid_res.rows[0]['sfid'];

            //task status picklist
            let task_status_sfid = `Select sfid from ${process.env.TABLE_SCHEMA_NAME}.${SF_PICKLIST_TABLE_NAME} WHERE ${SF_PICKLIST_TABLE_NAME}.object_name__c='Task__c' AND ${SF_PICKLIST_TABLE_NAME}.field_name__c= 'Task_Status__c'AND ${SF_PICKLIST_TABLE_NAME}.name = 'Open'`
            let task_status_sfid_res = await client.query(task_status_sfid);
            task_status_sfid_res = task_status_sfid_res.rows[0]['sfid'];

            //Site Quality picklist
            let premium_sfid = `Select sfid from ${process.env.TABLE_SCHEMA_NAME}.${SF_PICKLIST_TABLE_NAME} WHERE ${SF_PICKLIST_TABLE_NAME}.object_name__c='Lead__c' AND ${SF_PICKLIST_TABLE_NAME}.field_name__c= 'Site_Quality__c'AND ${SF_PICKLIST_TABLE_NAME}.name IN ('Premium','Semi Premium','Normal') order by name desc`
            let premium_sfid_res = await client.query(premium_sfid);
            let premium_picklist = premium_sfid_res.rows[0]['sfid'];
            let semipremium_picklist = premium_sfid_res.rows[1]['sfid'];
            let normal_picklist = premium_sfid_res.rows[2]['sfid'];


            // task_owner = 'a030w0000082Z09AAE';

            let parameter_value = `SELECT * FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_VALUE_PARAMETERIZATION_TABLE_NAME} where parameter_details__c = 'EMD-Today=< 45 (X) days'`
            let parameter_value_res = await client.query(parameter_value)
            if(parameter_value_res.rows.length > 0){
                parameter_45 = parameter_value_res.rows[0]['output_value__c']
            }else{
              parameter_45 = 45
            }

            let parameter_value_15 = `SELECT * FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_VALUE_PARAMETERIZATION_TABLE_NAME} where parameter_details__c = 'EMD-Today= 15 (X) days'`
            let parameter_value_res_15 = await client.query(parameter_value_15)
            if(parameter_value_res_15.rows.length > 0){
                parameter_15 = parameter_value_res_15.rows[0]['output_value__c']
            }else{
              parameter_15 = 15
            }

            let all_lead_sql = `SELECT lead__c.*,team_territory_mapping__c.team_member_id__c FROM salesforce.lead__c
                                LEFT JOIN salesforce.team_territory_mapping__c ON team_territory_mapping__c.territory_code__c=lead__c.territory_name__c where lead__c.sfid is NOT NULL and lead__c.site_status__c = '${hot_sfid_res}' and lead__c.is_verified_by_cc__c = 'true' and lead__c.town_name__c is NOT NULL`;
      
            console.log("selecting all leads for Hot", all_lead_sql);
            let all_lead_sql_res = await client.query(all_lead_sql);
      
            let today_date = new Date();
            today_date = moment(today_date).format("YYYY-MM-DD");
      
            if (all_lead_sql_res.rows.length > 0) {
              for(let i = 0; i < all_lead_sql_res.rows.length; i++) {
                let town_sql = `Select * from salesforce.area1__c where sfid = '${all_lead_sql_res.rows[i]["town_name__c"]}'`;
                // console.log("town_sql++++++", town_sql)
                let town_sql_res = await client.query(town_sql);
      
                let ter_sql = `Select * from salesforce.area1__c where sfid = '${town_sql_res.rows[0]["parent_code__c"]}'`;
                // console.log("ter_sql++++++", ter_sql)
                let ter_sql_res = await client.query(ter_sql);
      
                let se_sql = `Select * from salesforce.area1__c where sfid = '${ter_sql_res.rows[0]["parent_code__c"]}'`;
                // console.log("se_sql++++++", se_sql)
                let se_sql_res = await client.query(se_sql);
      
                let asm_sql = `Select * from salesforce.area1__c where sfid = '${se_sql_res.rows[0]["parent_code__c"]}'`;
                // console.log("se_sql++++++", se_sql)
                let asm_sql_res = await client.query(asm_sql);
      
                let branch_sql = `Select * from salesforce.area1__c where sfid = '${asm_sql_res.rows[0]["parent_code__c"]}'`;
                let branch_sql_res = await client.query(branch_sql);
                // console.log("Branch_res",branch_sql_res.rows)
      
                if (branch_sql_res.rows[0]["branch_type__c"] == nonpv_sfid_res) { //NON PV branch
      
                  let lead_sfid = all_lead_sql_res.rows[i]["sfid"];
                  let team_id = all_lead_sql_res.rows[i]['team_member_id__c']
    
                  let lead_expected_maturity_date = dtUtil.ISOtoLocal(
                    all_lead_sql_res.rows[i]["expected_maturity_date__c"]
                 );
                 //console.log( "Expected Maturity Date ----> ", lead_expected_maturity_date );
      
                  const date1 = new Date(`'${today_date}'`);
                  const emd_date = new Date(`'${lead_expected_maturity_date}'`);
                 //console.log("--->", date1, emd_date);
                  const diffTime = Math.ceil(emd_date - date1);
                  const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24));
                 //   console.log("Diff Days ---->", diffDays, diffTime);
      
                  let check15_before = dtUtil.addDays(date1, -15);
                  let check15_after = dtUtil.addDays(date1, +15);
                 //   console.log("DATE BOTH---------", check15_before, check15_after);
      
                  let task_15_before = `Select * from salesforce.task__c where createddate::date >= '${check15_before}' and createddate::date <= '${today_date}' and lead__c = '${all_lead_sql_res.rows[i]["sfid"]}'`;
                  let task_15_before_res = await client.query(task_15_before);
    
                 //   console.log("Task_15_before]]]]]] and res",task_15_before,task_15_before_res.rows);
      
                  let task_15_after = `Select * from salesforce.task__c where createddate::date >= '${today_date}' and createddate::date <= '${check15_after}' and lead__c = '${all_lead_sql_res.rows[i]["sfid"]}'`;
                  let task_15_after_res = await client.query(task_15_after);
      
                 //   console.log( "Task_15_after]]]]]] and res",task_15_after,task_15_after_res.rows);
      
                  if ((diffDays <= parameter_45 && task_15_before_res.rows.length == 0 && task_15_after_res.rows.length == 0) || (diffDays == parameter_15 && (all_lead_sql_res.rows[i]['site_quality__c'] == premium_picklist || all_lead_sql_res.rows[i]['site_quality__c'] == semipremium_picklist || all_lead_sql_res.rows[i]['site_quality__c'] == normal_picklist)))
                    {
                    let task_sql = `Select * from salesforce.task__c where lead__c = '${all_lead_sql_res.rows[i]["sfid"]}' and task_creation_remarks__c = 'AutoTask Follow up call'`;
                    let task_sql_res = await client.query(task_sql);
      
                    if (task_sql_res.rows.length > 0) {
                      console.log("Task is already there");
                    } else {
                      let pg_id__c = uuidv4();
                      let createddate = dtUtil.todayDatetime();
                      let today = dtUtil.todayDate();
                      const table_name = SF_TASK_TABLE_NAME;
      
                      const fieldtobeinserted = [
                                 `pg_id__c, 
                                  createddate,
                                  task_date__c,
                                  task_owner__c,
                                  task_type__c,
                                  task_assigned_to__c,
                                  task_creation_remarks__c,
                                  lead__c,
                                  nature_of_task__c,
                                  task_status__c`
                      ];
                      const fieldValue = [
                        pg_id__c,
                        createddate,
                        today,
                        team_id,
                        task_sfid_res,
                        team_id,
                        "AutoTask Follow up call",
                        lead_sfid,
                        nature_task_sfid_res,
                        task_status_sfid_res,
                      ];
      
                      let create_task_batch_sql = await qry.insertRecord(fieldtobeinserted,fieldValue,table_name);
                    }
                  }
                  console.log("++++++++++++++++++++", lead_sfid);
                }
              }
              console.log("Cronjob ended for  AutoTask Follow up");
            }
          } catch (e) {
            let error_log = `Error In Lead Cron Job AutoTaskForFollowUp ----->${e}`
            console.log(error_log);
            func.mailErrorLog(error_log, req.route['path'], req.body, req.query, req.headers.token, req.headers['client-name']);
            response.response = { success: false, message: 'Internal Server error.' };
            response.status = 500;
            return response;
          }
        },
        { scheduled: true }
);
const leadTaskAssignment = cron.schedule(
  `00 19 * * *`,  //Everyday At 7 pm 
  //`54 10 * * *`,  //Everyday At 7 pm 
  async () => {
    try {
      let today_date = new Date();
      today_date = moment(today_date).format('YYYY-MM-DD');
      let all_lead_sql = `SELECT * FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_LEAD_TABLE_NAME} 
      where sfid is NOT NULL AND 
       ${SF_LEAD_TABLE_NAME}.lead_created_date__c ='${today_date}'
       AND town_name__c is NOT NULL`
      console.log('this query used for all lead and task status:::', all_lead_sql)
      let all_lead_sql_res = await client.query(all_lead_sql);
      console.log('unique _sfid::::::::::::', all_lead_sql)
      if (all_lead_sql_res.rows.length > 0) {
        for (let i = 0; i < all_lead_sql_res.rows.length; i++) {
          let lead_id = all_lead_sql_res.rows[i]['sfid'];
          let town_data = `Select * from salesforce.area1__c where sfid = '${all_lead_sql_res.rows[i]['town_name__c']}'`
          console.log("town_data:::::::::::::::::::", town_data)
          let town_data_res = await client.query(town_data);


          let territory_data = `Select * from salesforce.area1__c where sfid = '${town_data_res.rows[0]['parent_code__c']}'`
          console.log("territory_data:::::::::::::::::::", territory_data)
          let territory_data_res = await client.query(territory_data);

          let se_data = `Select * from salesforce.area1__c where sfid = '${territory_data_res.rows[0]['parent_code__c']}'`
          console.log("se_data:::::::::::::::::::::::::::::", se_data)
          let se_data_res = await client.query(se_data);

          let asm_data = `Select * from salesforce.area1__c where sfid = '${se_data_res.rows[0]['parent_code__c']}'`
          console.log("asm_data::::::::::::::::::::::::::::::", asm_data)
          let asm_data_res = await client.query(asm_data);

          let branch_data = `Select * from salesforce.area1__c where sfid = '${asm_data_res.rows[0]['parent_code__c']}'`
          let branch_data_res = await client.query(branch_data);
          console.log('BRANCH-TYPE', branch_data_res.rows[0]['branch_type__c'])
          console.log('branch_data_res::::::::::::', branch_data)
          //THIS CONDITION USED FOR CHECK BRANCH PV OR NON -PV BRANCH
          let area_picklist_sql = `SELECT * FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_PICKLIST_TABLE_NAME} 
                        WHERE ${SF_PICKLIST_TABLE_NAME}.object_name__c='Area1__c' AND ${SF_PICKLIST_TABLE_NAME}.field_name__c= 'Branch_Type__c' AND ${SF_PICKLIST_TABLE_NAME}.name= 'PV Branch'`;
          let area_picklist = await client.query(area_picklist_sql);
          area_picklist = area_picklist.rows[0]['sfid'];
          if (branch_data_res.rows[0]['branch_type__c'] == area_picklist) {
            let task_picklist_sql = `SELECT * FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_PICKLIST_TABLE_NAME} 
                             WHERE ${SF_PICKLIST_TABLE_NAME}.object_name__c='Task__c' AND ${SF_PICKLIST_TABLE_NAME}.field_name__c= 'Task_Status__c' AND ${SF_PICKLIST_TABLE_NAME}.name= 'Closed'`;
            let task_picklist = await client.query(task_picklist_sql);
            task_picklist = task_picklist.rows[0]['sfid'];
            console.log('task_picklist:::::::::::::', task_picklist)
            let task_data = `Select ${SF_TASK_TABLE_NAME}.*, ${SF_TEAM_TABLE_NAME}.designation__c as profile from ${process.env.TABLE_SCHEMA_NAME}.${SF_TASK_TABLE_NAME}  
                           LEFT JOIN ${process.env.TABLE_SCHEMA_NAME}.${SF_TEAM_TABLE_NAME} ON 
                           ${SF_TASK_TABLE_NAME}.task_assigned_to__c=${SF_TEAM_TABLE_NAME}.sfid
                           where ${SF_TASK_TABLE_NAME}.lead__c='${lead_id}' AND 
                           ${SF_TASK_TABLE_NAME}.task_status__c !='${task_picklist}' AND
                           ${SF_TASK_TABLE_NAME}.task_assigned_to__c is NOT NULL`
            let task_data_res = await client.query(task_data);
            
            const callcentereSfid=await func.getCCFromTown(town_data_res.rows[0]['sfid'])
            console.log("callcentereSfid",callcentereSfid)

         
            console.log("task_data:::::::::::::::::::", task_data)
            let open_lead_sql =`Select * from salesforce.picklist__c where field_name__c = 'Lead_Open_Closed__c' and name = 'Open'`
            let open_lead_sql_res = await client.query(open_lead_sql);
            open_lead_sql_res = open_lead_sql_res.rows[0]['sfid']
            //console.log("Branch_data:::::::::::::::::",branch_data)
            //let task_lead=task_data_res.rows[0]['lead__c']

            console.log("task_data:::::::::::::::::::", task_data_res.rows.length, task_data)
            //let profile_des;
            if (task_data_res.rows.length > 0) {

              let profile_des;
              //  let prof_level={
              //      'SSE':1,
              //      'SE':2,
              //      'ASM':3,
              //      'BSM':4,
              //      'RSM':5,
              //      'GSM':6,
              //      'NSM':7,
              //      'President':8,
              //      'MD':9
              //  };
              let check_a = 0;
              task_data_res.rows.map((obj) => {
                if (obj.profile == 'Call Center' && check_a <= 10) {
                  profile_des = obj
                  check_a = 10;

                }
                if ((obj.profile == 'SSO' || obj.profile == 'SE/SSO' || obj.profile == 'SSA' || obj.profile == 'SSE') && check_a <= 9) {
                  //if (obj.profile == 'SSE' && check_a <= 9) {
                  profile_des = obj
                  check_a = 9;

                }
                if (obj.profile == 'SE' && check_a <= 8) {
                  profile_des = obj
                  check_a = 8
                }
                if (obj.profile == 'ASM' && check_a <= 7) {
                  profile_des = obj
                  check_a = 7
                }
                if (obj.profile == 'BSM' && check_a <= 6) {
                  profile_des = obj
                  check_a = 6
                }
                if (obj.profile == 'RSM' && check_a <= 5) {
                  profile_des = obj
                  check_a = 5
                }
                if (obj.profile == 'GSM' && check_a <= 4) {
                  profile_des = obj
                  check_a = 4
                }
                if (obj.profile == 'NSM' && check_a <= 3) {
                  profile_des = obj
                  check_a = 3
                }
                if (obj.profile == 'President' && check_a <= 2) {
                  profile_des = obj
                  check_a = 2
                }
                if (obj.profile == 'MD' && check_a <= 1) {
                  profile_des = obj
                  check_a = 1
                }

              })
              let update_sql = `update ${process.env.TABLE_SCHEMA_NAME}.${SF_LEAD_TABLE_NAME} set lead_owner__c='${callcentereSfid}',lead_assignment__c='${profile_des['task_assigned_to__c']}' where sfid='${lead_id}'and lead_open_closed__c = '${open_lead_sql_res}'`
              console.log('Update sql ---->', update_sql);
              let update_res = await client.query(update_sql)
            } else {
              let update_sql = `update ${process.env.TABLE_SCHEMA_NAME}.${SF_LEAD_TABLE_NAME} set lead_owner__c='${callcentereSfid}',lead_assignment__c='${callcentereSfid}' where sfid='${lead_id}' and lead_open_closed__c = '${open_lead_sql_res}'`
              console.log('update close:::::::::::::::::::', update_sql)
              let update_res = await client.query(update_sql)
            }

          }
        }
        console.log('Cron For All lead those are created today check task_status and Update Lead_assignment');
      }
      else {
        console.log('Cron Started But No Lead Record Found');
      }
    } catch (e) {
      let error_log = `Error In Lead Cron Job leadTaskAssignment ----->${e}`
      console.log(error_log);
      func.mailErrorLog(error_log, req.route['path'], req.body, req.query, req.headers.token, req.headers['client-name']);
      response.response = { success: false, message: 'Internal Server error.' };
      response.status = 500;
      return response;
    }
  },
  { scheduled: true }
)

const AutoTaskDmiAttachmentOpenlead = cron.schedule(
    `07 02 * * *`, //for Everyday At 1 Am In Night
    async () => {
      try {
        //All leads with site_status Hot_hot and dmi_attachement is not there

        let hot_hot_sfid = `Select sfid from ${process.env.TABLE_SCHEMA_NAME}.${SF_PICKLIST_TABLE_NAME} WHERE ${SF_PICKLIST_TABLE_NAME}.object_name__c='Site_Status_master__c' AND ${SF_PICKLIST_TABLE_NAME}.field_name__c= 'Site_Status__c'AND ${SF_PICKLIST_TABLE_NAME}.name = 'Hot Hot'`
        let hot_hot_sfid_res = await client.query(hot_hot_sfid);
        hot_hot_sfid_res = hot_hot_sfid_res.rows[0]['sfid'];

        //Task type picklist
        let task_sfid = `Select sfid from ${process.env.TABLE_SCHEMA_NAME}.${SF_PICKLIST_TABLE_NAME} WHERE ${SF_PICKLIST_TABLE_NAME}.object_name__c='Task__c' AND ${SF_PICKLIST_TABLE_NAME}.field_name__c= 'Task_Type__c'AND ${SF_PICKLIST_TABLE_NAME}.name = 'DMI Attachment (Open Leads)'`
        let task_sfid_res = await client.query(task_sfid);
        task_sfid_res = task_sfid_res.rows[0]['sfid']
        //Nature Task picklist
        let nature_task_sfid = `Select sfid from ${process.env.TABLE_SCHEMA_NAME}.${SF_PICKLIST_TABLE_NAME} WHERE ${SF_PICKLIST_TABLE_NAME}.object_name__c='Task__c' AND ${SF_PICKLIST_TABLE_NAME}.field_name__c= 'Mode_Of_Clousre__c'AND ${SF_PICKLIST_TABLE_NAME}.name = 'Phone Call'`
        let nature_task_sfid_res = await client.query(nature_task_sfid);
        nature_task_sfid_res = nature_task_sfid_res.rows[0]['sfid']
        //task status picklist
        let task_status_sfid = `Select sfid from ${process.env.TABLE_SCHEMA_NAME}.${SF_PICKLIST_TABLE_NAME} WHERE ${SF_PICKLIST_TABLE_NAME}.object_name__c='Task__c' AND ${SF_PICKLIST_TABLE_NAME}.field_name__c= 'Task_Status__c'AND ${SF_PICKLIST_TABLE_NAME}.name = 'Open'`
        let task_status_sfid_res = await client.query(task_status_sfid);
        task_status_sfid_res = task_status_sfid_res.rows[0]['sfid']
        //Site Quality picklist

        let premium_sfid = `Select sfid from ${process.env.TABLE_SCHEMA_NAME}.${SF_PICKLIST_TABLE_NAME} WHERE ${SF_PICKLIST_TABLE_NAME}.object_name__c='Lead__c' AND ${SF_PICKLIST_TABLE_NAME}.field_name__c= 'Site_Quality__c'AND ${SF_PICKLIST_TABLE_NAME}.name IN ('Premium','Semi Premium') order by name desc`
        let premium_sfid_res = await client.query(premium_sfid);
        let premium_picklist = premium_sfid_res.rows[0]['sfid']
        let semipremium_picklist = premium_sfid_res.rows[1]['sfid']

        let parameter_value = `SELECT * FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_VALUE_PARAMETERIZATION_TABLE_NAME} where parameter_details__c = 'EMD-Today= 15 (X) days'`
        let parameter_value_res = await client.query(parameter_value)
        if(parameter_value_res.rows.length > 0){
            parameter = parameter_value_res.rows[0]['output_value__c']
            
        }else{
          parameter = 15
        }


       let all_lead_sql = `SELECT lead__c.*,team_territory_mapping__c.team_member_id__c, contact.dmi_attachment__c FROM salesforce.lead__c
                            LEFT JOIN salesforce.team_territory_mapping__c ON team_territory_mapping__c.territory_code__c=lead__c.territory_name__c 
                            LEFT JOIN salesforce.contact ON contact.lead__c=lead__c.sfid 
                            where lead__c.sfid is NOT NULL and lead__c.site_status__c = '${hot_hot_sfid_res}' and contact.dmi_attachment__c = 'false'`

        console.log("selecting all leads for Hot_hot and dmi_attachment is false",all_lead_sql);
        let all_lead_sql_res = await client.query(all_lead_sql);
  
        let today_date = new Date();
        today_date = moment(today_date).format("YYYY-MM-DD");
  
        if (all_lead_sql_res.rows.length > 0) {
          for (let i = 0; i < all_lead_sql_res.rows.length; i++) {          
  
            if (
              all_lead_sql_res.rows[i]["site_quality__c"] == premium_picklist || all_lead_sql_res.rows[i]["site_quality__c"] == semipremium_picklist ) { //Premium/SemiPremium

              let lead_sfid = all_lead_sql_res.rows[i]["sfid"];
              let team_id = all_lead_sql_res.rows[i]['team_member_id__c']

              let lead_expected_maturity_date = dtUtil.ISOtoLocal(
                all_lead_sql_res.rows[i]["expected_maturity_date__c"]
              );
              console.log(
                "Expected Maturity Date ----> ",
                lead_expected_maturity_date
              );

              const date1 = new Date(`'${today_date}'`);
              const date2 = new Date(`'${lead_expected_maturity_date}'`);
              // console.log("--->", date1, date2);
              const diffTime = Math.ceil(date2 - date1);
              const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24));
              // console.log("Diff Days ---->", diffDays, diffTime);

              if (diffDays == parameter) {
                let task_sql = `Select * from salesforce.task__c where lead__c = '${all_lead_sql_res.rows[i]["sfid"]}' and task_creation_remarks__c = 'Auto Task DMI Attachment(Open Lead)'`;
                let task_sql_res = await client.query(task_sql);

                if (task_sql_res.rows.length > 0) {
                  console.log("Task is already there");
                } else {
                  let pg_id__c = uuidv4();
                  let createddate = dtUtil.todayDatetime();
                  let today = dtUtil.todayDate();
                  const table_name = SF_TASK_TABLE_NAME;

                  const fieldtobeinserted = [
                    `pg_id__c, 
                            createddate,
                            task_date__c,
                            task_owner__c,
                            task_type__c,
                            task_assigned_to__c,
                            task_creation_remarks__c,
                            lead__c,
                            nature_of_task__c,
                            task_status__c`,
                  ];
                  const fieldValue = [
                    pg_id__c,
                    createddate,
                    today,
                    team_id,
                    task_sfid_res,
                    team_id,
                    "Auto Task DMI Attachment(Open Lead)",
                    lead_sfid,
                    nature_task_sfid_res,
                    task_status_sfid_res,
                  ];

                  let create_task_batch_sql = await qry.insertRecord(
                    fieldtobeinserted,
                    fieldValue,
                    table_name
                  );
                }
              }

              console.log("++++++++++++++++++++", lead_sfid);
            }
            
          }
          console.log("Cronjob ended for  AutoTaskDmiAttachmentOpenlead");
        }
      } catch (e) {
        let error_log = `Error In Lead Cron Job AutoTaskDmiAttachmentOpenlead ----->${e}`
        console.log(error_log);
        func.mailErrorLog(error_log, req.route['path'], req.body, req.query, req.headers.token, req.headers['client-name']);
        response.response = { success: false, message: 'Internal Server error.' };
        response.status = 500;
        return response;
      }
    },
    { scheduled: true }
  );

  const AutoTaskDmiAttachmentClosedlead = cron.schedule(
    `27 02 * * *`, //for Everyday At 1 Am In Night
    async () => {
      try {
        //All leads with dmi_attachement is not there
  
        let all_lead_sql = `SELECT lead__c.*,team_territory_mapping__c.team_member_id__c, contact.dmi_attachment__c FROM salesforce.lead__c
                                  LEFT JOIN salesforce.team_territory_mapping__c ON team_territory_mapping__c.territory_code__c=lead__c.territory_name__c 
                                  LEFT JOIN salesforce.contact ON contact.lead__c=lead__c.sfid 
                                  where lead__c.sfid is NOT NULL and contact.dmi_attachment__c = 'false'`;
  
        console.log(
          "selecting all leads for dmi_attachment is false for closed leads",
          all_lead_sql
        );
        let all_lead_sql_res = await client.query(all_lead_sql);
  
        let today_date = new Date();
        today_date = moment(today_date).format("YYYY-MM-DD");
  
        if (all_lead_sql_res.rows.length > 0) {
          for (let i = 0; i < all_lead_sql_res.rows.length; i++) {
            let lead_sfid = all_lead_sql_res.rows[i]["sfid"];
            let team_id = all_lead_sql_res.rows[i]["team_member_id__c"];
            console.log("all_lead_sql+++++", lead_sfid);
  
            let town_sql = `Select * from salesforce.area1__c where sfid = '${all_lead_sql_res.rows[i]["town_name__c"]}'`;
            //  console.log("town_sql++++++", town_sql)
            let town_sql_res = await client.query(town_sql);
  
            let ter_sql = `Select * from salesforce.area1__c where sfid = '${town_sql_res.rows[0]["parent_code__c"]}'`;
            //  console.log("ter_sql++++++", ter_sql)
            let ter_sql_res = await client.query(ter_sql);
  
            let se_sql = `Select * from salesforce.area1__c where sfid = '${ter_sql_res.rows[0]["parent_code__c"]}'`;
            //  console.log("se_sql++++++", se_sql)
            let se_sql_res = await client.query(se_sql);
  
            let asm_sql = `Select * from salesforce.area1__c where sfid = '${se_sql_res.rows[0]["parent_code__c"]}'`;
            //  console.log("se_sql++++++", se_sql)
            let asm_sql_res = await client.query(asm_sql);
  
            let branch_sql = `Select * from salesforce.area1__c where sfid = '${asm_sql_res.rows[0]["parent_code__c"]}'`;
            let branch_sql_res = await client.query(branch_sql);
            //  console.log("Branch_res",branch_sql_res.rows)
            let area_picklist_sql = `SELECT * FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_PICKLIST_TABLE_NAME} 
                        WHERE ${SF_PICKLIST_TABLE_NAME}.object_name__c='Area1__c' AND ${SF_PICKLIST_TABLE_NAME}.field_name__c= 'Branch_Type__c' AND ${SF_PICKLIST_TABLE_NAME}.name= 'Non PV Branch'`;
            let area_picklist = await client.query(area_picklist_sql);
                area_picklist = area_picklist.rows[0]['sfid'];
            if (
              branch_sql_res.rows[0]["branch_type__c"] == area_picklist  //Non-PV Branch
            ) {
              let site_picklist_sql = `SELECT * FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_PICKLIST_TABLE_NAME} 
                        WHERE ${SF_PICKLIST_TABLE_NAME}.object_name__c='Lead__c' AND ${SF_PICKLIST_TABLE_NAME}.field_name__c= 'Site_Quality__c' AND ${SF_PICKLIST_TABLE_NAME}.name IN ('Premium','Semi Premium') order by name desc`;
              let site_picklist = await client.query(site_picklist_sql);
              let Premium = site_picklist.rows[0]['sfid'];
              let SemiPremium = site_picklist.rows[1]['sfid'];
              if (
                all_lead_sql_res.rows[i]["site_quality__c"] ==
                Premium ||
                all_lead_sql_res.rows[i]["site_quality__c"] ==
                SemiPremium
              ) {
                //Premium/SemiPremium
                let site_status_sql = `SELECT * FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_PICKLIST_TABLE_NAME} 
                        WHERE ${SF_PICKLIST_TABLE_NAME}.object_name__c='Site_Status_master__c' AND ${SF_PICKLIST_TABLE_NAME}.field_name__c= 'Site_Status__c' AND ${SF_PICKLIST_TABLE_NAME}.name IN ('Lost','Won and completed') order by name desc`;
                let site_status = await client.query(site_status_sql);
                let Lost = site_status.rows[0]['sfid'];
                let WonAndCompleted = site_status.rows[1]['sfid'];
                if (
                  all_lead_sql_res.rows[i]["site_status__c"] ==
                  Lost ||
                  all_lead_sql_res.rows[i]["site_status__c"] ==
                  WonAndCompleted
                ) {
                  //Lost/WonAndCompleted
  
                  console.log("Inside Check++++++++");
  
                  // let lead_expected_maturity_date = dtUtil.ISOtoLocal(
                  //   all_lead_sql_res.rows[i]["expected_maturity_date__c"]
                  // );
                  // console.log(
                  //   "Expected Maturity Date ----> ",
                  //   lead_expected_maturity_date
                  // );
  
                  // const date1 = new Date(`'${today_date}'`);
                  // const date2 = new Date(`'${lead_expected_maturity_date}'`);
                  // console.log("--->", date1, date2);
                  // const diffTime = Math.ceil(date2 - date1);
                  // const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24));
                  // console.log("Diff Days ---->", diffDays, diffTime);
  
                  // if (diffDays == "15") {
                  let task_sql = `Select * from salesforce.task__c where lead__c = '${all_lead_sql_res.rows[i]["sfid"]}' and task_creation_remarks__c = 'Auto Task DMI Attachment(Closed Lead)'`;
                  let task_sql_res = await client.query(task_sql);
  
                  if (task_sql_res.rows.length > 0) {
                    console.log("Task is already there");
                  } else {
                    let pg_id__c = uuidv4();
                    let createddate = dtUtil.todayDatetime();
                    let today = dtUtil.todayDate();
                    const table_name = SF_TASK_TABLE_NAME;
                    let task_type_sql = `SELECT * FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_PICKLIST_TABLE_NAME} 
                        WHERE ${SF_PICKLIST_TABLE_NAME}.object_name__c='Task__c' AND ${SF_PICKLIST_TABLE_NAME}.field_name__c= 'Task_Type__c' AND ${SF_PICKLIST_TABLE_NAME}.name= 'DMI attachement (Closed Leads)'`;
                    let task_type = await client.query(task_type_sql);
                         task_type = task_type.rows[0]['sfid'];
                    //nature_of_task__c
                    let naturetask_picklist_sql = `SELECT * FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_PICKLIST_TABLE_NAME} 
                         WHERE ${SF_PICKLIST_TABLE_NAME}.object_name__c='Task__c' AND ${SF_PICKLIST_TABLE_NAME}.field_name__c ='Mode_Of_Clousre__c' AND  ${SF_PICKLIST_TABLE_NAME}.name='Phone Call'`;
                    let naturetask_picklist = await client.query(naturetask_picklist_sql);
                        naturetask_picklist = naturetask_picklist.rows[0]['sfid'];

                    let task_status_sql = `SELECT * FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_PICKLIST_TABLE_NAME} 
                        WHERE ${SF_PICKLIST_TABLE_NAME}.object_name__c='Task__c' AND ${SF_PICKLIST_TABLE_NAME}.field_name__c= 'Task_Status__c' AND ${SF_PICKLIST_TABLE_NAME}.name= 'Closed'`;
                    let task_status = await client.query(task_status_sql);
                        task_status = task_status.rows[0]['sfid'];
                    const fieldtobeinserted = [
                      `pg_id__c, 
                                    createddate,
                                    task_date__c,
                                    task_owner__c,
                                    task_type__c,
                                    task_assigned_to__c,
                                    task_creation_remarks__c,
                                    lead__c,
                                    nature_of_task__c,
                                    task_status__c`,
                    ];
                    const fieldValue = [
                      pg_id__c,
                      createddate,
                      today,
                      team_id,
                      task_type,
                      team_id,
                      "Auto Task DMI Attachment(Closed Lead)",
                      lead_sfid,
                      naturetask_picklist,
                      task_status,
                    ];
  
                    let create_task_batch_sql = await qry.insertRecord(
                      fieldtobeinserted,
                      fieldValue,
                      table_name
                    );
                    // console.log("sql run perfct",create_task_batch_sql);
                  }
  
                  console.log("++++++++++++++++++++", lead_sfid);
                }
              }
            }
          }
          console.log(
            "Cronjob ended for  AutoTask AutoTaskDmiAttachmentClosedlead"
          );
        }
      } catch (e) {
        let error_log = `Error In Lead Cron Job AutoTaskDmiAttachmentClosedlead ----->${e}`
        console.log(error_log);
        func.mailErrorLog(error_log, req.route['path'], req.body, req.query, req.headers.token, req.headers['client-name']);
        response.response = { success: false, message: 'Internal Server error.' };
        response.status = 500;
        return response;
      }
    },
    { scheduled: true }
  );

  const sendWhtspMsg=cron.schedule(
  `00 18 * * *`,
  //`16 11 * * *`,  //For Testing Purpose
  async ()=>{
    try{
      let owner_id = await func.getPicklistSfid('Contact','Contact_Type__c','Owner')
      let all_lead_sql = `SELECT ${SF_LEAD_TABLE_NAME}.*,contact.mobilephone,contact.name as ownername FROM salesforce.contact 
      LEFT JOIN  ${process.env.TABLE_SCHEMA_NAME}.${SF_LEAD_TABLE_NAME} ON ${SF_LEAD_TABLE_NAME}.sfid = contact.lead__c
      WHERE ${SF_LEAD_TABLE_NAME}.sfid IS NOT NULL AND contact.contact_type__c='${owner_id}' AND contact.sfid IS NOT NULL AND contact.mobilephone IS NOT NULL`
      // let all_lead_sql = `SELECT ${SF_LEAD_TABLE_NAME}.*,contact.mobilephone,contact.name as ownername FROM salesforce.contact 
      // LEFT JOIN  ${process.env.TABLE_SCHEMA_NAME}.${SF_LEAD_TABLE_NAME} ON ${SF_LEAD_TABLE_NAME}.sfid = contact.lead__c
      // WHERE ${SF_LEAD_TABLE_NAME}.sfid = 'a0TC7000000E1YMMA0,a0TC7000000E1YgMAK,a0TC7000000E1YqMAK' AND contact.contact_type__c='${owner_id}' AND contact.sfid IS NOT NULL AND contact.mobilephone IS NOT NULL`
      console.log("all_lead_sql",all_lead_sql)
      let result = await client.query(all_lead_sql);
      if(result.rows.length > 0){
          for(let i = 0 ; i < result.rows.length ; i++){
              console.log(`Lead Sfid Inside Loop ::::::::::::::::::::::::::::::> ${result.rows[i]['sfid']}`);
              let date_repeat_message_arr = [];
              let emd_of_lead = result.rows[i]['expected_maturity_date__c']
              // let today_date = '2022-04-06' 
              let today_date = dtUtil.todayDate();
              let emd_change_count = result.rows[i]['emd_changed_count__c']
              let message_send_count = result.rows[i]['message_count__c']
              let lcd_of_lead = result.rows[i]['lead_created_date__c']
              let emd_changed = result.rows[i]['is_emd_changed__c']
              let t = TOTAL_NO_OF_MESSAGES;
              // console.log("message_send_count--------------------------------",message_send_count)
              //Difference Between Today date & Emd 
              const date1 = new Date(`'${today_date}'`);
              const date2 = new Date(`'${emd_of_lead}'`);
              const date3 = new Date(`'${lcd_of_lead}'`);
              //const lcd = new Date(`'${lcd_of_lead}'`);
              console.log('--->',date1,date2);
              const diffTime = Math.abs(date2 - date1);
              const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24));
              console.log('Diff Days ---->',diffDays,diffTime); 

              const diff_emd_lcd_time = Math.abs(date2 - date3);
              const diff_emd_lcd_date = Math.ceil(diff_emd_lcd_time / (1000 * 60 * 60 * 24));

              const diffTime_lcd_tod = Math.abs(date1 - date3);
              const diffDays_lcd_tod = Math.ceil(diffTime_lcd_tod / (1000 * 60 * 60 * 24));
              console.log('Diff Days bitween lcd_date and today ---->',diffTime_lcd_tod,diffDays_lcd_tod); 

              //FOR THE CASE EMD IS LESS THAN 45
              if(diff_emd_lcd_date < 45){
                  console.log(`-----FOR CASE EMD < 45 --------- ${diff_emd_lcd_date}`);
                  //LCD+((n-1)*(EMD-LCD))/(t-1)    //Where n is no. of message to be sent , t = total no. of messgae
                  let n ;
                  //THIS IS FOR CHECKING IF THE EMD IS CHANGED 
                  if(emd_change_count == 0 && message_send_count == 0 && emd_changed == false){
                      n = 1;
                  }else{
                      n = message_send_count + 1;
                  }     
                  let t = TOTAL_NO_OF_MESSAGES;
                  //EMD-LCD == DiffDays
                  let z = 1;
                  for(let j = 0 ; j < t ; j++){
                      let temp = Math.ceil(((z-1)*(diff_emd_lcd_date))/(t-1))
                      let temp_calculated_date = dtUtil.addDays(lcd_of_lead, +temp);
                      let calculated_date = new Date(`'${temp_calculated_date}'`)
                      date_repeat_message_arr.push(calculated_date.getTime()) 
                      z++
                  }
                  console.log(`Array Values -----888 ${date_repeat_message_arr}`);
                  let count = 0;
                  date_repeat_message_arr.map((dates)=>{
                      if(date1.getTime() === dates){
                          console.log('count',count);
                          count ++;
                      }
                  })
                  console.log(`------------------------------------------------------------------------------`);
                  console.log(`LCD DATE ====> ${lcd_of_lead} Value of n ====> ${n} Count ====> ${count}`);
                  let temp = Math.ceil(((n-1)*(diff_emd_lcd_date))/(t-1))
                  console.log(`Formula date ====> ${temp} ,value of t ====> ${t}`);

                  let calculated_date_value = dtUtil.addDays(lcd_of_lead, +temp);
                  //let calculated_date_temp = lcd_of_lead + Math.ceil(((n-1)*(diffDays))/(t-1))
                  let calculated_date = new Date(`'${calculated_date_value}'`)
                  console.log(`Calculated Date From Formula In Case For EMD-LCD < 45 ======> ${calculated_date} , Today Date --> ${date1}`);
                  if(date1.getTime() === calculated_date.getTime()){
                      for(let k = 0 ; k < count ; k ++){
                          console.log(`INSIDE MESSAGE LOGIC`);
                          let get_latest_count = `SELECT message_count__c FROM salesforce.lead__c WHERE sfid = '${result.rows[i]['sfid']}'`
                          console.log('get_latest_count',get_latest_count)
                          let get_latest_count_res = await client.query(get_latest_count);

                          let deciding_factor = get_latest_count_res.rows[0]['message_count__c']
                          let temp__p = get_latest_count_res.rows[0]['message_count__c'] + 1
                          console.log("eeeeeeeee",deciding_factor)
                          if(deciding_factor == 0){
                              console.log('Sends 1st Message');
                              let msg = await whatsapp.sendWhatsAppMsg_intro(`'${result.rows[i]['mobilephone']}'`, 'whatsapp/1/message/template', 'centuryply_intro', [` ${result.rows[i]['ownername']}`,' https://www.centuryply.com/'])
                              let meassage_count_update_sql = `UPDATE salesforce.lead__c SET message_count__c = ${temp__p} , is_emd_changed__c = 'false' WHERE sfid = '${result.rows[i]['sfid']}'`
                              let meassage_count_update_sql_res = await client.query(meassage_count_update_sql)
                              console.log(`Let Message Count Update SQL =====> ${meassage_count_update_sql}`);
                              console.log(`Let Message Count Update SQL Response =====> ${meassage_count_update_sql_res}`);
                          }
                          if(deciding_factor == 1){
                              console.log('Sends 2nd Message');
                              let msg = await whatsapp.sendWhatsAppMsg(`'${result.rows[i]['mobilephone']}'`, 'whatsapp/1/message/template', 'glpnt_msg_v1', [` ${result.rows[i]['ownername']} `])        // console.log(msg);
                              let meassage_count_update_sql = `UPDATE salesforce.lead__c SET message_count__c = ${temp__p} , is_emd_changed__c = 'false' WHERE sfid = '${result.rows[i]['sfid']}'`
                              let meassage_count_update_sql_res = await client.query(meassage_count_update_sql)
                              console.log(`Let Message Count Update SQL =====> ${meassage_count_update_sql}`);
                              console.log(`Let Message Count Update SQL Response =====> ${meassage_count_update_sql_res}`);
                          }
                          if(deciding_factor == 2){
                              console.log('Sends 3rd Message');
                              let msg = await whatsapp.sendWhatsAppMsg(`'${result.rows[i]['mobilephone']}'`, 'whatsapp/1/message/template', 'general_education_consumers', [` ${result.rows[i]['ownername']}`])        // console.log(msg);
                              let meassage_count_update_sql = `UPDATE salesforce.lead__c SET message_count__c = ${temp__p}, is_emd_changed__c = 'false' WHERE sfid = '${result.rows[i]['sfid']}'`
                              let meassage_count_update_sql_res = await client.query(meassage_count_update_sql)
                              console.log(`Let Message Count Update SQL =====> ${meassage_count_update_sql}`);
                              console.log(`Let Message Count Update SQL Response =====> ${meassage_count_update_sql_res}`);
                          }
                          if(deciding_factor == 3){
                              console.log('Sends 4th Message');
                              let msg = await whatsapp.sendWhatsAppMsg(`'${result.rows[i]['mobilephone']}'`, 'whatsapp/1/message/template', 'precaution_message_consumers', [` ${result.rows[i]['ownername']}`])        // console.log(msg);
                              let meassage_count_update_sql = `UPDATE salesforce.lead__c SET message_count__c = ${temp__p}, is_emd_changed__c = 'false' WHERE sfid = '${result.rows[i]['sfid']}'`
                              let meassage_count_update_sql_res = await client.query(meassage_count_update_sql)
                              console.log(`Let Message Count Update SQL =====> ${meassage_count_update_sql}`);
                              console.log(`Let Message Count Update SQL Response =====> ${meassage_count_update_sql_res}`);
                          }
                          if(deciding_factor == 4){
                              console.log('Sends 5th Message');
                              let msg = await whatsapp.sendWhatsAppMsg(`'${result.rows[i]['mobilephone']}'`, 'whatsapp/1/message/template', 'distributor_product_commnunication', [` ${result.rows[i]['ownername']} `,' https://www.centuryply.com/'])        // console.log(msg);
                              let meassage_count_update_sql = `UPDATE salesforce.lead__c SET message_count__c = ${temp__p}, is_emd_changed__c = 'false' WHERE sfid = '${result.rows[i]['sfid']}'`
                              let meassage_count_update_sql_res = await client.query(meassage_count_update_sql)
                              console.log(`Let Message Count Update SQL =====> ${meassage_count_update_sql}`);
                              console.log(`Let Message Count Update SQL Response =====> ${meassage_count_update_sql_res}`);
                          }
                      }
                  }
              }

              //FOR THE CASE EMD IS GREATER THAN 45
              if(diff_emd_lcd_date >= 45){
                  console.log(`-----FOR CASE EMD > 45 --------- ${diff_emd_lcd_date}`);
                  let n;
                  //THIS IS FOR CHECKING IF THE EMD IS CHANGED 
                  if(emd_change_count != 0 && emd_changed == true){
                          n = 2;
                          let message_count_update_sql = `UPDATE salesforce.lead__c SET message_count__c = 1 WHERE sfid = '${result.rows[i]['sfid']}'`
                          console.log(`Message Count Update Sql In Case Of Updated EMD =====> ${message_count_update_sql}`);
                          let message_count_result = await client.query(message_count_update_sql)
                  }else{
                          n = message_send_count + 1;
                  }
                  //(EMD-45)+((n-2)*45)/(t-2)
                  let calculate_part_1 = dtUtil.addDays(emd_of_lead, -45);
                  console.log(`Calculated Part 1 ----> ${calculate_part_1}`);

                  let calculate_part_2 = Math.ceil(((n-2)*45)/(t-2))
                  console.log(`Calculated Part 2 ----> ${calculate_part_2} , values of n ===> ${n},values of t ===> ${t} , ${((n-2)*45) / (t-2)}`);

                  let calculated_date = dtUtil.addDays(calculate_part_1, +calculate_part_2);
                  calculated_date = new Date(`'${calculated_date}'`)
                  //let calculated_date_temp = lcd_of_lead + Math.ceil(((n-1)*(diffDays))/(t-1))
                  //let calculated_date = new Date(`'${check15_after}'`)

                  let z = 1;
                  for(let j = 0 ; j < t ; j++){

                      let calculate_part_1 = dtUtil.addDays(emd_of_lead, -45);
                      let calculate_part_2 = Math.ceil(((z-2)*45)/(t-2))
                      let calculated_date = dtUtil.addDays(calculate_part_1, +calculate_part_2);
                      let calculated_date_temp = new Date(`'${calculated_date}'`)
                      date_repeat_message_arr.push(calculated_date_temp.getTime()) 

                      z++
                  }
                  console.log(`Array Values ----- ${date_repeat_message_arr}`);
                  let count = 0;
                  date_repeat_message_arr.map((dates)=>{
                      if(date1.getTime() === dates){   //For Checking How Many Messages I have To Send Today
                          //console.log('count',count);
                          count ++;
                      }
                  })

                  console.log(`Calculated Date From Formula In Case For EMD-LCD > 45 ======> ${calculated_date} , Today Date --> ${date1} , count ---> ${count}`);

                  if(message_send_count == 0){
                      if(date1.getTime() === date3.getTime()){ //Today date == LCD
                          //SENT 1st Message 
                          let m_count = 1;
                          console.log('Sends 1st Message');
                          let msg = await whatsapp.sendWhatsAppMsg_intro(`'${result.rows[i]['mobilephone']}'`, 'whatsapp/1/message/template', 'centuryply_intro', [` ${result.rows[i]['ownername']}`,' https://www.centuryply.com/'])        // console.log(msg);
                          let meassage_count_update_sql = `UPDATE salesforce.lead__c SET message_count__c = ${m_count} , is_emd_changed__c = false WHERE sfid = '${result.rows[i]['sfid']}'`
                          console.log(`Let Message Count Update SQL =====> ${meassage_count_update_sql}`);
                          let meassage_count_update_sql_res = await client.query(meassage_count_update_sql)
                          console.log(`Let Message Count Update SQL Response =====> ${meassage_count_update_sql_res}`);
                      }
                  }else{
                      console.log("date1.getTime()",date1)
                      console.log("calculated_date.getTime()",calculated_date)
                      if(date1.getTime() === calculated_date.getTime()){
                          for(let k = 0 ; k < count ; k ++){

                              let get_latest_count = `SELECT message_count__c FROM salesforce.lead__c WHERE sfid = '${result.rows[i]['sfid']}'`
                              let get_latest_count_res = await client.query(get_latest_count);
                              let deciding_factor = get_latest_count_res.rows[0]['message_count__c']
                              let temp_variable = deciding_factor + 1
                              // console.log("temp_variable----------------------",temp_variable)
                              console.log("deciding_factor",deciding_factor)
                              if(deciding_factor == 1){
                                  console.log('Sends 2nd Message');
                                  let msg = await whatsapp.sendWhatsAppMsg(`'${result.rows[i]['mobilephone']}'`, 'whatsapp/1/message/template', 'glpnt_msg_v1', [` ${result.rows[i]['ownername']} `])        // console.log(msg);
                                  let meassage_count_update_sql = `UPDATE salesforce.lead__c SET message_count__c = ${temp_variable} , is_emd_changed__c = false WHERE sfid = '${result.rows[i]['sfid']}'`
                                  let meassage_count_update_sql_res = await client.query(meassage_count_update_sql)
                                  console.log(`Let Message Count Update SQL =====> ${meassage_count_update_sql}`);
                                  console.log(`Let Message Count Update SQL Response =====> ${meassage_count_update_sql_res}`);
                              }
                              if(deciding_factor == 2){
                                  console.log('Sends 3rd Message');
                                  let msg = await whatsapp.sendWhatsAppMsg(`'${result.rows[i]['mobilephone']}'`, 'whatsapp/1/message/template', 'general_education_consumers', [` ${result.rows[i]['ownername']}`])                                       
                                  let meassage_count_update_sql = `UPDATE salesforce.lead__c SET message_count__c = ${temp_variable} , is_emd_changed__c = false WHERE sfid = '${result.rows[i]['sfid']}'`
                                  let meassage_count_update_sql_res = await client.query(meassage_count_update_sql)
                                  console.log(`Let Message Count Update SQL =====> ${meassage_count_update_sql}`);
                                  console.log(`Let Message Count Update SQL Response =====> ${meassage_count_update_sql_res}`);
                              }
                              if(deciding_factor == 3){
                                  console.log('Sends 4th Message');
                                  let msg = await whatsapp.sendWhatsAppMsg(`'${result.rows[i]['mobilephone']}'`, 'whatsapp/1/message/template', 'precaution_message_consumers', [` ${result.rows[i]['ownername']}`])        // console.log(msg);
                                  let meassage_count_update_sql = `UPDATE salesforce.lead__c SET message_count__c = ${temp_variable} , is_emd_changed__c = false WHERE sfid = '${result.rows[i]['sfid']}'`
                                  let meassage_count_update_sql_res = await client.query(meassage_count_update_sql)
                                  console.log(`Let Message Count Update SQL =====> ${meassage_count_update_sql}`);
                                  console.log(`Let Message Count Update SQL Response =====> ${meassage_count_update_sql_res}`);
                              }
                              if(deciding_factor == 4){
                                  console.log('Sends 5th Message');
                                  let msg = await whatsapp.sendWhatsAppMsg(`'${result.rows[i]['mobilephone']}'`, 'whatsapp/1/message/template', 'distributor_product_commnunication',[` ${result.rows[i]['ownername']}`,' https://www.centuryply.com/'])        // console.log(msg);
                                  let meassage_count_update_sql = `UPDATE salesforce.lead__c SET message_count__c = ${temp_variable} , is_emd_changed__c = false WHERE sfid = '${result.rows[i]['sfid']}'`
                                  let meassage_count_update_sql_res = await client.query(meassage_count_update_sql)
                                  console.log(`Let Message Count Update SQL =====> ${meassage_count_update_sql}`);
                                  console.log(`Let Message Count Update SQL Response =====> ${meassage_count_update_sql_res}`);
                              }    
                          }
                      }
                  }
              }
              //FOR THE CASE EMD CHANGES BEFORE BY 45 DAYS
              // if(diffDays_lcd_tod < 45 && is_emd_changed__c){

              // } 
              // //FOR THE CASE EMD CHANGES AFTER 45 DAYS
              // if(diffDays_lcd_tod > 45 && is_emd_changed__c){

              // } 
          }
        console.log(`---------------------Cron Ended For Watsapp Lead Cultivation Notification -----------------------`);
      }
}catch(e){
      let error_log = `Error In Lead Cron Job sendWhtspMsg ----->${e}`
      console.log(error_log);
      func.mailErrorLog(error_log, req.route['path'], req.body, req.query, req.headers.token, req.headers['client-name']);
      response.response = { success: false, message: 'Internal Server error.' };
      response.status = 500;
      return response;
}
 }   
  ,{schedule:true})

const changeTaskAssignmentLead = cron.schedule(
  `00 21 * * *`,
  async () => {
    try {
      console.log("*********************changeTaskAssignmentLead cron started*********************")
      let task_sfid = `Select sfid from ${process.env.TABLE_SCHEMA_NAME}.${SF_PICKLIST_TABLE_NAME} WHERE ${SF_PICKLIST_TABLE_NAME}.object_name__c='Task__c' AND ${SF_PICKLIST_TABLE_NAME}.field_name__c= 'Task_Status__c' order by name desc`
      let task_sfid_res = await client.query(task_sfid);
      let Open = task_sfid_res.rows[0]['sfid'];
      let Close = task_sfid_res.rows[1]['sfid'];
      console.log("task_sfid:::::::", Open)
      let lead_sql = `SELECT * FROM salesforce.lead__c where sfid is not null`
      let lead_sql_res = await client.query(lead_sql)

      if (lead_sql_res.rows.length > 0) {
        for (let i = 0; i < lead_sql_res.rows.length; i++) {
          console.log('lead id::::::',lead_sql_res.rows[i]['sfid'])
          let lead_assignment = lead_sql_res.rows[i]['lead_assignment__c']
          let task_sql = `SELECT task__c.*, team__c.hierarchylevel__c,team__c.team_member_name__c,picklist__c.name as level_name  
                              FROM salesforce.task__c 
                              LEFT JOIN salesforce.team__c ON team__c.sfid = task__c.task_assigned_to__c
                              LEFT JOIN salesforce.picklist__c ON picklist__c.sfid=team__c.hierarchylevel__c 
                              where task__c.lead__c = '${lead_sql_res.rows[i]['sfid']}' 
                              and task__c.task_status__c = '${Open}' and task__c.task_assigned_to__c is not null`     //Open:a050w000003PeOjAAK
          console.log('task id::::::',task_sql)
          let task_res = await client.query(task_sql);

          if (task_res.rows.length > 0) {
            let team_data_arr = [];
            task_res.rows.map((data) => {
              if (data['task_assigned_to__c'] != null && data['task_assigned_to__c'].length > 0) {
                let obj = {
                  team_member_name: data['team_member_name__c'],
                  team_member_id: data['task_assigned_to__c'],
                  team_herarichy_level: data['level_name']
                }
                team_data_arr.push(obj)
              }
            })

            function compare(a, b) {
              if (a.team_herarichy_level < b.team_herarichy_level) {
                return -1;
              }
              if (a.team_herarichy_level > b.team_herarichy_level) {
                return 1;
              }
              return 0;
            }
            team_data_arr.sort(compare);

            let update_lead_sql = `UPDATE salesforce.lead__c
                                      SET task_assignment__c = '${team_data_arr[team_data_arr.length - 1]['team_member_id']}'
                                      WHERE sfid = '${lead_sql_res.rows[i]['sfid']}'`
            let update_res = await client.query(update_lead_sql)
            console.log(`Update Res -----> ${update_res}`);



          } else {
            //if no task data found
            let update_lead_sql = `UPDATE salesforce.lead__c
                                      SET task_assignment__c = '${lead_assignment}'
                                      WHERE sfid = '${lead_sql_res.rows[i]['sfid']}'`
            let update_res = await client.query(update_lead_sql)
            console.log(`update_lead_sql Res -----> update_lead_sql`,update_lead_sql);
            console.log(`Update Res -----> ${update_res}`);
          }
        }
      } else {
        console.log(`No Lead Data Found`);
      }
      console.log("*********************changeTaskAssignmentLead cron ended*********************")
    } catch (e) {
      console.log(`Error In Test -----> ${e}`);
      let error_log = `cron error changeTaskAssignmentLead:::: ${e}`
        console.log(error_log,req.route['path']);
      func.mailErrorLog(error_log,req.route['path']);

    }
  }
  , { schedule: true })

module.exports = {
    leadStatusOnDateJob,
    autoTask_and_NotificationForOwnerNo,
    leadStatusOnCreationDate,
    leadStatusOnInformation,
    leadStatusOnDateAndStage,
    AutoTaskFirstSupply,
    AutoTaskFurtherSupply,
    AutoTaskValidateEMD,
    AutoTaskTransferofLeadsToDealer,
    AutoTaskForFollowUp,
    leadTaskAssignment,
    AutoTaskDmiAttachmentOpenlead,
    AutoTaskDmiAttachmentClosedlead,
    sendWhtspMsg,
    changeTaskAssignmentLead
};