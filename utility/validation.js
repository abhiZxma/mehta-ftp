var validator = require('validator');
var moment = require('moment');
const dtUtil = require(`${PROJECT_DIR}/utility/dateUtility`);


module.exports = {
    issetNotEmpty,
    isset,
    isValidDate,
    isPicklistValueValid,
    isValidSalesforceId,
    taskStageValidation,
    taskStatusOpenCloseLiveLeadValidation,
    leadStatusOnDate,
    getLastEMD,
    siteStatusOnTaskOutcome
};

function issetNotEmpty(str){
    if(str!=undefined && typeof(str)=='string' && str.trim()!='')
        return true;
    else if(str!=undefined && str!='')
        return true;
    else
        return false;
}

function isset(str){
    if(str!=undefined)
        return true;
    else
        return false;
}

function isValidDate(timestamp){
    if(issetNotEmpty(timestamp)){
        if((typeof(timestamp) =='string' && timestamp.length == 13) || (typeof(timestamp) =='number' && timestamp.toString().length == 13) )
            timestamp = (typeof(timestamp) =='number')?timestamp.toString().substring(0, timestamp.length-3):timestamp.substring(0, timestamp.length-3);
            
            console.log('Is valid Date >>>   ',moment.unix(timestamp).format("YYYY-MM-DD HH:mm:ss"));
            
            if(moment.unix(timestamp).format("YYYY-MM-DD HH:mm:ss")=='Invalid date'){
                return false;
            }else{
                return true;
            }
    }else{
        return false;
    }
}

/**
 * Compare picklist values from constant files 
 * @param {*} fieldValue | input field value
 * @param {*} objName | Salesforce object name
 * @param {*} apiName | Salesforce field api name
 */

function picklistValidation(fieldValue, objName, apiName){
    if (objName == "Site") {
        return (site_PickList[apiName].indexOf(fieldValue) >= 0) ? true : false;
    } else if (objName == "Influencer") {
        return (influencers_PickList[apiName].indexOf(fieldValue) >= 0) ? true : false;
    } else if (objName == "Event") {
        return (event_PickList[apiName].indexOf(fieldValue) >= 0) ? true : false;
    } else if (objName == "Visit") {
        return (visit_PickList[apiName].indexOf(fieldValue) >= 0) ? true : false;
    } else if (objName == "attendance") {
        return (attendance_PickList[apiName].indexOf(fieldValue) >= 0) ? true : false;
    } else {
        return false;
    }
}
/**
 * This function is used to validate salesforce picklist values 
 * @param {*} fieldValue 
 * @param {*} objName Salesforce object name
 * @param {*} apiName Salesforce field api name
 * @param {*} isMandatory true|false
 * @author Rohit Ramawat
 */

function isPicklistValueValid(fieldValue, objName, apiName, isMandatory) {
    var ValidationResponse =  false;
    try {
        if (isMandatory==undefined || isMandatory) {
            ValidationResponse = picklistValidation(fieldValue, objName, apiName);
        } else {
            if (fieldValue != undefined && fieldValue != null && fieldValue != '') {
                ValidationResponse = picklistValidation(fieldValue, objName, apiName);
            } else {
                ValidationResponse = true;
            }
        }
        console.log(`INFO(Validation)::::   isMandatory = ${isMandatory}   objName = ${objName}  apiName = ${apiName} fieldValue = ${fieldValue}  ValidationResponse = ${ValidationResponse}`)
        return ValidationResponse;
    } catch (e) {
        return false;
    }
}
var db = require(`${PROJECT_DIR}/utility/selectQueries`);


    // console.log('Start Time:  ', moment().format('YYYY-MM-DD HH:mm:ss'))
    // await isValidSalesforceId('Contact','0031m0000071LKyAAM', true);
    // console.log('End Time:  ', moment().format('YYYY-MM-DD HH:mm:ss'))

async function checkSFIDReference(objName, sfid) {
    
    if (issetNotEmpty(sfid) && issetNotEmpty(objName)) {

        var WhereClouse = [];
        WhereClouse.push({ "fieldName": "sfid", "fieldValue": sfid });
        sql = db.SelectAllQry('sfid', objName, WhereClouse, '0', '1', '');
        var records = await client.query(sql);
        console.log('INFO :: sql = ',sql)
        console.log('INFO :: records.rowCount = ',records.rowCount)
        if (records.rowCount != undefined && records.rowCount > 0) {
            return {"success": true , "message":""};
        } else {
            return {"success": false , "message":"No record found."};
        }
    } else {
        return {"success": false , "message":"Field api name and object name should not be blank."};
    }
} 

async function isValidSalesforceId(objName, sfid, isMandatory) {
    try {
        response = { "success": false, "message": "No Record found." };
        if (isMandatory != undefined && isMandatory) {
            response = await checkSFIDReference(objName, sfid);
        } else {
            if (issetNotEmpty(sfid)) {
                response = await checkSFIDReference(objName, sfid);
            } else {
                response = { "success": true, "message": "" };
            }
        }
        console.log(`INFO:: IS valid Object = ${objName}   sfid = ${sfid}  isMandatory = ${isMandatory}  response = `, response);
        return response;

    } catch (e) {
        console.log('ERROR: SERVER ERROR ', e)
        return { "success": false, "message": "Internal server error" };
    }
}

async function taskStageValidation(task_type,stage_value_before_updation){
    try{

        let stage_value;
        let stage_value_in_number;
        let statement_to_be_run ;
        let stage_value_before_updation_in_number ;

        let stage_sfid = `Select * from salesforce.picklist__c where field_name__c = 'Stage__c' order by name desc`
        let stage_sfid_res = await client.query(stage_sfid);

        //Dynamic Picklist Ids
        let stage5 = stage_sfid_res.rows[0]['sfid']
        let stage4 = stage_sfid_res.rows[1]['sfid']
        let stage3 = stage_sfid_res.rows[2]['sfid']
        let stage2 = stage_sfid_res.rows[3]['sfid']
        let stage1 = stage_sfid_res.rows[4]['sfid']

        let task_type_sfid = `Select * from salesforce.picklist__c where field_name__c = 'Task_Type__c' order by name desc`
        let task_type_sfid_res = await client.query(task_type_sfid);
        let emd_change = task_type_sfid_res.rows[16]['sfid']
        let get_owner_no = task_type_sfid_res.rows[8]['sfid']
        let call_owner_1st = task_type_sfid_res.rows[18]['sfid']
        let call_owner_follow = task_type_sfid_res.rows[17]['sfid']
        let Product_pitching = task_type_sfid_res.rows[5]['sfid']
        let sampling_demo = task_type_sfid_res.rows[2]['sfid']
        let Product_selection_quantity  = task_type_sfid_res.rows[4]['sfid']
        let Give_Quotation = task_type_sfid_res.rows[7]['sfid']
        let Align_with_Contractor_or_DMI = task_type_sfid_res.rows[19]['sfid']
        let DMI_Attachment_Open_Leads = task_type_sfid_res.rows[10]['sfid']
        let Negotiation = task_type_sfid_res.rows[6]['sfid']
        let Dealer_alignment_with_owner = task_type_sfid_res.rows[12]['sfid']
        let FinaliseOrder = task_type_sfid_res.rows[9]['sfid']
        let Confirm_Delivery_1st_Supply = task_type_sfid_res.rows[15]['sfid']
        let Confirm_Delivery_further_Supply = task_type_sfid_res.rows[14]['sfid']
        let Confirm_Lost = task_type_sfid_res.rows[13]['sfid']
        let DMI_Attachment_Closed_Leads = task_type_sfid_res.rows[11]['sfid']

        //Dynamic Picklist End

        console.log('Values of task type and before updateion-------->',task_type,stage_value_before_updation);
        /********************* STAGE VALUE Before Updation Calculation  ***********************************/
        switch(stage_value_before_updation) {
            case stage1:   //stage 1
                statement_to_be_run = 1;
                stage_value_before_updation_in_number = 1;
                console.log('--Stage 1',statement_to_be_run);
                break;
            case stage2:   //stage 2
                statement_to_be_run = 1;
                stage_value_before_updation_in_number = 2;
                console.log('--Stage 2',statement_to_be_run);
                break;
            case stage3:   //stage 3
                statement_to_be_run = 1;
                stage_value_before_updation_in_number = 3;
                console.log('--Stage 3',statement_to_be_run);
                break;
            case stage4:   //stage 4
                statement_to_be_run = 1;
                stage_value_before_updation_in_number = 4;
                console.log('--Stage 4',statement_to_be_run);
                break;
            case stage5:   //stage 5
                statement_to_be_run = 1;
                stage_value_before_updation_in_number = 5;
                console.log('--Stage 5',statement_to_be_run);
                break;
            default:
                statement_to_be_run = 0;
                console.log('*******************DEFAULT STATEMENT IS RUNNING***************************',statement_to_be_run);
        }
        /********************* STAGE VALUE Before Updation Calculation Ends ***********************************/


        /************************      Current Stage Value Calculation    ********************************************/
        //Validate/ Change EMD, Transfer of leads to dealers  -------> No Stages Will Be Updated
        if(task_type == emd_change){
            stage_value = null;
            stage_value_in_number = 0;
            console.log('No Stage');
        }
        //Get owner phone no, Call owner (first time), Call owner (follow up) ----> Stage 1
        if(task_type == get_owner_no || task_type == call_owner_1st || task_type == call_owner_follow){
            stage_value = stage1;
            stage_value_in_number = 1;
            console.log('Stage 1');

        }
        //Product pitching to owner, Sampling and Demo to owner ------> Stage 2
        if(task_type == Product_pitching || task_type == sampling_demo){
            stage_value = stage2;
            stage_value_in_number = 2;
            console.log('Stage 2');
        }
        //Product selection & quantity finalisation with Influencer, Give Quotation, Align with Contractor/ DMI, DMI Attachment (Open Leads)-------> Stage 3
        if(task_type == Product_selection_quantity || task_type == Give_Quotation || task_type == Align_with_Contractor_or_DMI || task_type == DMI_Attachment_Open_Leads){
            stage_value = stage3;
            stage_value_in_number = 3;
            console.log('Stage 3');
        }
        //Negotiation , Dealer alignment with owner -------> Stage 4
        if(task_type == Negotiation || task_type == Dealer_alignment_with_owner){
            stage_value = stage4;
            stage_value_in_number = 4;
            console.log('Stage 4');
        }
        //Finalise Order , Confirm Delivery (First Supply), Confirm Delivery (Further Supply), Confirm Lost, DMI attachement (Closed Leads) -------> Stage 5
        if(task_type == FinaliseOrder || task_type == Confirm_Delivery_1st_Supply || task_type == Confirm_Delivery_further_Supply || task_type == Confirm_Lost || task_type == DMI_Attachment_Closed_Leads){
            stage_value = stage5;
            stage_value_in_number = 5;
            console.log('Stage 5');
        }

        /************************      Current Stage Value Calculation Ends  ********************************************/
        console.log('STAGE VALUE ------->',stage_value);
        console.log('STAGE VALUE IN NUMBER ------->',stage_value_in_number);
        console.log('Stage Value Before Updation In Number ----->',stage_value_before_updation_in_number);

        if(statement_to_be_run == 0){
            console.log('Statement To Be Run 0 ------->');
            return {stage_value}
        }
        if(statement_to_be_run == 1){
            if(stage_value_in_number >= stage_value_before_updation_in_number){
                console.log('Statement To Be Run else part-------> !st prt');
                return {stage_value};
            }else{
                console.log('@@@@2nd Part');
                return {stage_value_before_updation};
            }
        }
        
    }catch(e){
        console.log('Error In Task Stage Validation ------>',e);
    }
}

async function taskStatusOpenCloseLiveLeadValidation(site_status_value){
    try{
        //Dynamic Picklist Ids
        let site_status_sql = `SELECT sfid FROM salesforce.picklist__c where field_name__c = 'Site_Status__c' order by name desc`
        let site_status_sql_res = await client.query(site_status_sql)
        let Won_and_Ongoing = site_status_sql_res.rows[0]['sfid']
        let Won_and_confirmation_pending = site_status_sql_res.rows[1]['sfid']
        let Won_and_completed = site_status_sql_res.rows[2]['sfid']
        let Warm = site_status_sql_res.rows[3]['sfid']
        let Pending_for_phone_no = site_status_sql_res.rows[4]['sfid']
        let Lost = site_status_sql_res.rows[5]['sfid']
        let Information = site_status_sql_res.rows[6]['sfid']
        let Inactive = site_status_sql_res.rows[7]['sfid']
        let Hot_Hot = site_status_sql_res.rows[8]['sfid']
        let Hot = site_status_sql_res.rows[9]['sfid']
        let Cold = site_status_sql_res.rows[10]['sfid']

        let open_closed = `SELECT sfid FROM salesforce.picklist__c where field_name__c = 'Lead_Open_Closed__c' order by name desc`
        let open_closed_res = await client.query(open_closed)
        let open = open_closed_res.rows[0]['sfid']
        let close = open_closed_res.rows[1]['sfid']
        //Dynamic Picklist End


        let lead_value ;
        let live_lead_value ; 
        console.log('Site Status Value In Validation Function =====> ',site_status_value);
        switch(site_status_value) {
            case Hot_Hot:   //Hot Hot
                lead_value = open;  //Open
                live_lead_value = true;
                break;
            case Hot:   //Hot
                lead_value = open;  //Open
                live_lead_value = true;
                break;
            case Warm:   //Warm
                lead_value = open;  //Open
                live_lead_value = true;
                break;
            case Cold:   //Cold
                lead_value = open;  //Open
                live_lead_value = false;
                break;
            case Inactive:   //Inactive
                lead_value = close;  //Close
                live_lead_value = false;
                break;
            case Information:   //Information
                lead_value = open;  //Open
                live_lead_value = false;
                    break;
            case Won_and_Ongoing:   ////Won And Ongoing
                lead_value = close;  //Close
                live_lead_value = true;;
                break;
            case Won_and_completed:   //Won And Completed
                lead_value = close;  //Close
                live_lead_value = false;
                break;
            case Won_and_confirmation_pending:   //Won And Confirmation Pending
                lead_value = close;  //Close
                live_lead_value = false;
                break;
            case Lost:   //Project Lost(Extra added not as per logic)
                lead_value = close;  //Close
                live_lead_value = false;
                break;    
            default:
                lead_value = 'No Update';  //Close
                live_lead_value = false;
                console.log('*******************DEFAULT STATEMENT IS RUNNING***************************');
        }
        console.log(`Lead Value ${lead_value} and Live_lead_value ${live_lead_value}`);
        return {lead_value,live_lead_value}
    }catch(e){
        console.log('Error In Task Status Open , Close , Live Lead Validation Function ------->',e);
    }
}

async function leadStatusOnDate(expected_maturity_date){
    try{

        //Dynamic Picklist Ids
        let site_status_sql = `SELECT sfid FROM salesforce.picklist__c where field_name__c = 'Site_Status__c' order by name desc`
        let site_status_sql_res = await client.query(site_status_sql)
        let Warm = site_status_sql_res.rows[3]['sfid']
        let Information = site_status_sql_res.rows[6]['sfid']
        let Hot_Hot = site_status_sql_res.rows[8]['sfid']
        let Hot = site_status_sql_res.rows[9]['sfid']
        let Cold = site_status_sql_res.rows[10]['sfid']
        //Dynamic Picklist Ids End

            let hot_hot_min_value;
            let hot_hot_max_value;
            let hot_min_value;
            let hot_max_value;
            let warm_min_value;
            let warm_max_value;
            let cold_min_value;
            let cold_max_value;
            let info_min_value;

            let status;
            let status_id;
            console.log('Emd Value ----->',expected_maturity_date);
            //*******************  Value Parameterisation Calculation Starts *********************************/
            
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
            //***************************  Ends **********************************************/

            let today_date = new Date();
            today_date = moment(today_date).format('YYYY-MM-DD');
            console.log('Today Date ---->',today_date);
    
            //for(let i = 0 ; i < all_lead_sql_res.rows.length ; i++){
            //let emd = dtUtil.ISOtoLocal(expected_maturity_date);
            // console.log('Expected Maturity Date ----> ',expected_maturity_date);
            const date1 = new Date(`'${today_date}'`);
            const date2 = new Date(`'${expected_maturity_date}'`);
            console.log('--->',date1,date2);
            const diffTime = Math.abs(date2 - date1);
            const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24));
            console.log('Diff Days ---->',diffDays,diffTime);
            console.log(`Hot Min Values ===> ${hot_min_value}  && ${hot_max_value}`);
            if(diffDays <= hot_hot_max_value){
                //Case For Hot Hot
                console.log('Case 1');
                status = 'Hot Hot';
                status_id = Hot_Hot;
            }
            if(diffDays >= hot_min_value && diffDays <= hot_max_value){
                //Case For Hot
                console.log('Case 2');
                status = 'Hot';
                status_id = Hot;
            }
            if(diffDays >= warm_min_value && diffDays <= warm_max_value){
                //Case For Warm
                console.log('Case 3');
                status = 'Warm';
                status_id = Warm;
            }
            if(diffDays >= cold_min_value && diffDays <= cold_max_value){
                //Case For Cold
                console.log('Case 4');
                status = 'Cold';
                status_id = Cold;
            }
            if(diffDays >= info_min_value){
                //Case For Informaton
                console.log('Case 5');
                status = 'Information';
                status_id = Information;
            }

            return {status,status_id}
            //}   
    }catch(e){
        console.log('Error In Lead Status On Date Function In Validation ====>',e);
    }
}

// async function leadStageOnDate(){
//     try{
//         console.log('.......');
//         let lead_sql = `SELECT * FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_LEAD_TABLE_NAME} where site_status__c != 'a050w000003QLo7AAG' and lead_open_closed__c = 'a050w000003RF6rAAG'`
//         console.log('Lead SQL -----> ',lead_sql);
//         let lead_sql_res = await client.query(lead_sql);

//         let today_date = new Date();
//         today_date = moment(today_date).format('YYYY-MM-DD');

//         if(lead_sql_res.rows.length > 0){
//             for(let i = 0 ; i < lead_sql_res.rows.length ; i++){   
//                 console.log(`Iteration :::::::  ${i}`);
//                 let lead_sfid = lead_sql_res.rows[i]['sfid'];
//                 console.log('Lead Sfid -------> ',lead_sfid);
                
//                 let lead_stage = lead_sql_res.rows[i]['stage__c'];
//                 console.log('Lead Stage --------> ',lead_stage);

//                 let owner_number__c = lead_sql_res.rows[i]['owner_number__c'];
//                 console.log('Owner Number --------> ',owner_number__c);
                
//                 let lead_created_date = dtUtil.ISOtoLocal(lead_sql_res.rows[i]['lead_created_date__c']);
//                 console.log('Lead Created Date ----> ',lead_created_date);

//                 // let supplier_dealer__c = lead_sql_res.rows[i]['supplier_dealer__c'];
//                 // console.log('Supplier Dealer Id ----> ',supplier_dealer__c);

                

//                 const date1 = new Date(`'${today_date}'`);
//                 const date2 = new Date(`'${lead_created_date}'`);
//                 console.log('--->',date1,date2);
//                 const diffTime = Math.abs(date2 - date1);
//                 const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24));
//                 console.log('Diff Days ---->',diffDays,diffTime); 

//                 if(diffDays > 360){
//                     console.log('Case ::: Diff Days greater than 360');
//                     let update_sql = `update ${process.env.TABLE_SCHEMA_NAME}.${SF_LEAD_TABLE_NAME} set site_status__c='a050w000003QLoHAAW' where sfid = '${lead_sfid}'`
//                     let update_res = await client.query(update_sql)
//                     console.log('Update sql ---->',update_sql);
//                     //console.log('Update Sql Res ---->',update_res);
//                 }
                
//                 if(diffDays > 180 && (lead_stage == 'a050w000003R4zkAAC' || lead_stage == 'a050w000003R4zpAAC')){   //Stage 1 , Stage 2
//                     console.log('Case ::: Diff Days Greater Than !80 And Stage is 1,2');
//                     let update_sql = `update ${process.env.TABLE_SCHEMA_NAME}.${SF_LEAD_TABLE_NAME} set site_status__c='a050w000003QLoHAAW' where sfid = '${lead_sfid}'`
//                     let update_res = await client.query(update_sql)
//                     console.log('Update sql ---->',update_sql);
//                     //console.log('Update Sql Res ---->',update_res);
//                 }

//                 if(diffDays >= 30 && owner_number__c == null ){
//                     console.log('Case ::: Diff Days Is greater Than 30 and Owner Number Is Null');
//                     let update_sql = `update ${process.env.TABLE_SCHEMA_NAME}.${SF_LEAD_TABLE_NAME} set site_status__c='a050w000003QLoHAAW' where sfid = '${lead_sfid}'`
//                     let update_res = await client.query(update_sql)
//                     console.log('Update sql ---->',update_sql);
//                     //console.log('Update Sql Res ---->',update_res);
//                 }

//             }

//             console.log('::::::::::: Cron Ended For Open Lead Other Than Won And Ongoing ::::::::::::::');
//         }
//         console.log('::::::::::: Cron Ended For Open Lead Other Than Won And Ongoing (No record) ::::::::::::::');


//         //2nd Point For Won And Ongoing Lead
//         console.log('2nd Point For Won And Ongoing Lead     .......................');
//         let lead_sql_second_case= `SELECT * FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_LEAD_TABLE_NAME} where site_status__c = 'a050w000003QLo7AAG'`
//         console.log('Lead SQL 2nd Point For Won And Ongoing Lead -----> ',lead_sql_second_case);
//         let lead_sql_second_case_res = await client.query(lead_sql_second_case);

//         if(lead_sql_second_case_res.rows.length > 0){
//             for(let i = 0 ; i < lead_sql_second_case_res.rows.length ; i++ ){
//                 let lead_sfid = lead_sql_second_case_res.rows[i]['sfid'];
//                 console.log('Lead Sfid -------> ',lead_sfid);
                
//                 let lead_created_date = dtUtil.ISOtoLocal(lead_sql_second_case_res.rows[i]['lead_created_date__c']);
//                 console.log('Lead Created Date ----> ',lead_created_date);

//                 //let supply_false_count = await suppleDetailsCount(lead_sfid);
//                 //console.log('supply_false_count Values --->',supply_false_count.last_supply_false_count);

//                 let supply_detail_sql = `SELECT * FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_SUPPLY_TABLE_NAME} where lead__c = '${lead_sfid}' order by supply_date__c desc`;
//                 let supply_detail_sql_res = await client.query(supply_detail_sql)

//                 if(supply_detail_sql_res.rows.length > 0){
//                     let supply_date = dtUtil.ISOtoLocal(supply_detail_sql_res.rows[0]['supply_date__c']);
//                     let first_supply = supply_detail_sql_res.rows[0]['first_supply__c'];
//                     let last_supply = supply_detail_sql_res.rows[0]['last_supply__c'];
//                     let going_on_supply = supply_detail_sql_res.rows[0]['on_going_supply__c'];

//                     const supply_date1 = new Date(`'${supply_date}'`);
//                     const lead_created_date2 = new Date(`'${lead_created_date}'`);
//                     console.log('--->',supply_date1,lead_created_date2);
//                     const diffTime = Math.abs(lead_created_date2 - supply_date1);
//                     const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24));
//                     console.log('Diff Days ---->',diffDays,diffTime); 

//                     if(diffDays > 90 && (first_supply == true || going_on_supply == true)){  //For Won And Completed
//                         let update_sql = `update ${process.env.TABLE_SCHEMA_NAME}.${SF_LEAD_TABLE_NAME} set site_status__c='a050w000003QLoCAAW' where sfid = '${lead_sfid}'`
//                         let update_sql_res = await client.query(update_sql);
//                         console.log('Update sql In Case 2 -------> ',update_sql);
//                     }   
//                 }

//             }
//             console.log('::::::::::: Cron Ended For Won And Ongoing And No Last Supply In 90 Days ::::::::::::::');
//         }
//         console.log('::::::::::: Cron Ended For Won And Ongoing And No Last Supply In 90 Days (No record) ::::::::::::::');

//     }catch(e){
//         console.log('Error In Cron Test ---->',e);
//     }
// }

async function getLastEMD(lead_id){
    try{
        let emd_sql ;
        if(lead_id.length > 20){
            emd_sql = `SELECT expected_maturity_date__c FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_LEAD_TABLE_NAME} where pg_id__c = '${lead_id}'`
        }else{
            emd_sql = `SELECT expected_maturity_date__c FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_LEAD_TABLE_NAME} where sfid = '${lead_id}'`
        }
        console.log(`Previous EMD SQL ===> ${emd_sql}`);
        let emd_result = await client.query(emd_sql);
        if(emd_result.rows.length > 0){
            if(emd_result.rows[0]['expected_maturity_date__c'] != null && emd_result.rows[0]['expected_maturity_date__c'] != undefined){
                return dtUtil.ISOtoLocal(emd_result.rows[0]['expected_maturity_date__c'])
            }else{
                return 'no_data'
            }        
        }else{
            return 'no_data'
        }

    }catch(e){
        console.log(`Error In Last EMD Validation ======> ${e}`);
    }
}

async function siteStatusOnTaskOutcome(taskoutcome_id){
    try{
        
        //Dynamic Picklist Ids
        let site_status_sql = `SELECT sfid FROM salesforce.picklist__c where field_name__c = 'Site_Status__c' order by name desc`
        let site_status_sql_res = await client.query(site_status_sql)
        let Won_and_confirmation_pending = site_status_sql_res.rows[1]['sfid']
        let Won_and_completed = site_status_sql_res.rows[2]['sfid']
        let Lost = site_status_sql_res.rows[5]['sfid']

        let task_outcome = `SELECT sfid FROM salesforce.picklist__c where field_name__c = 'Task_Outcome__c' order by name desc`
        let task_outcome_res = await client.query(task_outcome)
        let project_lost = task_outcome_res.rows[3]['sfid']
        let Won_AndConfirmation_Pending_outcome = task_outcome_res.rows[0]['sfid']
        let Won_And_Completed_outcome = task_outcome_res.rows[1]['sfid']
        //Dynamic Picklist Ids End

        let site_status;
        let deciding_count = 0;
        if(taskoutcome_id == project_lost){ //Project Lost
            site_status = Lost //Lost
            deciding_count = 1;
        }
        if(taskoutcome_id == Won_AndConfirmation_Pending_outcome){  //Won And Confirmation Pending
            site_status = Won_and_confirmation_pending //Won and confirmation pending
            deciding_count = 1;
        }
        if(taskoutcome_id == Won_And_Completed_outcome){  //Won And Completed
            site_status = Won_and_completed  //Won And Completed
            deciding_count = 1;
        }

        if(deciding_count == 1){
            return {site_status,deciding_count}
        }else{
            site_status = 'no_change'
            return {site_status,deciding_count}
        }
    }catch(e){
        console.log(`Error In Site Status On Task Outcome Function In Validation ====> ${e}`);
    }
}