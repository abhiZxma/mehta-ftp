var validator = require('validator');
var validation = require(`${PROJECT_DIR}/utility/validation`);
const sort = require(`${PROJECT_DIR}/utility/sort`);
const moment = require('moment');
const uuidv4 = require('uuid/v4');
//const func = require(`${PROJECT_DIR}/utility/functionalUtility`);


module.exports = {
    selectAllQuery,
    SelectAllQry,
    SelectWithSubAllQry,
    fetchAllWithJoinQry,
    getDbResult,
    agentDetail,
    insertRecord,
    updateRecord,
    getAsmHirarchy,
    insertManyRecord,
    attendanceInsertRecord,
    attendanceUpdateRecord,
    fetchAllWithJoinQryNew,
    attendanceUpdateRecordv2,
    insertManyRecordCustom,
    SelectAllQryv2,
    updateManyCustom
};

/**
 * 
 * @param {*} fieldsArray, tableName, WhereClouse, offset, limit, orderBy 
 * @param {*} tableName 
 * @param {*} WhereClouse 
 * @param {*} offset 
 * @param {*} limit 
 * @param {*} orderBy 
 */

function selectAllQuery(param) {

    var fields = param.fields.toString();
    var WhereClouse = param.WhereClouse;
    var tableName = param.tableName;
    var offset = param.offset;
    var limit = param.limit;
    var orderBy = param.orderBy;
    var sql = `SELECT ${fields} FROM ${process.env.TABLE_SCHEMA_NAME}.${tableName}`;
    
    if (WhereClouse != undefined && WhereClouse.length > 0) {
        sql+= ' where';
        
        var couter = 0;
        WhereClouse.forEach(element => {
            if(couter > 0){
                sql+= ' and';
            }
            console.log("sql", sql);
            if(validation.issetNotEmpty(element.type)){
                switch(element.type){
                    case 'IN':
                        teamsMemString = element.fieldValue.join("','");
                        sql+=` ${element.fieldName} IN ('${teamsMemString}')`;
                    break;
                    case 'NOTIN':
                        teamsMemString = element.fieldValue.join("','");
                        sql+=` ${element.fieldName} NOT IN ('${teamsMemString}')`;
                    break;
                    case 'LIKE':
                        sql+=` ${element.fieldName} LIKE '%${element.fieldValue}%'`;
                    break;  
                    case 'GTE':
                        sql+=` ${element.fieldName} >= '${element.fieldValue}'`;
                    break;  
                    case 'LTE':
                        sql+=` ${element.fieldName} <= '${element.fieldValue}'`;
                    break;  
                    case 'BETWEEN':
                        sql+=` ${element.fieldName} BETWEEN ${element.fieldValue}`;
                    break; 
                    case 'NOTNULL':
                        sql+=` ${element.fieldName} is not null`;
                    break;   
                }
            }else{
                sql+=` ${element.fieldName}='${element.fieldValue}'`;
            }
            couter++;
        });
    }

    if(validation.issetNotEmpty(orderBy)){
        sql+=` ${orderBy}`;
    }
    if(validation.issetNotEmpty(offset)){
        sql+=` offset ${offset}`;
    }
    if(validation.issetNotEmpty(limit)){
        sql+=` limit ${limit}`;
    }
    return sql;
}

function SelectAllQry(fieldsArray, tableName, WhereClouse, offset, limit, orderBy ) {
    var fields = fieldsArray.toString();
    var sql = `SELECT ${fields} FROM ${process.env.TABLE_SCHEMA_NAME}.${tableName}`;
    if (WhereClouse != undefined && WhereClouse.length > 0) {
        sql+= ` where`;
        
        var couter = 0;
        WhereClouse.forEach(element => {
            if(couter > 0){
                sql+= ` and`;
            }

            if(element.type!=undefined && element.type!=''){
                switch(element.type){
                    case 'IN':
                        teamsMemString = element.fieldValue.join("','");
                        sql+=` ${element.fieldName} IN ('${teamsMemString}')`;
                    break;
                    case 'NOTIN':
                        teamsMemString = element.fieldValue.join("','");
                        sql+=` ${element.fieldName} NOT IN ('${teamsMemString}')`;
                    break;
                    case 'LIKE':
                        sql+=` ${element.fieldName} LIKE '%${element.fieldValue}%'`;
                    break;  
                    case 'GTE':
                        sql+=` ${element.fieldName} >= '${element.fieldValue}'`;
                    break;  
                    case 'LTE':
                        sql+=` ${element.fieldName} <= '${element.fieldValue}'`;
                    break;  
                    case 'BETWEEN':
                        sql+=` ${element.fieldName} BETWEEN ${element.fieldValue}`;
                    break; 
                    case 'NOTNULL':
                        sql+=` ${element.fieldName} is not null`;
                    break;   
                }
            }else{
                sql+=` ${element.fieldName}='${element.fieldValue}'`;
            }
            couter++;
        });
    }

    console.log('orderBy >>>>> ',orderBy  )
    if(orderBy!=undefined && orderBy!=''){
        sql+=` ${orderBy}`;
    }
    if(offset!=undefined && validator.isInt(offset,{ min: 0, max: 9999999999999 })){
        sql+=` offset ${offset}`;
    }
    if(limit!=undefined && validator.isInt(limit,{ min: 0, max: 1000 })){
        sql+=` limit ${limit}`;
    }
    return sql;
}

function SelectWithSubAllQry(fieldsArray, subQuery, WhereClouse, offset, limit, orderBy ) {
    const fields = fieldsArray.toString();
    var sql = `SELECT ${fields} FROM ${subQuery}`;
    if (WhereClouse != undefined && WhereClouse.length > 0) {
        sql+= ` where`;
        
        var couter = 0;
        WhereClouse.forEach(element => {
            if(couter > 0){
                sql+= ` and`;
            }

            if(element.type!=undefined && element.type!=''){
                switch(element.type){
                    case 'IN':
                        teamsMemString = element.fieldValue.join("','");
                        sql+=` ${element.fieldName} IN ('${teamsMemString}')`;
                    break;
                    case 'LIKE':
                        sql+=` ${element.fieldName} LIKE '%${element.fieldValue}%'`;
                    break;  
                    case 'GTE':
                        sql+=` ${element.fieldName} >= '${element.fieldValue}'`;
                    break;  
                    case 'LTE':
                        sql+=` ${element.fieldName} <= '${element.fieldValue}'`;
                    break;  
                    case 'BETWEEN':
                        sql+=` ${element.fieldName} BETWEEN ${element.fieldValue}`;
                    break;   
                    case 'NOTNULL':
                        sql+=` ${element.fieldName} is not null`;
                    break;  
                }
            }else{
                sql+=` ${element.fieldName}='${element.fieldValue}'`;
            }
            couter++;
        });
    }

    console.log('orderBy >>>>> ',orderBy  )
    if(orderBy!=undefined && orderBy!=''){
        sql+=` ${orderBy}`;
    }
    if(offset!=undefined && validator.isInt(offset,{ min: 0, max: 9999999999999 })){
        sql+=` offset ${offset}`;
    }
    if(limit!=undefined && validator.isInt(limit,{ min: 0, max: 1000 })){
        sql+=` limit ${limit}`;
    }
    return sql;
}

function fetchAllWithJoinQry(fieldsArray, tableName,joins, WhereClouse, offset, limit, orderBy ) {
    const fields = fieldsArray.toString();
    var sql = `SELECT ${fields} FROM ${process.env.TABLE_SCHEMA_NAME}.${tableName}`;
    var joinString = ``;
    if (joins != undefined && joins.length > 0) {
       
        joins.forEach(async element => {
           
            joinString += ` ${element.type} JOIN ${process.env.TABLE_SCHEMA_NAME}.${element.table_name} ON ${element.p_table_field} = ${element.s_table_field}`;
            
        });
        sql+=joinString;
    }

    if (WhereClouse != undefined && WhereClouse.length > 0) {
        sql+=` where`;
        var couter = 0;
        WhereClouse.forEach(element => {
            if(couter > 0){
                sql += ` and`;
            }
            if(element.type!=undefined && element.type!=''){
                switch(element.type){
                    case 'IN':
                        teamsMemString = element.fieldValue.join("','");
                        sql+=` ${element.fieldName} IN ('${teamsMemString}')`;
                    break;
                    case 'LIKE':
                        sql+=` ${element.fieldName} LIKE '%${element.fieldValue}%'`;
                    break;  
                    case 'GTE':
                        sql+=` ${element.fieldName} >= '${element.fieldValue}'`;
                    break;
                    case 'GT':
                        sql+=` ${element.fieldName} > '${element.fieldValue}'`;
                    break;
                    case 'LT':
                        sql+=` ${element.fieldName} < '${element.fieldValue}'`;
                    break;
                    case 'LTE':
                        sql+=` ${element.fieldName} <= '${element.fieldValue}'`;
                    break;  
                    case 'BETWEEN':
                        sql+=` ${element.fieldName} BETWEEN ${element.fieldValue}`;
                    break;  
                    case 'NOTNULL':
                        sql+=` ${element.fieldName} is not null `;
                    break;  

                }
            }else{
                sql+=` ${element.fieldName}='${element.fieldValue}'`;
            }
            couter++;
        });
    }
    
    if(orderBy!=undefined && orderBy!=''){
        sql +=` ${orderBy}`;
    }
    if(offset!=undefined && validator.isInt(offset,{ min: 0, max: 9999999999999 })){
        sql+=` offset ${offset}`;
    }
    if(limit!=undefined && validator.isInt(limit,{ min: 0, max: 1000 })){
        sql+=` limit ${limit}`;
    }
    return sql;
}

function fetchAllWithJoinQryNew(fieldsArray, tableName,joins,joinsand ,WhereClouse, offset, limit, orderBy ) {
    const fields = fieldsArray.toString();
    var sql = `SELECT ${fields} FROM ${process.env.TABLE_SCHEMA_NAME}.${tableName}`;
    var joinString = ``;
    if (joins != undefined && joins.length > 0) {
       
        joins.forEach(async element => {
            joinString += ` ${element.type} JOIN ${process.env.TABLE_SCHEMA_NAME}.${element.table_name} ON ${element.p_table_field} = ${element.s_table_field}`; 
        });

        if(joinsand != undefined ){
            joinsand.forEach(async element => {
                joinString += ` AND ${element.fieldName} = ${element.fieldValue}`; 
            })
        }
        sql+=joinString;
    }

    if (WhereClouse != undefined && WhereClouse.length > 0) {
        sql+=` where`;
        var couter = 0;
        WhereClouse.forEach(element => {
            if(couter > 0){
                sql += ` and`;
            }
            if(element.type!=undefined && element.type!=''){
                switch(element.type){
                    case 'IN':
                        teamsMemString = element.fieldValue.join("','");
                        sql+=` ${element.fieldName} IN ('${teamsMemString}')`;
                    break;
                    case 'LIKE':
                        sql+=` ${element.fieldName} LIKE '%${element.fieldValue}%'`;
                    break;  
                    case 'GTE':
                        sql+=` ${element.fieldName} >= '${element.fieldValue}'`;
                    break;
                    case 'GT':
                        sql+=` ${element.fieldName} > '${element.fieldValue}'`;
                    break;
                    case 'LT':
                        sql+=` ${element.fieldName} < '${element.fieldValue}'`;
                    break;
                    case 'LTE':
                        sql+=` ${element.fieldName} <= '${element.fieldValue}'`;
                    break;  
                    case 'BETWEEN':
                        sql+=` ${element.fieldName} BETWEEN ${element.fieldValue}`;
                    break;  
                    case 'NOTNULL':
                        sql+=` ${element.fieldName} is not null `;
                    break;  

                }
            }else{
                sql+=` ${element.fieldName}='${element.fieldValue}'`;
            }
            couter++;
        });
    }
    
    if(orderBy!=undefined && orderBy!=''){
        sql +=` ${orderBy}`;
    }
    if(offset!=undefined && validator.isInt(offset,{ min: 0, max: 9999999999999 })){
        sql+=` offset ${offset}`;
    }
    if(limit!=undefined && validator.isInt(limit,{ min: 0, max: 1000 })){
        sql+=` limit ${limit}`;
    }
    return sql;
} 

async function getDbResult(sql) {
    return await client.query(sql)
        .then(data => {
            console.log('INFO::: Fetch DB result');
            return data;
        })
        .catch(err => {
            console.log('err ====>>>  ',err);
            return [];
        });
}

async function insertRecord(fieldsToBeInsert, fieldValues, tableName, returnIds){
   

    sql = `INSERT into ${process.env.TABLE_SCHEMA_NAME}.${tableName} (${fieldsToBeInsert}) VALUES(`;
    if(fieldValues.length > 0){
        var counter = 1;
        fieldValues.forEach(element => {
            if(counter > 1){ sql += `,`; }
            sql += `$${counter}`;
            counter++
        })
    }
    sql += `) RETURNING id`;
    if(returnIds!=undefined){
        sql +=` ${returnIds}`;
    }
    console.log('INFO ::::::  SQL::::  ', sql);
    return await client.query(sql,fieldValues)
        .then(data => { 
            console.log(` INFO::::: INSERT RESPONSE table =${tableName} >>>>> `,data)
            if(data.rowCount > 0){
                return { "success": true, "message": "", "data": data.rows };

            }else{
                return { "success": false, "message": "Error while create record. Please try again.", "data": {} };
            }
        }).catch(err => {
            console.log('Error::: Catch 162 >>>> ', err);
            return { "success": false, "message": "Error while insert", "data": {} };
        });

}

async function insertManyRecord(fieldsToBeInsert, fieldValues, tableName, returnIds){
   

    sql = `INSERT into ${process.env.TABLE_SCHEMA_NAME}.${tableName} (${fieldsToBeInsert}) VALUES`;
    if(fieldValues.length > 0){
        let counter = 1;  // for giving '(' / ')'
        let counter2 = 1;     // for giving ','
        fieldValues.forEach((fieldValue)=> {
            if(counter == 1){ 
                sql += `(`; 
            } else {
                sql += ',('
            }
            let data = Object.values(fieldValue);
            data.forEach(value => {
                if (counter2 == 1) {
                    sql += `'${value}'`;
                } else{
                    sql += `, '${value}'`;
                }
                counter2++
            })
            counter2 = 1;
            sql += `)`;
            counter++
        })
    }
    sql += ` RETURNING id`;
    if(returnIds!=undefined){
        sql +=` ${returnIds}`;
    }
    console.log('INFO ::::::  SQL::::  ', sql);
    return await client.query(sql)
        .then(data => { 
            console.log(` INFO::::: INSERT RESPONSE table =${tableName} >>>>> `,data)
            if(data.rowCount > 0){
                return { "success": true, "message": "", "data": data.rows };

            }else{
                return { "success": false, "message": "Error while create record. Please try again.", "data": {} };
            }
        }).catch(err => {
            console.log('Error::: Catch 162 >>>> ', err);
            return { "success": false, "message": "Error while insert", "data": {} };
        });

}

async function updateRecord(tableName, fieldValue, WhereClouse){
    try {

        //sql = `update zoxima.${tableName} set End_Day__c='true', End_Time__c='${attendance_time}' where Team__c='${agentid}' and Attendance_Date__c='${attendance_date}'`;
        
         var sql = `update ${process.env.TABLE_SCHEMA_NAME}.${tableName} set`;


        counter = 1;
        fieldValue.forEach(element => {
            if(counter > 1)
                sql+=`,`;
            if(element.type!=undefined && element.type == 'BOOLEAN')
                sql +=` ${element.field}=${element.value}`;
            else
                sql +=` ${element.field}='${element.value}'`;
            counter++;
        });

        sql +=` where `;


        counter = 1;
        WhereClouse.forEach(element => {
            if(counter > 1)
                sql+=` and `;
            if(element.type!=undefined && element.type=='IN'){
                teamsMemString =element.value.join("','");
                sql +=` ${element.field} IN ('${teamsMemString}')`;
            }  else
                sql +=` ${element.field}='${element.value}'`;
            counter++;
        });

        console.log(`INFO::::: ${sql}`);

        return await client.query(sql)
            .then(data => {
                if(data.rowCount > 0){
                    return { "success": true, "message": "Record updated successfully.","data":data };
                }else{
                    return { "success": false, "message": "Record updated failed.","data":{} };
                }
            }).catch(err => {
                console.log('ERROR:::: err 137 >>>> ', err);
                return { "success": false, "message": "Error while update record." };
            });
    } catch (e) {
        return { "success": false, "message": "Error while update record." };
    }
  
}

async function agentDetail(agentId){
    if (validation.issetNotEmpty(agentId)) {
        fieldsArray = [
            `team__c.member_type__c as member_type`,
            `team__c.email__c as email`, `team__c.name as team_member_name`,
            `team__c.dob__c as dob`, `team__c.designation__c as designation`,
            `team__c.phone_no__c as phone_no`,
            `team__c.Business__c as business`,
            `team__c.Manager__c as manager_id`,
            `team__c.sfid as team_id`
        ];
        tableName = `team__c`;
        WhereClouse = [];
            WhereClouse.push({ "fieldName": "sfid", "fieldValue": agentId  })
        
        orderBy = '';
        var sql = SelectAllQry(fieldsArray, tableName, WhereClouse, '0', '1', orderBy );
        console.log(`INFO:::: GET AGENT DETAIL: ${sql}`);
        var result =  await getDbResult(sql);
        return result;
    }else{
        return false;
    }
}

//getAsmHirarchy('a0H1m000001Owv4EAC');
async function getAsmHirarchy(agentid) {
    var team = {};
    team['ASM'] = [];
    team['PSM'] = [];
    team['memberType'] = '';
    team['success'] = true;
    try {
        myDetails = await agentDetail(agentid);
        
        if (myDetails.rowCount > 0) {
            team['memberType'] = myDetails.rows[0].member_type;
            var sql = '';
            if (myDetails.rowCount > 0 && myDetails.rows[0].member_type == 'PSM') {
                team['PSM'].push(agentid)
            } else {
                sql = `WITH RECURSIVE subordinates AS (
                SELECT
                sfid,
                manager__c,
                name,
                member_type__c
                FROM
                cns.team__c
                WHERE
                sfid = '${agentid}'
                UNION
                SELECT
                    e.sfid,
                    e.manager__c,
                    e.name,
                    e.member_type__c
                FROM
                    cns.team__c e
                INNER JOIN subordinates s ON s.sfid = e.manager__c
            ) SELECT
                *
            FROM
                subordinates`;
                var result = await getDbResult(sql);
                if (result.rows.length > 0) {
                    for (i in result.rows) {
                        if (result.rows[i].member_type__c == 'PSM') {
                            team['PSM'].push(result.rows[i].sfid);
                        } else {
                            team['ASM'].push(result.rows[i].sfid);
                        }
                    }
                }else{
                    team['success'] = false;
                }
            }
            
            console.log('result  > ', team)
            return team;
        }
    } catch (e) {
        team['success'] = false;
        return team;
    }
}

async function attendanceInsertRecord(latitude, longitude, checkin_address__c, attendance_date, attendance_time, team__c, type, absent_reason) {

    try {
        //let present_picklist_value = await func.getPicklistSfid('Attendence__c','Attendance_Type__c','Present')
        let present_picklist_value_sql = `SELECT sfid,name FROM salesforce.picklist__c where object_name__c='Attendence__c' AND field_name__c='Attendance_Type__c' AND name='Present'`
        let present_picklist_value_sql_res = await client.query(present_picklist_value_sql)
        let present_picklist_value = present_picklist_value_sql_res.rows[0]['sfid']
        let current_datetime = moment().format('YYYY-MM-DD HH:mm:ss');
        console.log(`attendance time ---> ${attendance_time}`)
        //console.log('=============================================');
        //console.log(arguments);
        let UUID_attendance = uuidv4();
        let sql = null;
        let params = null;
        if(type == present_picklist_value){
            sql = `INSERT into ${process.env.TABLE_SCHEMA_NAME}.${SF_ATTENDANCE_TABLE_NAME} (pg_id__c, checkin_location__latitude__s, checkin_location__longitude__s,checkin_address__c, start_day__c, emp_id__c, end_day__c,attendance_date__c,start_time__c,createddate, attendance_type__c) VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11) RETURNING id`
            params = [UUID_attendance, latitude, longitude, checkin_address__c, true, team__c, false, attendance_date, attendance_time, current_datetime, type]
            console.log('sql',sql);
        }else{
            sql = `INSERT into ${process.env.TABLE_SCHEMA_NAME}.${SF_ATTENDANCE_TABLE_NAME} (pg_id__c, start_day__c, emp_id__c, attendance_date__c,start_time__c,createddate, attendance_type__c, absent_reason__c) VALUES($1, $2, $3, $4, $5, $6, $7, $8) RETURNING id` 
            params = [UUID_attendance, false, team__c, attendance_date, attendance_time, current_datetime, type, absent_reason]
        }
        return await client.query(sql,params)
            // `INSERT into ${process.env.TABLE_SCHEMA_NAME}.attendance__c (pg_id__c, checkin_location__latitude__s, checkin_location__longitude__s,checkin_address__c, start_day__c, flsp__c, End_Day__c,Attendance_Date__c,createddate, type__c, present_type__c) VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11) RETURNING id`,
            // [UUID_attendance, latitude, longitude, checkin_address__c, true, team__c, false, attendance_date, current_datetime,'Present', present_type])
            .then(data => {
                return { "success": true, "message": "Attendance mark successfully", "data": {id:UUID_attendance} };
            }).catch(err => {
                console.log("Error :::::>>>>>>> 003 :::::::", err);
                return { "success": false, "message": "Error while insert", "data": {} };
            });
    } catch (e) {
        console.log("Error :::::>>>>>>> 004 :::::::", e);
        return { "success": false, "message": "Error while insert", "data": {} };
    }
}

async function attendanceUpdateRecord(attendance_date, attendance_time, team__c, latitude, longitude, checkout_address, working_hours_start__c, working_hours_end__c, visit_working_hour__c, exact_visit_time_exhausted__c) {

    try {

        let sql = `update ${process.env.TABLE_SCHEMA_NAME}.${SF_ATTENDANCE_TABLE_NAME} 
        set end_day__c='true', end_time__c='${attendance_time}', checkout_location__latitude__s='${latitude}', 
        checkout_location__longitude__s='${longitude}', checkout_address__c='${checkout_address}', 
        working_hours_start__c='${working_hours_start__c}',
        working_hours_end__c='${working_hours_end__c}',
        visit_working_hour__c='${visit_working_hour__c}'
         where emp_id__c='${team__c}' and attendance_date__c='${attendance_date}'`;
        console.log('SQL1.................', sql)
        return await client.query(sql)
            .then(data => {
                console.log('INFO:::: Data >>>> ', data);
                return { "success": true, "message": "Attendance updated successfully." };
            }).catch(err => {
                console.log("Error :::::>>>>>>> 005 :::::::", err);
                return { "success": false, "message": "Error while update record." };
            });
    } catch (e) {
        console.log("Error :::::>>>>>>> 006 :::::::", e);
        return { "success": false, "message": "Error while update record." };
    }
}

async function attendanceUpdateRecordv2(attendance_date, attendance_time, team__c, latitude, longitude, checkout_address, working_hour__c) {

    try {

        let sql = `update ${process.env.TABLE_SCHEMA_NAME}.${SF_ATTENDANCE_TABLE_NAME} 
        set end_day__c='true', end_time__c='${attendance_time}', checkout_location__latitude__s='${latitude}', 
        checkout_location__longitude__s='${longitude}', checkout_address__c='${checkout_address}',
        working_hour__c='${working_hour__c}'
         where emp_id__c='${team__c}' and attendance_date__c='${attendance_date}'`;
        console.log('SQL.................', sql)
        return await client.query(sql)
            .then(data => {
                console.log('INFO:::: Data >>>> ', data);
                return { "success": true, "message": "Attendance updated successfully." };
            }).catch(err => {
                console.log("Error :::::>>>>>>> 005 :::::::", err);
                return { "success": false, "message": "Error while update record." };
            });
    } catch (e) {
        console.log("Error :::::>>>>>>> 006 :::::::", e);
        return { "success": false, "message": "Error while update record." };
    }
}

async function insertManyRecordCustom(fieldsToBeInsert, fieldValues, tableName, returnIds){
    sql = `INSERT into ${process.env.TABLE_SCHEMA_NAME}.${tableName} (${fieldsToBeInsert}) VALUES`;
    if(fieldValues.length > 0){
        let counter = 1;  // for giving '(' / ')'
        let counter2 = 1;     // for giving ','
        fieldValues.forEach((fieldValue)=> {
            if(counter == 1){ 
                sql += `(`; 
            } else {
                sql += ',('
            }
            let data = Object.values(fieldValue);
            data.forEach(value => {
                if (counter2 == 1) {
                    sql += `'${value}'`;
                } else{
                    sql += `, '${value}'`;
                }
                counter2++
            })
            counter2 = 1;
            sql += `)`;
            counter++
        })
    }
    sql += ` ON CONFLICT (emp_id__c, pjp_date__c) 
    DO NOTHING RETURNING id`;
    if(returnIds!=undefined){
        sql +=` ${returnIds}`;
    }
    console.log('INFO ::::::  SQL::::  ', sql);
    return await client.query(sql)
        .then(data => { 
            console.log(` INFO::::: INSERT RESPONSE table =${tableName} >>>>> `,data)
            if(data.rowCount > 0){
                return { "success": true, "message": "", "data": data.rows };

            }else{
                return { "success": false, "message": "Error while create record. Please try again.", "data": {} };
            }
        }).catch(err => {
            console.log('Error::: Catch 162 >>>> ', err);
            console.log('============================Failed SQL START============================', sql)
            console.log('============================Failed SQL END============================')
            return { "success": false, "message": "Error while insert", "data": {} };
        });

}
function SelectAllQryv2(fieldsArray, tableName, WhereClouse, offset, limit, orderBy ) {
    var fields = fieldsArray.toString();
    var sql = `SELECT ${fields} FROM ${process.env.TABLE_SCHEMA_NAME}.${tableName}`;
    if (WhereClouse != undefined && WhereClouse.length > 0) {
        sql+= ` where`;
        
        var couter = 0;
        WhereClouse.forEach(element => {
            console.log(`In SQL FUNCTION ----> ${element.condition}`);
            if(couter > 0 && element.condition == 'or' && element.condition!=undefined && element.condition!=''){
                sql+= ` or`;
            }
            if(couter > 0 && element.condition == 'and' && element.condition!=undefined && element.condition!=''){
                sql+= ` and`;
            }

            if(element.type!=undefined && element.type!=''){
                switch(element.type){
                    case 'IN':
                        teamsMemString = element.fieldValue.join("','");
                        sql+=` ${element.fieldName} IN ('${teamsMemString}')`;
                    break;
                    case 'NOTIN':
                        teamsMemString = element.fieldValue.join("','");
                        sql+=` ${element.fieldName} NOT IN ('${teamsMemString}')`;
                    break;
                    case 'LIKE':
                        sql+=` ${element.fieldName} LIKE '%${element.fieldValue}%'`;
                    break;  
                    case 'GTE':
                        sql+=` ${element.fieldName} >= '${element.fieldValue}'`;
                    break;  
                    case 'LTE':
                        sql+=` ${element.fieldName} <= '${element.fieldValue}'`;
                    break;  
                    case 'GT':
                        sql+=` ${element.fieldName} > '${element.fieldValue}'`;
                    break;  
                    case 'LT':
                        sql+=` ${element.fieldName} < '${element.fieldValue}'`;
                    break; 
                    case 'BETWEEN':
                        sql+=` ${element.fieldName} BETWEEN ${element.fieldValue}`;
                    break; 
                    case 'NOTNULL':
                        sql+=` ${element.fieldName} is not null`;
                    break;   
                }
            }else{
                sql+=` ${element.fieldName}='${element.fieldValue}'`;
            }
            couter++;
        });
    }

    console.log('orderBy >>>>> ',orderBy  )
    if(orderBy!=undefined && orderBy!=''){
        sql+=` ${orderBy}`;
    }
    if(offset!=undefined && validator.isInt(offset,{ min: 0, max: 9999999999999 })){
        sql+=` offset ${offset}`;
    }
    if(limit!=undefined && validator.isInt(limit,{ min: 0, max: 1000 })){
        sql+=` limit ${limit}`;
    }
    return sql;
}
/* field_value{
    pan_no__c:[
        {sfid:0010w00000sse1SAAQ,value:133},
        {sfid:0010w00000sse1RAAQ,value:133}
    ] 
} */
async function updateManyCustom(field_value, tableName){
    let sql = `UPDATE ${process.env.TABLE_SCHEMA_NAME}.${tableName} SET `;
    if(Object.keys(field_value).length > 0){
        for (let i = 0; i < Object.keys(field_value).length; i++) {
            const col_name = Object.keys(field_value)[i];
            const last_col=Object.keys(field_value)[Object.keys(field_value).length-1]
            const values=await field_value[col_name]
            console.log("values",values.length)
            sql+=`${col_name} = CASE `
            let first_val_name=Object.keys(values[0])[0]
            console.log("first_val_name--->",first_val_name)
            sql+=`${first_val_name} `

            if(col_name==last_col){
                values.map((val_obj)=>{
                    console.log("val_obj----->",val_obj)
                     if(val_obj!=values[values.length-1]){
                        sql+=`WHEN '${val_obj[first_val_name]}' THEN '${val_obj['value']}' `
                     }else{
                        sql+=`WHEN '${val_obj[first_val_name]}' THEN '${val_obj['value']}' ELSE ${col_name} END `
                     }   
                })
            }else{
                values.map((val_obj)=>{
                    console.log("val_obj----->",val_obj)
                     if(val_obj!=values[values.length-1]){
                        sql+=`WHEN '${val_obj[first_val_name]}' THEN '${val_obj['value']}' `
                     }else{
                        sql+=`WHEN '${val_obj[first_val_name]}' THEN '${val_obj['value']}' ELSE ${col_name} END,`
                     }   
                })
            }
            
        }
    }
    console.log("sqlllllllll",sql)
}
