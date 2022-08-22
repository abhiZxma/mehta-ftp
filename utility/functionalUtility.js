var validator = require('validator');
var validation = require(`${PROJECT_DIR}/utility/validation`);
var rp = require('request-promise');
const sort = require(`${PROJECT_DIR}/utility/sort`);
const moment = require('moment');
const qry = require(`${PROJECT_DIR}/utility/selectQueries`);
const email = require(`${PROJECT_DIR}/utility/sendEmail.js`);
const dtUtil = require(`${PROJECT_DIR}/utility/dateUtility`);
var polyline = require('@mapbox/polyline');
var _ = require('lodash');
const turf = require('@turf/turf')


module.exports = {
    getDistanceTravlled,
    errorlog,
    getCCFromTown,
    distanceP2p,
    findByMatchingProperties,
    getMyTeamMember,
    getAllTeam,
    teamMemberLogic,
    getLocationAddr,
    teamWiseTerritoryData,
    territoryWiseAccount,
    territoryWiseTown,
    townWiseBeatGrid,
    beatGridWiseParty,
    getRegionWiseData,
    getLeadFromTerritory,
    getTeamManager,
    getUniqueValuesFromArrayOfObject,
    getUniqueValuesFromArrayOfObjectname,
    getTeamDayType,
    townWiseGrid,
    getMultiPicklistSfids,
    getAllTownFunc,
    getWorkingHour,
    getTeam,
    getVisitDay,
    mailErrorLog,
    getTeamAreaWiseFunc,
    getGeographicalTownFromTown,
    getTeamFromTerritory,
    // getRegionWiseDataV2,
    // townWiseBeatGridv2,
    // townWiseGridv2
    getTeamRegionWiseData,
    getTeamRegionWiseData2,
    getAllAreaFromTeam,
    getDayWorkingHour,
    getSubordinatesOnArea,
    getUpperAndLowerHerrarichy,
    getLowerHerrarichyAndCC,
    getBranchFromTown,
    leadDropMailAlert,
    getBranchFromLowerArea,
    getTownFromUpperArea,
    townWiseBeatGridV2,
    getTeamFromLobDivision,
    getPicklistSfid,
    getTerritoryFromLowerArea,
    getTeamsDetail,
    getWorkingHoursAndDistance,
    getTownFromTerritory,
    townMatchFunction,
    getNewGridName,
    setEncodedLatLongPath,
    getPathLatLongInArrayFormat,
    getDirection,
    getTerritoryFromAnyAreaOrTeam,
    getTerritoryFromAnyArea,
    getPathLatLongForSpecificVisitGrid,
    getDistanceOnWaypoints,
    getWorkingHourInMinutes,
    getDayWorkingHourInMinutes,
    getVisitData,
    getPjpData,
    getTotalAttendenceData,
    getTotalWorkingData,
    updateVisitDataInPjp,
    updateAttendanceDataInPjp,
    GetDistanceLengthTurf,
    getDistanceHarvsine,
    getAnyAreaFromArea,
    deletePjpRelatedData,
    createPjpData,
    getLowerAreaFromBranch
};

let area_type_name_number_map = {
    1: 'Zone',
    2: 'Nation',
    3: 'Region',
    4: 'Branch',
    5: 'Territory',
    6: 'ASM',
    7: 'Town',
    8: 'Grid',
    9: 'Beat',
    'Zone': 1,
    'Nation': 2,
    'Region': 3,
    'Branch': 4,
    'Territory': 5,
    'ASM': 6,
    'Town': 7,
    'Grid': 8,
    'Beat': 9
}

let area_level_map = {};
let area_sfid_map = {};

async function getAllArea(){
    
}

async function getTeam(team_id, required_area_level){
/**
 * 
    area_level_map = {
        'Level-1': 'a050w000002jazRAAQ',
        'Level-2': 'a050w000002jazWAAQ',
        'Level-3': 'a050w000002jazbAAA',
        'Level-4': 'a050w000002jazqAAA',
        'Level-5': 'a050w000002jb05AAA',
        'Level-6': 'a050w000002jb0AAAQ',
        'Level-7': 'a050w000002jb0UAAQ',
        'Level-8': 'a050w000002jb0ZAAQ'
        a050w000002jb0AAAQ: 'Level-6',
        a050w000002jazRAAQ: 'Level-1',
        a050w000002jazWAAQ: 'Level-2',
        a050w000002jazbAAA: 'Level-3',
        a050w000002jazqAAA: 'Level-4',
        a050w000002jb05AAA: 'Level-5',
        a050w000002jb0UAAQ: 'Level-7',
        a050w000002jb0ZAAQ: 'Level-8',
}
    area_sfid_map :::: {
        Nation: 'a050w000002jNn1AAE',
        Zone: 'a050w000002jNn0AAE',
        Region: 'a050w000002jNn2AAE',
        Branch: 'a050w000002jNn3AAE',
        ASM: 'a050w000002jb0FAAQ',
        Territory: 'a050w000002jNn4AAE',
        Town: 'a050w000002jb18AAA',
        Grid: 'a050w000002jb1DAAQ',
        Beat: 'a050w000003QLJHAA4'
        a050w000002jNn0AAE: 'Zone',
        a050w000002jNn1AAE: 'Nation',
        a050w000002jNn2AAE: 'Region',
        a050w000002jNn3AAE: 'Branch',
        a050w000002jb0FAAQ: 'ASM',
        a050w000002jNn4AAE: 'Territory',
        a050w000002jb18AAA: 'Town',
        a050w000002jb1DAAQ: 'Grid',
        a050w000003QLJHAA4: 'Beat',
    }
 */
    try{
        let area_picklist_sql = `SELECT * FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_PICKLIST_TABLE_NAME} 
        WHERE ${SF_PICKLIST_TABLE_NAME}.object_name__c='Area1__c' AND ${SF_PICKLIST_TABLE_NAME}.field_name__c= 'Area_Type__c';`;
        let area_picklist = await client.query(area_picklist_sql);
        area_picklist = area_picklist.rows;
        
        // let area2_picklist_sql = `SELECT * FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_PICKLIST_TABLE_NAME} 
        // WHERE ${SF_PICKLIST_TABLE_NAME}.object_name__c='Area2__c' AND ${SF_PICKLIST_TABLE_NAME}.field_name__c= 'Area_Type__c';`;
        // let area2_picklist = await client.query(area2_picklist_sql);
        // area2_picklist = area2_picklist.rows;
        area_picklist.forEach((area)=> {
            area_type_name_number_map[area['sfid']] = area['name']
            area_type_name_number_map[area['name']] = area['sfid']
        });
        // area2_picklist.forEach((area)=> {
        //     area_type_name_number_map[area['sfid']] = area['name']
        //     area_type_name_number_map[area['name']] = area['sfid']
        // });
        
        let area_level_picklist_sql = `SELECT * FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_PICKLIST_TABLE_NAME} 
        WHERE ${SF_PICKLIST_TABLE_NAME}.object_name__c='Area1__c' AND ${SF_PICKLIST_TABLE_NAME}.field_name__c= 'Area_Level__c';`;
        let area_level_picklist = await client.query(area_level_picklist_sql);
        area_level_picklist = area_level_picklist.rows;
        
        // let area2_level_picklist_sql = `SELECT * FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_PICKLIST_TABLE_NAME} 
        // WHERE ${SF_PICKLIST_TABLE_NAME}.object_name__c='Area2__c' AND ${SF_PICKLIST_TABLE_NAME}.field_name__c= 'Area_Level__c';`;
        // let area2_level_picklist = await client.query(area2_level_picklist_sql);
        // area2_level_picklist = area2_level_picklist.rows;
        area_level_picklist.forEach((area)=> {
            area_level_map[area['sfid']] = area['name']
            area_level_map[area['name']] = area['sfid']
        });
        // area2_level_picklist.forEach((area)=> {
        //     area_level_map[area['sfid']] = area['name']
        //     area_level_map[area['name']] = area['sfid']
        // });
        console.log('area_level_map ::::', area_level_map);
        console.log('area_type_name_number_map ::::', area_type_name_number_map);
        
        const fields = `${SF_TEAM_TABLE_NAME}.sfid, ${SF_TEAM_TABLE_NAME}.lob__c, ${SF_TEAM_TABLE_NAME}.branch__c, ${SF_TEAM_TABLE_NAME}.division__c,
        ${SF_TEAM_TABLE_NAME}.employee_type__c, ${SF_TEAM_TABLE_NAME}.designation__c, ${SF_TEAM_TERRITORY_MAPPING_TABLE_NAME}.territory_code__c`;
        
        let team_sql = `SELECT ${fields} FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_TEAM_TABLE_NAME} 
            LEFT JOIN ${process.env.TABLE_SCHEMA_NAME}.${SF_TEAM_TERRITORY_MAPPING_TABLE_NAME} ON 
            ${SF_TEAM_TERRITORY_MAPPING_TABLE_NAME}.lob__c = ${SF_TEAM_TABLE_NAME}.lob__c AND
            ${SF_TEAM_TERRITORY_MAPPING_TABLE_NAME}.division__c = ${SF_TEAM_TABLE_NAME}.division__c AND
            ${SF_TEAM_TERRITORY_MAPPING_TABLE_NAME}.team_member_id__c = ${SF_TEAM_TABLE_NAME}.sfid
            WHERE ${SF_TEAM_TABLE_NAME}.sfid='${team_id}';`;
        let team = await client.query(team_sql);
        console.log('Team Data SQL for last record', team_sql)
        console.log('Team Data for last record', team.rows)
        if(team.rows.length > 0){
            let team_data = {
                sfid: team.rows[0]['sfid'],
                lob__c: team.rows[0]['branch__c'],
                division__c: team.rows[0]['division__c'],
                employee_type__c: team.rows[0]['employee_type__c'],
                designation__c: team.rows[0]['designation__c']
        }
        let territory_code = team.rows.map((territory_data)=> {
            if(territory_data['territory_code__c']){
                return territory_data['territory_code__c']
            }else{
                return null;
            }
        })
        // Now check area with the help of team territory data
        let fieldsArray = ['*'];
        let WhereClouse = [];
        WhereClouse.push({ "fieldName": "sfid", "fieldValue": territory_code, "type": 'IN'}); 
        
        let area_sql = await qry.SelectAllQry(fieldsArray, SF_AREA_1_TABLE_NAME, WhereClouse, '0', '10000', ' order by createddate desc' );
        let area = await client.query(area_sql);
        console.log('SQL ::::', area_sql);
            /** This is sample Area data
             *  [
                {
                    "parent_code__c": "a010w000003tnOlAAI",
                    "territory_type__c": null,
                    "name": "AP-TG",
                    "branch_type__c": null,
                    "isdeleted": false,
                    "systemmodstamp": "2021-11-11T02:10:34.000Z",
                    "createddate": "2021-11-11T02:10:34.000Z",
                    "name__c": "Andhra Pradesh & Telangana",
                    "sfid": "a010w000003tnPbAAI",
                    "id": 3,
                    "_hc_lastop": "SYNCED",
                    "_hc_err": null,
                    "area_level__c": "a050w000002jazbAAA",  // Level-3
                    "area_type__c": "a050w000002jNn2AAE"    // Region
                }
            ],
            */
            //    return new Promise((resolve, reject) => {
            if(area.rows && area.rows.length > 0){
                console.log('Starting getAnotherLevelArea ', area.rows)
                let area_level = area_level_map[area.rows[0]['area_level__c']];
                console.log("area_level ::::", area_level);
                let area_arr = [];
                area.rows.forEach((ar)=> {
                    if(ar['sfid']){
                        area_arr.push(ar['sfid'])
                    }
                })
                let data = await getAnotherLevelArea(team_data, area_arr, required_area_level, area_level);
                console.log(data);
                return data;
                        // .then((data)=> {
                        //     if(data){
                        //         resolve(data)
                        //     }else {
                        //         reject('No data found')
                        //     }
                        // })
            }
            // else{
            //     response.status = 200;
            //     response.response = { success: false, message: 'No area found for team_id' };
            //     return response;
            // }
            // });
        }else {
            response.status = 200;
            response.response = { success: false, message: 'No data found for team_id' };
            return response;
        }
    }catch(e){
        console.log('Error in Territory Wise Account Function ====>',e);
    }
}

async function getAnotherLevelArea(team_data, area_arr = [], required_area_level, current_area_level){
    console.log('Inside getAnotherLevelArea')
    console.log(arguments)
    /**
    [Arguments] {   // this is for getting account
        '0': [ 'a030w0000080nHuAAI' ],
        '1': [ 'a010w000003tnPbAAI' ],
        '2': 'Level-5',
        '3': 'Level-3'
    }
    */
    if(required_area_level == current_area_level){
       return area_arr;
    }else {
        let tableName = SF_AREA_1_TABLE_NAME;
        let area_2_level_data = [8, 9]
        if(area_2_level_data.indexOf(current_area_level) > -1){
            tableName = SF_AREA_2_TABLE_NAME;
        }
        // Get another level of area from DB
        let fieldsArray = ['*'];
        let WhereClouse = [];
        WhereClouse.push({ "fieldName": "parent_code__c", "fieldValue": area_arr, "type": 'IN'}); 
        
        let area_sql = await qry.SelectAllQry(fieldsArray, tableName, WhereClouse, '0', '10000', ' order by createddate desc' );
        let area = await client.query(area_sql);
        console.log('SQL :::: :::::', area_sql);
        // get array of area_sfid;
        if(area.rows.length > 0){
            let area_level = area_level_map[area.rows[0]['area_level__c']];
            let area_code = area.rows.map((area)=> {
                if(area['sfid']){
                    return area['sfid'];
                }else {
                    return null;
                }
            })
            // return new Promise((resolve, reject) => {
                let data = await getAnotherLevelArea(team_data, area_code, required_area_level, area_level);
                if(data){
                    return data;
                }
            // })
        }
    }
}

async function getLocationAddr(lat, long) {
    if (lat != null && lat != '' && long != null && long != '') {
        return rp(`https://maps.googleapis.com/maps/api/geocode/json?latlng=${lat},${long}&key=${process.env.GOOGLE_API_KEY2}`)
            .then(async function (data) {

                data = JSON.parse(data);

                var isResultFound = false, address = 'N/A';
                if (data != undefined && data.results.length > 0) {
                    for (i in data.results) {
                        if (isResultFound == false) {

                            for (j in data.results[i].address_components) {
                                if (data.results[i].geometry.location_type == 'GEOMETRIC_CENTER' && isResultFound == false) {
                                    isResultFound = true;
                                    address = data.results[i].formatted_address;
                                }
                            }
                        }
                    }
                }
                return address;
            })
            .catch(function (err) {
                console.log(err);
                // Crawling failed...
            });
    } else {
        return 'N/A';
    }
}

async function distanceP2p(lat1, lon1, lat2, lon2, unit) {
	if ((lat1 == lat2) && (lon1 == lon2)) {
		return 0;
	}
	else {
        console.log(`function values ----->lat1 ${lat1}  long1 ${lon1}  lat2 ${lat2}  long2 ${lon2}`);
        console.log('infunction');
		var radlat1 = Math.PI * lat1/180;
		var radlat2 = Math.PI * lat2/180;
		var theta = lon1-lon2;
		var radtheta = Math.PI * theta/180;
		var dist = Math.sin(radlat1) * Math.sin(radlat2) + Math.cos(radlat1) * Math.cos(radlat2) * Math.cos(radtheta);
		if (dist > 1) {
			dist = 1;
		}
		dist = Math.acos(dist);
		dist = dist * 180/Math.PI;
		dist = dist * 60 * 1.1515;
		if (unit=="K") { dist = dist * 1.609344 }
		if (unit=="N") { dist = dist * 0.8684 }
        if (unit=="M") { dist = dist * 1.609344 * 1000}
		return dist;
	}
}

async function getDistanceTravlled(lat1,lon1,lat2,lon2) {
    if (lat1 != null && lat1 != '' && lon1 != null && lon1 != '' && lat2 != null && lat2 != '' && lon2 != null && lon2 != '') {
        return rp(`https://maps.googleapis.com/maps/api/distancematrix/json?origins=${lat1},${lon1}&destinations=${lat2},${lon2}&key=${process.env.GOOGLE_API_KEY}`)
            .then(async function (data) {

                data = JSON.parse(data);
                // console.log('Data in Get distance API ::::', data.rows[0].elements[0])
                let value_in_km = data.rows[0].elements[0].distance.text;
                let value = data.rows[0].elements[0].distance.value;
                console.log('DATA ------------------>',value);
                console.log('length ------------------>',data.rows.length);
                let valueInKm = '';
                let valueInNum = '';
                if (data != undefined && data.rows.length > 0) {
                    valueInKm = value_in_km;
                    valueInNum = value;
                }
                console.log('KM ----->',valueInKm);
                console.log('NO ----->',valueInNum);
                return {value_in_km,value};
            })
            .catch(function (err) {
                console.log(err);
                // Crawling failed...
            });
    } else {
        return 'N/A';
    }
}

async function errorlog(data,file_name,query_params,header_params,body_params,url){
    //let unique_name = new Date().getTime();
    //console.log('Value ----->',unique_name);
    if(!data){
        //console.log('If Part');
        return 'No Data';
    }else{
        //console.log('Else Part',data,'QP--',query_params,'HP--',header_params , 'BP---',body_params);
        // let new_param = JSON.stringify(query_params)
        // console.log('value--',new_param);
        // To Write Error In Unique Files
        // fs.writeFile(`./console_errors/${unique_name}.txt`,data,()=>{
        //     return 'File Write Success';
        // });
        // To Write All Error In Same File In Appended Form 
        fs.writeFile(`./console_errors/${file_name}.txt`,"API Name --->" + url + '\n' + "Query Params --->" + JSON.stringify(query_params) +'\n' + "Header Token --->" + header_params +'\n' + "Body Params --->" + JSON.stringify(body_params) + '\n' + "Error --->" + data + '\n' + '\r\n\n',{ flag: "a" },()=>{
            return 'File Write Success';
        });
    }
}

function findByMatchingProperties(set, properties) {
    return set.filter(function (entry) {
        return Object.keys(properties).every(function (key) {
            return entry[key] === properties[key];
        });
    });
}

async function getMyTeamMember(manager_id){

    let user_list_sql = `select Id from [dbo].[zx_team] where zx_teammanager='${manager_id}'`;
    let user_list_result = await azure.runQuery(user_list_sql);


    let final_team = [];
        user_list_result.map((team) => {
        final_team.push(`'${team['Id']}'`);
    })
    let recent_data=[];
    for (let i = 0; i < 7; i++) {
        if (i == 0) {
            recent_data = [`'${manager_id}'`,...final_team];
        }
       // console.log(`final_team starting inside for loop  :::: ${i}>>>>`, recent_data);
        if (recent_data.length > 0) {
            let teamSql = `SELECT Id from [dbo].[zx_team] WHERE zx_teammanager IN (${recent_data})`;
            // let all_visit= `SELECT Id from [dbo].[zx_visits] WHERE zx_team IN (${recent_data})`;
           // console.log("teamSql+++++++", teamSql)
            let teams = await azure.runQuery(teamSql);
           // console.log("teamResult+++++++", teams)

            recent_data = []
            if (teams.length > 0) {
                teams.map(async(team) => {
                    recent_data.push(`'${team['Id']}'`);
                    final_team.push(`'${team['Id']}'`);
                });
            }
        }

    }

    final_team = await sort.removeDuplicates(final_team);
   // console.log("final", final_team)
    
    return final_team

}

async function getAllTeam(manager_id){

    let user_list_sql = `select Id from [dbo].[zx_team] where zx_teammanager='${manager_id}'`;
    let user_list_result = await azure.runQuery(user_list_sql);


    let final_team = [];
        user_list_result.map((team) => {
        final_team.push(`'${team['Id']}'`);
    })
    let recent_data=[];
    for (let i = 0; i < 7; i++) {
        if (i == 0) {
            recent_data = [`'${manager_id}'`,...final_team];
        }
        console.log(`final_team starting inside for loop  :::: ${i}>>>>`, recent_data);
        if (recent_data.length > 0) {
            let teamSql = `SELECT Id from [dbo].[zx_team] WHERE zx_teammanager IN (${recent_data})`;
            // let all_visit= `SELECT Id from [dbo].[zx_visits] WHERE zx_team IN (${recent_data})`;
           // console.log("teamSql+++++++", teamSql)
            let teams = await azure.runQuery(teamSql);
           // console.log("teamResult+++++++", teams)

            recent_data = []
            if (teams.length > 0) {
                teams.map(async(team) => {
                    recent_data.push(`'${team['Id']}'`);
                    final_team.push(`'${team['Id']}'`);
                });
            }
        }

    }

    final_team = await sort.removeDuplicates(final_team);
  //  console.log("final", final_team)
    final_team.unshift(`'${manager_id}'`)
    return final_team

}

async function teamMemberLogic(user_id){
    try{    
        let final_sfid_arr = [];
        let final_name_sfid_arr = [];
        let test_arr = [];
        for(let i = 0 ; i < 9 ; i++){
            let fields = ['*']
            let tableName = SF_TEAM_TABLE_NAME;
            var offset='0', limit='100';
            let WhereClouse = [];
            if(i == 0){
                WhereClouse.push({ "fieldName": "next_level_person_id__c", "fieldValue": user_id}); 
            }else{
                WhereClouse.push({ "fieldName": "next_level_person_id__c", "fieldValue": test_arr ,"type": 'IN' }); 
            }  
            let user_sql = qry.SelectAllQry(fields, tableName,WhereClouse, offset, limit,' order by createddate desc');
            console.log("user manager sql ------> ", user_sql);
            let user_res = await client.query(user_sql);

            if(test_arr.length > 0){
                test_arr.length = 0;
                console.log(`Test Array In 0 length in ith ${i} ${test_arr}`);
            }

            if(user_res.rows.length == 0){
                // final_sfid_arr.push(user_id)
                console.log('***********Break Loop************');
                break;
            }

            if(user_res.rows.length > 0){
                user_res.rows.map((sfids)=> {
                    final_sfid_arr.push(sfids['sfid'])  
                    let obj = {
                        id: sfids['sfid'],
                        name: sfids['team_member_name__c'],
                        designation : sfids['designation__c']
                      };
                      final_name_sfid_arr.push(obj);                
                    test_arr.push(sfids['sfid'])                  
                })
            //console.log(`Inside if condition team data ${i} ${sfid_arr}`);
            }
        }
        console.log('----------',final_sfid_arr.length);
        if(final_sfid_arr.length > 0){
            final_sfid_arr = sort.removeDuplicates(final_sfid_arr);
            console.log('final Sfid arr ---->',final_sfid_arr , 'Length -----',final_sfid_arr.length);
            return [final_sfid_arr , final_name_sfid_arr];
        }else{
            return [[],[]];
        }
        
    }catch(e){
        console.log('Error In team Manager logic ======>',e)
    }
}
//@DONE
async function teamWiseTerritoryData(team_ids,is_primary){
    try{
        let territory_arr = [];
        let territory_id_arr = [];
        let town_arr = [];
        let town_id_arr = [];
        let grid_arr = [];
        let beat_arr = [];
        let team_territory_sql;
        let town_id_temp_arr = [];
        //console.log(`Value ----> ${is_primary}`);
        // let territory_fields = ['*']
        // let territory_tableName = SF_TEAM_TERRITORY_MAPPING_TABLE_NAME;
        // let territory_offset='0', territory_limit='100';
        // let territory_WhereClouse = [];
        // territory_WhereClouse.push({ "fieldName": "team_member_id__c", "fieldValue": team_ids ,"type": 'IN' }); 
        // territory_WhereClouse.push({ "fieldName": `territory_type__c`, "fieldValue": 'Secondary' }); 
        if(is_primary){
            team_territory_sql = `Select * from salesforce.team_territory_mapping__c where team_member_id__c IN ('${team_ids.join("','")}')`
            // let team_territory_sql = qry.SelectAllQry(territory_fields, territory_tableName,territory_WhereClouse, territory_offset, territory_limit,' order by createddate desc');
            console.log("Team Territory sql if case------> ", team_territory_sql);
           
        }else{
            team_territory_sql = `Select * from salesforce.team_territory_mapping__c where team_member_id__c IN ('${team_ids.join("','")}') and (territory_type__c = 'Secondary' or territory_type__c is null)`
            // let team_territory_sql = qry.SelectAllQry(territory_fields, territory_tableName,territory_WhereClouse, territory_offset, territory_limit,' order by createddate desc');
            console.log("Team Territory sql else------> ", team_territory_sql);
        }
        let team_territory_sql_res = await client.query(team_territory_sql);

        if(team_territory_sql_res.rows.length > 0){
            team_territory_sql_res.rows.map((sfids)=> {
                territory_id_arr.push(sfids['territory_code__c'])  
                // let obj = {
                //     territory_id: sfids['territory_code__c'],
                //     name: sfids['territory_name__c'],
                //     territory_type: sfids['territory_type__c']
                // };
                // territory_arr.push(obj);                
            });
            console.log(`Territory id arr ----> ${territory_id_arr}`);
            let territory_id = [];
            for(let i = 0 ; i < territory_id_arr.length ; i++){
                let temp_data = await getTerritoryFromAnyArea(team_territory_sql_res.rows[i]['territory_code__c'])
                territory_id = [...territory_id,...temp_data]
            }
            let temp = [...territory_id]
            //let territory_id = temp[6]
            console.log(`Territory Id's ==========> ${territory_id}`);
            let territory_sql = `SELECT area1__c.*,picklist__c.name as territory_type_name 
                                FROM salesforce.area1__c
                                LEFT JOIN salesforce.picklist__c ON area1__c.territory_type__c = picklist__c.sfid   
                                where area1__c.sfid IN ('${territory_id.join("','")}')`
            console.log(`Sql -------- ${territory_sql}`);
            let territory_res = await client.query(territory_sql)
            if(territory_res.rows.length > 0){
                territory_res.rows.map((sfids) => {
                    let obj = {
                        territory_id: sfids['sfid'],
                        name: sfids['name__c'],
                        territory_type: sfids['territory_type_name']
                    };
                    territory_arr.push(obj); 
                })
            }

            for(let i =0 ; i< territory_id_arr.length ; i++){
                console.log(`Territory Id ---> ${territory_id_arr[i]} For ${i} Iteration`);
                let temp_data = await getTownFromUpperArea(territory_id_arr[i],is_primary);
                console.log(`Temp Data ----> ${temp_data}`);
                town_id_temp_arr = [...town_id_temp_arr,...temp_data]
                console.log(`Inside Loop Town Id Arr ----> ${town_id_temp_arr}`);
            }
            //let town_id = await getTownFromUpperArea(territory_id_arr[0]);
            //console.log("TOWN___Ids",town_id_temp_arr)

            let town_sql = `SELECT area1__c.*, geographical_town__c.town_name__c as geographical_town_name__c ,parent_area_table.territory_type__c as territory_type_id , town_type_name_table.name as town_type ,picklist__c.name as territory_type_name
            FROM salesforce.area1__c
            LEFT JOIN salesforce.geographical_town__c ON area1__c.geographical_town__c = geographical_town__c.sfid
            LEFT JOIN salesforce.picklist__c as town_type_name_table ON area1__c.town_type__c = town_type_name_table.sfid
            LEFT JOIN salesforce.area1__c as parent_area_table on area1__c.parent_code__c = parent_area_table.sfid
            LEFT JOIN salesforce.picklist__c ON parent_area_table.territory_type__c = picklist__c.sfid  
            where area1__c.sfid IN ('${town_id_temp_arr.join("','")}')`
            // let town_sql_res = await client.query(town_sql);


            // let town_fields = [`${SF_AREA_1_TABLE_NAME}.*,${SF_GEOGRAPHICAL_TOWN_TABLE_NAME}.town_name__c as geographical_town_name__c`]
            // let town_tableName = SF_AREA_1_TABLE_NAME;
            // let town_offset='0', town_limit='100';
            // let town_WhereClouse = [];
            // town_WhereClouse.push({ "fieldName": `${SF_AREA_1_TABLE_NAME}.parent_code__c`, "fieldValue": territory_id_arr ,"type": 'IN' }); 
            // town_WhereClouse.push({ "fieldName": `${SF_AREA_1_TABLE_NAME}.territory_type__c`, "fieldValue": 'a050w000002jNn6AAE' }); 

            // let town_joins = [
            //     {
            //         "type": "LEFT",
            //         "table_name": `${SF_GEOGRAPHICAL_TOWN_TABLE_NAME}`,
            //         "p_table_field": `${SF_AREA_1_TABLE_NAME}.geographical_town__c`,
            //         "s_table_field": `${SF_GEOGRAPHICAL_TOWN_TABLE_NAME}.sfid `
            //     },
            // ]
    
            // let town_sql = qry.fetchAllWithJoinQry(town_fields, town_tableName, town_joins, town_WhereClouse,town_offset,town_limit,`order by ${SF_AREA_1_TABLE_NAME}.createddate desc`);
            
            //let town_sql = qry.SelectAllQry(town_fields, town_tableName,town_WhereClouse, town_offset, town_limit,' order by createddate desc');
            console.log("Town sql ------> ", town_sql);
            let town_sql_res = await client.query(town_sql);

            if(town_sql_res.rows.length > 0){
                let area_type_sql = `SELECT name,sfid FROM salesforce.picklist__c where name IN ('Grid','Beat') and field_name__c = 'Area_Type__c' order by name desc`
                let area_type_res = await client.query(area_type_sql)
                let grid_id = area_type_res.rows[0]['sfid']
                let beat_id =  area_type_res.rows[1]['sfid']
                let visit_completed_status_id = await getPicklistSfid('Visit__c','Visit_Status__c','Completed')

                town_sql_res.rows.map((sfids)=> { 
                    town_id_arr.push(sfids['sfid']) 
                    let obj = {
                        town_id: sfids['sfid'],
                        name: sfids['geographical_town_name__c'],
                        town_type_name: sfids['town_type__c'],
                        town_name: sfids['name__c'],
                        territory_type_name: sfids['territory_type_name'],
                        town_type: sfids['town_type']
                    };
                    town_arr.push(obj);                
                });

                //To Find Grid Related Data From A Particular Town
                let grid_fields = [`${SF_AREA_1_TABLE_NAME}.*, pk2.name as grid_potential_name,
                pk2.picklist_detail2__c as grid_potential_color,
                pk2.picklistpicklist1__c as grid_potential_code,
                ${SF_GEOGRAPHICAL_TOWN_TABLE_NAME}.town_name__c as geographical_town_name,
                ${SF_GEOGRAPHICAL_TOWN_TABLE_NAME}.sfid as geographical_town_id`]
                let grid_tableName = SF_AREA_1_TABLE_NAME;
                let grid_offset='0', grid_limit='100';
                let grid_WhereClouse = [];
                grid_WhereClouse.push({ "fieldName": `${SF_AREA_1_TABLE_NAME}.parent_code__c`, "fieldValue": town_id_arr ,"type": 'IN' }); 
                grid_WhereClouse.push({ "fieldName": `${SF_AREA_1_TABLE_NAME}.area_type__c`, "fieldValue": grid_id }); 
                let joins = [
                    {
                        "type": "LEFT",
                        "table_name": `${SF_PICKLIST_TABLE_NAME} as pk2`,
                        "p_table_field": `${SF_AREA_1_TABLE_NAME}.grid_potential__c`,
                        "s_table_field": `pk2.sfid`
                    },
                    {
                        "type": "LEFT",
                        "table_name": `${SF_GEOGRAPHICAL_TOWN_TABLE_NAME}`,
                        "p_table_field": `${SF_AREA_1_TABLE_NAME}.geographical_town__c`,
                        "s_table_field": `${SF_GEOGRAPHICAL_TOWN_TABLE_NAME}.sfid `
                    }
                ]
                let grid_sql = qry.fetchAllWithJoinQry(grid_fields, grid_tableName,joins,grid_WhereClouse, grid_offset, grid_limit,`order by ${SF_AREA_1_TABLE_NAME}.createddate desc`);
                console.log("Grid sql ------> ", grid_sql);
                let grid_sql_res = await client.query(grid_sql);


                //To Find Beat Related Data From A Particular Town

                let beat_fields = [`${SF_AREA_1_TABLE_NAME}.*, pk2.name as grid_potential_name,
                pk2.picklist_detail2__c as grid_potential_color,
                pk2.picklistpicklist1__c as grid_potential_code,
                ${SF_GEOGRAPHICAL_TOWN_TABLE_NAME}.town_name__c as geographical_town_name,
                ${SF_GEOGRAPHICAL_TOWN_TABLE_NAME}.sfid as geographical_town_id`]
                let beat_tableName = SF_AREA_1_TABLE_NAME;
                let beat_offset='0', beat_limit='100';
                let beat_WhereClouse = [];
                beat_WhereClouse.push({ "fieldName": `${SF_AREA_1_TABLE_NAME}.parent_code__c`, "fieldValue": town_id_arr ,"type": 'IN' }); 
                beat_WhereClouse.push({ "fieldName": `${SF_AREA_1_TABLE_NAME}.area_type__c`, "fieldValue": beat_id }); 
                let beat_joins = [
                    {
                        "type": "LEFT",
                        "table_name": `${SF_PICKLIST_TABLE_NAME} as pk2`,
                        "p_table_field": `${SF_AREA_1_TABLE_NAME}.grid_potential__c`,
                        "s_table_field": `pk2.sfid`
                    },
                    {
                        "type": "LEFT",
                        "table_name": `${SF_GEOGRAPHICAL_TOWN_TABLE_NAME}`,
                        "p_table_field": `${SF_AREA_1_TABLE_NAME}.geographical_town__c`,
                        "s_table_field": `${SF_GEOGRAPHICAL_TOWN_TABLE_NAME}.sfid `
                    }
                ]
                let beat_sql = qry.fetchAllWithJoinQry(beat_fields, beat_tableName,beat_joins,beat_WhereClouse, beat_offset, beat_limit,`order by ${SF_AREA_1_TABLE_NAME}.createddate desc`);
                console.log("Beat sql ------> ", beat_sql);
                let beat_sql_res = await client.query(beat_sql);


                for (let j = 0; j < grid_sql_res.rows.length; j++) {
                    const fields_visit = ['visit_date__c'];
                    const tableName_visit = SF_VISIT_TABLE_NAME;
                    let offset_visit = '0', limit_visit = '1';
                    let orderBy_visit = 'ORDER BY visit_date__c desc'

                    const WhereClouse_visit = [];

                    WhereClouse_visit.push({ fieldValue: visit_completed_status_id, fieldName: 'visit_status__c' });  //completed
                    WhereClouse_visit.push({ fieldValue: team_ids, fieldName: 'emp_id__c' });

                    //@TODO -- have to confirm this grid
                    // WhereClouse_visit.push({ fieldValue: grid_sql_res.rows[i]['sfid'], fieldName: 'grid__c' });


                    let visit_sql1 = qry.SelectAllQry(fields_visit, tableName_visit, WhereClouse_visit, offset_visit, limit_visit, orderBy_visit);
                    // console.log('visit sql for date ------->', visit_sql1);

                    let visit_result = await client.query(visit_sql1);
                    // console.log('test for visit date --->',visit_result.rows);
                    // console.log('last visit date ---> ', dtUtil.ISOtoLocal(visit_result.rows[0].visit_date__c));
                    if (visit_result.rows.length > 0) {
                        grid_sql_res.rows[j]['last_visit_date'] = dtUtil.ISOtoLocal(visit_result.rows[0]['visit_date__c'])
                    }
                    else {
                        grid_sql_res.rows[j]['last_visit_date'] = null
                    }

                    console.log(`--------------------- ${grid_sql_res.rows[j]["sfid"]} ---------------`);
                    let grid_sql_1 = `Select * from salesforce.area1__c where sfid = '${grid_sql_res.rows[j]["sfid"]}'`;
                    console.log("town_sql++++++", grid_sql_1)
                    let grid_sql_res_1 = await client.query(grid_sql_1);

                    let town_sql = `Select * from salesforce.area1__c where sfid = '${grid_sql_res_1.rows[0]["parent_code__c"]}'`;
                    console.log("ter_sql++++++",town_sql )
                    let town_sql_res = await client.query(town_sql);

                    let terr_sql = `Select * from salesforce.area1__c where sfid = '${town_sql_res.rows[0]["parent_code__c"]}'`;
                    console.log("se_sql++++++", terr_sql)
                    let terr_sql_res = await client.query(terr_sql);

                    if (terr_sql_res.rows.length > 0) {
                        grid_sql_res.rows[j]['territory_id'] = terr_sql_res.rows[0]['sfid']
                        grid_sql_res.rows[j]['territory_name'] = terr_sql_res.rows[0]['name__c']
                        grid_sql_res.rows[j]['town_id'] = town_sql_res.rows[0]['sfid']
                        grid_sql_res.rows[j]['town_name'] = town_sql_res.rows[0]['name__c']
                    }
                    else {
                        grid_sql_res.rows[j]['territory_id']  = null
                        grid_sql_res.rows[j]['territory_name'] = null
                        grid_sql_res.rows[j]['town_id'] = null
                        grid_sql_res.rows[j]['town_name'] = null


                    }


                    const fields_grid_work = [`${SF_GRID_WORKING_TABLE_NAME}.*,ar_town.name__c as town_name, ar_grid.name__c as grid_name, ${SF_PICKLIST_TABLE_NAME}.name as grid_working_status_name`];
                    const tableName_grid_work = SF_GRID_WORKING_TABLE_NAME;
                    const WhereClouse_grid_work = [];
                    let d = new Date();
                    let current_month = d.getMonth() + 1;
                    let prev_month = d.getMonth();
                    // console.log("current month an prev month",current_month,prev_month,d)
                    //console.log('PGID -->', i, '---VALUE--', expected_order_sql_res.rows[i]['sfid']);
                    if (grid_sql_res.rows[j]['sfid']) {
                        WhereClouse_grid_work.push({ 'fieldName': `${SF_GRID_WORKING_TABLE_NAME}.grid__c`, 'fieldValue': grid_sql_res.rows[j]['sfid'] });
                    }
                    // if (from_date && to_date) {
                    //     WhereClouse_grid_work.push({ "fieldName": `${SF_GRID_WORKING_TABLE_NAME}.planned_date__c`, "fieldValue": from_date, type: 'GTE' });
                    //     WhereClouse_grid_work.push({ "fieldName": `${SF_GRID_WORKING_TABLE_NAME}.planned_date__c`, "fieldValue": to_date, type: 'LTE' });
                    // }
                    // else {
                    //     WhereClouse_grid_work.push({ 'fieldName': `EXTRACT(MONTH FROM planned_date__c)`, 'fieldValue': current_month, type: 'LTE' });
                    //     WhereClouse_grid_work.push({ 'fieldName': `EXTRACT(MONTH FROM planned_date__c)`, 'fieldValue': prev_month, type: 'GTE' });
                    // }
                    let offset_grid = '0', limit_grid = '1000';

                    let grid_joins = [
                        {
                            "type": "LEFT",
                            "table_name": `${SF_AREA_1_TABLE_NAME} as ar_town`,
                            "p_table_field": `${SF_GRID_WORKING_TABLE_NAME}.town__c`,
                            "s_table_field": `ar_town.sfid `
                        },
                        {
                            "type": "LEFT",
                            "table_name": `${SF_AREA_1_TABLE_NAME} as ar_grid`,
                            "p_table_field": `${SF_GRID_WORKING_TABLE_NAME}.grid__c`,
                            "s_table_field": `ar_grid.sfid `
                        },
                        {
                            "type": "LEFT",
                            "table_name": `${SF_PICKLIST_TABLE_NAME}`,
                            "p_table_field": `${SF_GRID_WORKING_TABLE_NAME}.grid_working_status__c`,
                            "s_table_field": `${SF_PICKLIST_TABLE_NAME}.sfid `
                        }
                    ]


                    let working_hour_sql = qry.fetchAllWithJoinQry(fields_grid_work, tableName_grid_work, grid_joins, WhereClouse_grid_work, offset_grid, limit_grid, ` order by ${SF_GRID_WORKING_TABLE_NAME}.createddate desc`);
                    console.log(`working grid SQL ----> ${working_hour_sql}`)
                    // let orderLine_sql = db.SelectAllQry(fields, tableName,orderLine_WhereClouse, offset, limit,' order by createddate desc');
                    let working_hour_sql_res = await client.query(working_hour_sql);

                    //@Change requested by bittu sir, as there was undefined coming in a field no_of_days_to_complete_grid__c
                    if (grid_sql_res.rows[j]['no_of_days_to_complete_grid__c'] != null && grid_sql_res.rows[j]['no_of_days_to_complete_grid__c'] != undefined && grid_sql_res.rows[j]['no_of_days_to_complete_grid__c'] != 'undefined') {
                        let update_no_of_days = `update salesforce.${SF_GRID_WORKING_TABLE_NAME} set no_of_days_for_the_completion__c = ${grid_sql_res.rows[j]['no_of_days_to_complete_grid__c']} where grid__c = '${grid_sql_res.rows[j]['sfid']}'`
                        let update_no_of_days_res = await client.query(update_no_of_days);
                        // console.log("update_no_of_days_res-------------", update_no_of_days_res)
                    }

                    if (working_hour_sql_res.rows.length > 0) {
                        grid_sql_res.rows[j]['proposed_visit_date'] = dtUtil.addDays(dtUtil.ISOtoLocal(working_hour_sql_res.rows[0]['planned_date__c']), working_hour_sql_res.rows[0]['no_of_days_for_the_completion__c'])
                        // console.log("=====================",dtUtil.addDays(dtUtil.ISOtoLocal(working_hour_sql_res.rows[0]['visit_completion_date__c']), working_hour_sql_res.rows[0]['no_of_days_for_the_completion__c']))
                    } else {
                        grid_sql_res.rows[j]['proposed_visit_date'] = null;
                    }


                    let temp_arr = []
                    let final_index = 0;
                    let no_value_pushed_decider = 0;
                    if (working_hour_sql_res.rows.length > 0) {

                        const ongoing_index = working_hour_sql_res.rows.findIndex(data => data.grid_working_status_name === "Ongoing");
                        temp_arr.push(ongoing_index)
                        const planned_index = working_hour_sql_res.rows.findIndex(data => data.grid_working_status_name === "Planned");
                        temp_arr.push(planned_index)
                        const completed_index = working_hour_sql_res.rows.findIndex(data => data.grid_working_status_name === "Completed");
                        temp_arr.push(completed_index)


                        for (let z = 0; z < temp_arr.length; z++) {
                            if (temp_arr[z] > -1) {
                                no_value_pushed_decider = 1
                                final_index = temp_arr[z]
                                break;
                            }
                        }
                        console.log(`Temp Array ====> ${temp_arr}`);

                        if (final_index > -1 && no_value_pushed_decider == 1) {
                            grid_sql_res.rows[j]['grid_working_data'] = [working_hour_sql_res.rows[final_index]]
                        } else {
                            grid_sql_res.rows[j]['grid_working_data'] = []
                        }
                    } else {
                        grid_sql_res.rows[j]['grid_working_data'] = []
                    }

                }
                console.log("$$$$$$$$$$$$$$$$$$$",grid_sql_res.rows)

                if(grid_sql_res.rows.length > 0){
                    grid_sql_res.rows.map((sfids)=> { 
                        let obj = {
                            name__c: sfids['name__c'],
                            name: sfids['name'],
                            sfid: sfids['sfid'],
                            grid_frequency__c: sfids['grid_frequency__c'],
                            parent_code__c: sfids['parent_code__c'],
                            grid_cordinates__c: sfids['grid_cordinates__c'],
                            perimeter__c: sfids['perimeter__c'],
                            border_width__c: sfids['border_width__c'],
                            perimeter__c: sfids['perimeter__c'],
                            area_level__c: sfids['area_level__c'],
                            grid_last_visit_date__c: sfids['grid_last_visit_date__c'],
                            grid_center_longitude__s: sfids['grid_center_longitude__s'],
                            opacity_level__c: sfids['opacity_level__c'],
                            geographical_town__c: sfids['geographical_town__c'],
                            approval_status__c: sfids['approval_status__c'],
                            area_type__c: sfids['area_type__c'],
                            grid_potential1__c: sfids['grid_potential1__c'],
                            grid_center_latitude__s: sfids['grid_center_latitude__s'],
                            town_type__c: sfids['town_type__c'],
                            no_of_days_to_complete_grid__c: sfids['no_of_days_to_complete_grid__c'],
                            grid_area__c: sfids['grid_area__c'],
                            grid_potential__c: sfids['grid_potential__c'],
                            grid_potential_name:sfids['grid_potential_name'],
                            grid_potential_color:sfids['grid_potential_color'],
                            grid_potential_code:sfids['grid_potential_code'],
                            last_visit_date:sfids['last_visit_date'],
                            grid_working_data:sfids['grid_working_data'],
                            geographical_town_name:sfids['geographical_town_name'],
                            geographical_town_id:sfids['geographical_town_id'],
                            territory_id:sfids['territory_id'],
                            territory_name:sfids['territory_name'],
                            town_id:sfids['town_id'],
                            town_name:sfids['town_name']
                        };
                        grid_arr.push(obj);                
                    });
                }

                if(beat_sql_res.rows.length > 0){
                    beat_arr = [...beat_sql_res.rows]
                }
                return [territory_arr,town_arr,grid_arr,territory_id_arr,town_id_arr,beat_arr];


            }else{
                return [territory_arr,town_arr,grid_arr,territory_id_arr,town_id_arr,beat_arr];
            }

        }else{
            return [territory_arr,town_arr,grid_arr,territory_id_arr,town_id_arr,beat_arr];
        }

    }catch(e){
        console.log('Error In team Wise Territory logic ======>',e) 
    }
}

async function getUniqueValuesFromArrayOfObject(array_of_objects){
    try{
        const uniqueData = [...array_of_objects.reduce((map, obj) => map.set(obj.sfid, obj), new Map()).values()];
        //console.log('GG------',uniqueData.length);
        return uniqueData;
    }catch(e){
        console.log('Error In Unique Values In Functional Util ---->',e);
    }
}
async function getUniqueValuesFromArrayOfObjectname(array_of_objects){
    try{
        const uniqueData = [...array_of_objects.reduce((map, obj) => map.set(obj.name, obj), new Map()).values()];
        //console.log('GG------',uniqueData.length);
        return uniqueData;
    }catch(e){
        console.log('Error In Unique Values In Functional Util ---->',e);
    }
}

async function getUniqueValuesForDayType(array_of_objects){
    try{
        const uniqueData = [...array_of_objects.reduce((map, obj) => map.set(obj.day_type__id, obj), new Map()).values()];
        //console.log('GG------',uniqueData.length);
        return uniqueData;
    }catch(e){
        console.log('Error In Unique Values In Functional Util ---->',e);
    }
}

async function territoryWiseAccount(territory_ids,lob_id,division_id, limit, offset){
    try{
        // console.log("++++++++++++++++++++++++++++++++++++++++++",limit)
        // console.log("++++++++++++++++++++++++++++++++++++++++++",offset)

        const fields = [`${SF_ACCOUNT_TABLE_NAME}.*,
        ${SF_ACCOUNT_TYPE_ACTIVATION_TABLE_NAME}.lob__c as lob__c,
        ${SF_ACCOUNT_TYPE_ACTIVATION_TABLE_NAME}.division__c as division__c,
        ${SF_AREA_2_TABLE_NAME}.name__c as town_name,
        ${SF_ACCOUNT_TYPE_ACTIVATION_TABLE_NAME}.account_type__c as account_type__c,
        ${SF_PICKLIST_TABLE_NAME}.name as account_type_name`];
        const tableName = SF_ACCOUNT_TABLE_NAME ; 
        const WhereClouse = [];
        // let offset = '0', limit = '100';
        WhereClouse.push({ "fieldName": `${SF_ACCOUNT_TABLE_NAME}.territory__c`, "fieldValue": territory_ids , "type": 'IN' });
        let joins = [
            {
                "type": "LEFT",
                "table_name": `${SF_ACCOUNT_TYPE_ACTIVATION_TABLE_NAME}`,
                "p_table_field": `${SF_ACCOUNT_TABLE_NAME}.sfid`,
                "s_table_field": `${SF_ACCOUNT_TYPE_ACTIVATION_TABLE_NAME}.account_name__c `
            },
            {
                "type": "LEFT",
                "table_name": `${SF_PICKLIST_TABLE_NAME}`,
                "p_table_field": `${SF_ACCOUNT_TYPE_ACTIVATION_TABLE_NAME}.account_type__c`,
                "s_table_field": `${SF_PICKLIST_TABLE_NAME}.sfid `
            },
            {
                "type": "LEFT",
                "table_name": `${SF_AREA_2_TABLE_NAME}`,
                "p_table_field": `${SF_ACCOUNT_TABLE_NAME}.town__c`,
                "s_table_field": `${SF_AREA_2_TABLE_NAME}.sfid `
            }
            // {
            //     "type": "LEFT",
            //     "table_name": `${SF_PICKLIST_TABLE_NAME}`,
            //     "p_table_field": `${SF_ACCOUNT_TYPE_ACTIVATION_TABLE_NAME}.division__c`,
            //     "s_table_field": `${SF_PICKLIST_TABLE_NAME}.account_name__c `
            // },
            // {
            //     "type": "LEFT",
            //     "table_name": `${SF_PICKLIST_TABLE_NAME}`,
            //     "p_table_field": `${SF_ACCOUNT_TYPE_ACTIVATION_TABLE_NAME}.account_name__c`,
            //     "s_table_field": `${SF_PICKLIST_TABLE_NAME}.account_name__c `
            // }
        ]   
        let joinsand = [];
        joinsand.push({ "fieldName": `${SF_ACCOUNT_TYPE_ACTIVATION_TABLE_NAME}.active__c`, "fieldValue": 'true' });
        joinsand.push({ "fieldName": `${SF_ACCOUNT_TYPE_ACTIVATION_TABLE_NAME}.lob__c`, "fieldValue": `'${lob_id}'` });
        joinsand.push({ "fieldName": `${SF_ACCOUNT_TYPE_ACTIVATION_TABLE_NAME}.division__c`, "fieldValue": `'${division_id}'` });
        let sql = qry.fetchAllWithJoinQryNew(fields, tableName, joins,joinsand, WhereClouse,offset,limit,`order by ${SF_ACCOUNT_TABLE_NAME}.createddate desc`);
        console.log('Party SQL ------>',sql);
        let records = await client.query(sql);
        let final_data = await getUniqueValuesFromArrayOfObject(records.rows);

        if(final_data.length > 0){
            return final_data;
        }else{
            return [];
        }
    }catch(e){
        console.log('Error in Territory Wise Account Function ====>',e);
    }
}

async function territoryWiseTown(territory_id_arr){
    try{
        let fields = [`${SF_AREA_1_TABLE_NAME}.*,
                    ${SF_AREA_1_TABLE_NAME}.name as town_code ,
                    ${SF_PICKLIST_TABLE_NAME}.name as town_type__c ,
                    territory_table.name__c as territory_name__c ,
                    ${SF_GEOGRAPHICAL_TOWN_TABLE_NAME}.town_name__c as geographical_town_name__c`]
        let tableName = SF_AREA_1_TABLE_NAME;
        let offset='0', limit='100';
        let WhereClouse = [];
        WhereClouse.push({ "fieldName": `${SF_AREA_1_TABLE_NAME}.parent_code__c`, "fieldValue": territory_id_arr ,"type": 'IN'}); 
        let joins = [
            {
                "type": "LEFT",
                "table_name": `${SF_PICKLIST_TABLE_NAME}`,
                "p_table_field": `${SF_AREA_1_TABLE_NAME}.town_type__c`,
                "s_table_field": `${SF_PICKLIST_TABLE_NAME}.sfid `
            },
            {
                "type": "LEFT",
                "table_name": `${SF_AREA_1_TABLE_NAME} as territory_table`,
                "p_table_field": `${SF_AREA_1_TABLE_NAME}.parent_code__c`,
                "s_table_field": `territory_table.sfid `
            },
            {
                "type": "LEFT",
                "table_name": `${SF_GEOGRAPHICAL_TOWN_TABLE_NAME}`,
                "p_table_field": `${SF_AREA_1_TABLE_NAME}.geographical_town__c`,
                "s_table_field": `${SF_GEOGRAPHICAL_TOWN_TABLE_NAME}.sfid `
            }
        ]
        let town_from_territory_sql = qry.fetchAllWithJoinQry(fields, tableName,joins,WhereClouse, offset, limit,`order by ${SF_AREA_1_TABLE_NAME}.createddate desc`);
        console.log("town_from_territory_sql ----> ", town_from_territory_sql);
        let town_from_territory_sql_res = await client.query(town_from_territory_sql);

        if(town_from_territory_sql_res.rows.length > 0){
            return town_from_territory_sql_res.rows
        }else{
            return [];
        }
    }catch(e){
        console.log('Error in Territory Wise Town Function --->',e);
    }
}

// async function townWiseBeatGrid(town_id_arr,area_type){
//     try{
//         let fields = [`${SF_AREA_2_TABLE_NAME}.*,
//         ${SF_GEOGRAPHICAL_TOWN_TABLE_NAME}.town_name__c as geographical_name,
//         a1.name__c as parent_name, pl.name as area_type_name`
//     ]
//         let tableName = SF_AREA_2_TABLE_NAME;
//         var offset='0', limit='1000';
//         let WhereClouse = [];
//         if(area_type != undefined && area_type.length > 0){
//             WhereClouse.push({ "fieldName":`pl.name`, "fieldValue":area_type}); 
//         }
//         WhereClouse.push({ "fieldName": `${SF_AREA_2_TABLE_NAME}.parent_code__c`, "fieldValue": town_id_arr ,"type": 'IN'}); 
//         // WhereClouse.push({ "fieldName": `${SF_AREA_2_TABLE_NAME}.area_type__c`, "fieldValue": 'a050w000003QLJHAA4' });
//         WhereClouse.push({ "fieldName": `${SF_AREA_2_TABLE_NAME}.sfid`, "type": 'NOTNULL' });
//         // WhereClouse.push({ "fieldName": `${SF_GEOGRAPHICAL_TOWN_TABLE_NAME}.sfid`, "type": 'NOTNULL' });



//         let joins = [
//             {
//                 "type": "LEFT",
//                 "table_name": `${SF_GEOGRAPHICAL_TOWN_TABLE_NAME}`,
//                 "p_table_field": `${SF_AREA_2_TABLE_NAME}.geographical_town__c`,
//                 "s_table_field": `${SF_GEOGRAPHICAL_TOWN_TABLE_NAME}.sfid `
//             },
//             {
//                 "type": "LEFT",
//                 "table_name": `${SF_AREA_2_TABLE_NAME} as a1`,
//                 "p_table_field": `${SF_AREA_2_TABLE_NAME}.parent_code__c`,
//                 "s_table_field": `a1.sfid `
//             },
//             {
//                 "type": "LEFT",
//                 "table_name": `${SF_PICKLIST_TABLE_NAME} as pl`,
//                 "p_table_field": `${SF_AREA_2_TABLE_NAME}.area_type__c`,
//                 "s_table_field": `pl.sfid `
//             }
//         ]

//         let beat_grid_from_town_sql = qry.fetchAllWithJoinQry(fields, tableName, joins, WhereClouse,offset,limit,`order by ${SF_AREA_2_TABLE_NAME}.createddate desc`);
//         //let beat_grid_from_town_sql = qry.SelectAllQry(fields, tableName,WhereClouse, offset, limit,' order by createddate desc');
//         console.log("beats and grids from town sql ----> ", beat_grid_from_town_sql);
//         let beat_grid_from_town_sql_res = await client.query(beat_grid_from_town_sql);

//         if(beat_grid_from_town_sql_res.rows.length > 0){
//             return beat_grid_from_town_sql_res.rows
//         }else{
//             return [];
//         }
//     }catch(e){
//         console.log('Error in Town Wise Beat/Grid Function --->',e);
//     }
// }

// async function townWiseGrid(town_id_arr,area_type){
//     try{
//         let fields = [`${SF_AREA_2_TABLE_NAME}.*,
//         ${SF_GEOGRAPHICAL_TOWN_TABLE_NAME}.town_name__c as geographical_name,
//         a1.name__c as parent_name`
//     ]
//         let tableName = SF_AREA_2_TABLE_NAME;
//         var offset='0', limit='1000';
//         let WhereClouse = [];
//         if(area_type != undefined && area_type.length > 0){
//             WhereClouse.push({ "fieldName":`${SF_AREA_2_TABLE_NAME}.area_type__c`, "fieldValue":area_type}); 
//         }
//         WhereClouse.push({ "fieldName": `${SF_AREA_2_TABLE_NAME}.parent_code__c`, "fieldValue": town_id_arr ,"type": 'IN'}); 
//         WhereClouse.push({ "fieldName": `${SF_AREA_2_TABLE_NAME}.area_type__c`, "fieldValue": 'a050w000002jb1DAAQ' }); 
//         WhereClouse.push({ "fieldName": `${SF_AREA_2_TABLE_NAME}.sfid`, "type": 'NOTNULL' }); 
//         // WhereClouse.push({ "fieldName": `${SF_GEOGRAPHICAL_TOWN_TABLE_NAME}.sfid`, "type": 'NOTNULL' }); 



//         let joins = [
//             {
//                 "type": "LEFT",
//                 "table_name": `${SF_GEOGRAPHICAL_TOWN_TABLE_NAME}`,
//                 "p_table_field": `${SF_AREA_2_TABLE_NAME}.geographical_town__c`,
//                 "s_table_field": `${SF_GEOGRAPHICAL_TOWN_TABLE_NAME}.sfid `
//             },
//             {
//                 "type": "LEFT",
//                 "table_name": `${SF_AREA_2_TABLE_NAME} as a1`,
//                 "p_table_field": `${SF_AREA_2_TABLE_NAME}.parent_code__c`,
//                 "s_table_field": `a1.sfid `
//             },
//         ]

//         let grid_from_town_sql = qry.fetchAllWithJoinQry(fields, tableName, joins, WhereClouse,offset,limit,`order by ${SF_AREA_2_TABLE_NAME}.createddate desc`);
//         //let beat_grid_from_town_sql = qry.SelectAllQry(fields, tableName,WhereClouse, offset, limit,' order by createddate desc');
//         console.log("grids from town sql ----> ", grid_from_town_sql);
//         let grid_from_town_sql_res = await client.query(grid_from_town_sql);

//         if(grid_from_town_sql_res.rows.length > 0){
//             return grid_from_town_sql_res.rows
//         }else{
//             return [];
//         }
//     }catch(e){
//         console.log('Error in Town Wise Grid Function --->',e);
//     }
// }
//@TODO
async function beatGridWiseParty(id, emp_id, pjp_date) {
    try {

        let parent_id_arr = [];
        let territory_id_arr = [];
        let town_id_arr = [];
        let party_data_arr = [];
        let retailer_data_arr = [];
        let fields = ['parent_code__c']
        let tableName = SF_AREA_1_TABLE_NAME;
        var offset = '0', limit = '100';
        let WhereClouse = [];
        let dealer_arr = [];
        let retailer_arr = [];
        let count_dealer_related_visit = 0;
        let count_retailer_related_visit = 0;
        WhereClouse.push({ "fieldName": "sfid", "fieldValue": id });
        let grid_sql = qry.SelectAllQry(fields, tableName, WhereClouse, offset, limit, ' order by createddate desc');
        console.log("grid sql ------> ", grid_sql);
        let grid_sql_res = await client.query(grid_sql);

        if (grid_sql_res.rows.length > 0) {

            grid_sql_res.rows.map((sfids) => {
                parent_id_arr.push(sfids['parent_code__c'])
            });

            let fields = ['*']
            let tableName = SF_AREA_1_TABLE_NAME;
            var offset = '0', limit = '100';
            let WhereClouse = [];
            //WhereClouse.push({ "fieldName": "town_code__c", "fieldValue": grid_sql_res.rows});  
            WhereClouse.push({ "fieldName": "sfid", "fieldValue": parent_id_arr, "type": 'IN' });
            let town_sql = qry.SelectAllQry(fields, tableName, WhereClouse, offset, limit, ' order by createddate desc');
            console.log("town sql ------> ", town_sql);
            let town_sql_res = await client.query(town_sql);

            if (town_sql_res.rows.length > 0) {

                town_sql_res.rows.map((sfids) => {
                    territory_id_arr.push(sfids['parent_code__c'])
                    town_id_arr.push(sfids['sfid'])
                });

                //----------------------------------------For Dealer ----------------------------------------
                let fields = [`${SF_ACCOUNT_TABLE_NAME}.*, arr1.name__c as town_name`]
                let tableName = SF_ACCOUNT_TABLE_NAME;
                var offset = '0', limit = '1000';
                let WhereClouse = [];
                //WhereClouse.push({ "fieldName": "town_code__c", "fieldValue": grid_sql_res.rows});  
                //WhereClouse.push({ "fieldName": `${SF_ACCOUNT_TABLE_NAME}.territory__c`, "fieldValue": territory_id_arr, "type": 'IN' });
                WhereClouse.push({ "fieldName": `${SF_ACCOUNT_TABLE_NAME}.town__c`, "fieldValue": town_id_arr, "type": 'IN' });
                let joins = [
                    {
                        "type": "LEFT",
                        "table_name": `${SF_AREA_1_TABLE_NAME} as arr1`,
                        "p_table_field": `${SF_ACCOUNT_TABLE_NAME}.town__c`,
                        "s_table_field": `arr1.sfid `
                    }
                ]
                let party_sql = qry.fetchAllWithJoinQry(fields, tableName, joins, WhereClouse, offset, limit, ` order by ${SF_ACCOUNT_TABLE_NAME}.createddate desc`);
                console.log("party sql ------> ", party_sql);
                let party_sql_res = await client.query(party_sql);

                if (party_sql_res.rows.length > 0) {
                    let visit_completed_status_id = await getPicklistSfid('Visit__c','Visit_Status__c','Completed')
                    party_sql_res.rows.map((sfids) => {
                        dealer_arr.push(sfids['sfid'])
                    });
                    console.log('dealer_arr:::::::::::', dealer_arr)
                    for (let i = 0; i < party_sql_res.rows.length; i++) {

                        ////// order by last visited date which is in descending order /////
                        const fields = ['visit_date__c'];
                        const tableName = SF_VISIT_TABLE_NAME;
                        let offset = '0', limit = '1';
                        let orderBy = 'ORDER BY visit_date__c desc'

                        const WhereClouse = [];

                        // WhereClouse.push({fieldValue: req.query.team__c, fieldName: 'team_id__c'});
                        WhereClouse.push({ fieldValue: visit_completed_status_id, fieldName: 'visit_status__c' });
                        WhereClouse.push({ fieldValue: party_sql_res.rows[i]['sfid'], fieldName: 'dealer_retailer__c' });

                        let visit_sql1 = qry.SelectAllQry(fields, tableName, WhereClouse, offset, limit, orderBy);
                        console.log('visit sql for date ------->', visit_sql1);

                        let visit_result = await client.query(visit_sql1);
                        // console.log('test for visit date --->',visit_result.rows);
                        // console.log('last visit date ---> ', dtUtil.ISOtoLocal(visit_result.rows[0].visit_date__c));
                        if (visit_result.rows.length > 0) {
                            party_sql_res.rows[i]['last_visit_date'] = dtUtil.ISOtoLocal(visit_result.rows[0]['visit_date__c'])
                        }
                        else {
                            party_sql_res.rows[i]['last_visit_date'] = null
                        }
                    }
                    let check_visit_dealer_retailer_sql = `SELECT * FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_VISIT_TABLE_NAME} 
                         WHERE dealer_retailer__c IN ('${dealer_arr.join("','")}') and emp_id__c='${emp_id}' and visit_date__c='${pjp_date}';`
                    let check_visit_dealer_retailer_sql_res = await client.query(check_visit_dealer_retailer_sql);
                    console.log('check_visit_dealer_retailer_sql:::::::', check_visit_dealer_retailer_sql)
                    count_dealer_related_visit = check_visit_dealer_retailer_sql_res.rows.length
                    console.log('length ---->', check_visit_dealer_retailer_sql_res.rows.length)

                    //party_data_arr.push(party_sql_res.rows)
                    party_data_arr = [...party_sql_res.rows]
                } 
                // -----------------------------For Retailer Logic ------------------------------------

                let retailer_fields = [`${SF_RETAILER_TABLE_NAME}.*, arr1.name__c as town_name , geo_town_table.name as geographical_town_name`]
                let reatiler_tableName = SF_RETAILER_TABLE_NAME;
                var reatiler_offset = '0', retailer_limit = '1000';
                let retailer_WhereClouse = [];
                //WhereClouse.push({ "fieldName": "town_code__c", "fieldValue": grid_sql_res.rows}); 
                //Territory Wise Logic Removed As Per Customer Demand 
                //retailer_WhereClouse.push({ "fieldName": `${SF_RETAILER_TABLE_NAME}.territory__c`, "fieldValue": territory_id_arr, "type": 'IN' });
                retailer_WhereClouse.push({ "fieldName": `${SF_RETAILER_TABLE_NAME}.sub_town__c`, "fieldValue": town_id_arr, "type": 'IN' });
                let retailer_joins = [
                    {
                        "type": "LEFT",
                        "table_name": `${SF_AREA_1_TABLE_NAME} as arr1`,
                        "p_table_field": `${SF_RETAILER_TABLE_NAME}.sub_town__c`,
                        "s_table_field": `arr1.sfid `
                    },
                    {
                        "type": "LEFT",
                        "table_name": `${SF_GEOGRAPHICAL_TOWN_TABLE_NAME} as geo_town_table`,
                        "p_table_field": `${SF_RETAILER_TABLE_NAME}.geographical_town__c`,
                        "s_table_field": `geo_town_table.sfid `
                    }
                ]
                let retailer_sql = qry.fetchAllWithJoinQry(retailer_fields, reatiler_tableName, retailer_joins, retailer_WhereClouse, reatiler_offset, retailer_limit, ` order by ${SF_RETAILER_TABLE_NAME}.createddate desc`);
                console.log("Retailer sql ------> ", retailer_sql);
                let retailer_sql_res = await client.query(retailer_sql);

                if (retailer_sql_res.rows.length > 0) {
                    let visit_completed_status_id = await getPicklistSfid('Visit__c','Visit_Status__c','Completed')
                    retailer_sql_res.rows.map((sfids) => {
                        retailer_arr.push(sfids['sfid'])
                    });
                    console.log('dealer_arr:::::::::::', dealer_arr)
                    for (let i = 0; i < retailer_sql_res.rows.length; i++) {

                        ////// order by last visited date which is in descending order /////
                        const fields = ['visit_date__c'];
                        const tableName = SF_VISIT_TABLE_NAME;
                        let offset = '0', limit = '1';
                        let orderBy = 'ORDER BY visit_date__c desc'

                        const WhereClouse = [];

                        // WhereClouse.push({fieldValue: req.query.team__c, fieldName: 'team_id__c'});
                        WhereClouse.push({ fieldValue: visit_completed_status_id, fieldName: 'visit_status__c' });
                        WhereClouse.push({ fieldValue: retailer_sql_res.rows[i]['sfid'], fieldName: 'retailer__c' });

                        let visit_sql1 = qry.SelectAllQry(fields, tableName, WhereClouse, offset, limit, orderBy);
                        console.log('visit sql for date ------->', visit_sql1);

                        let visit_result = await client.query(visit_sql1);
                        // console.log('test for visit date --->',visit_result.rows);
                        // console.log('last visit date ---> ', dtUtil.ISOtoLocal(visit_result.rows[0].visit_date__c));
                        if (visit_result.rows.length > 0) {
                            retailer_sql_res.rows[i]['last_visit_date'] = dtUtil.ISOtoLocal(visit_result.rows[0]['visit_date__c'])
                        }
                        else {
                            retailer_sql_res.rows[i]['last_visit_date'] = null
                        }
                    }
                    let check_visit_retailer_sql = `SELECT * FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_VISIT_TABLE_NAME} 
                         WHERE dealer_retailer__c IN ('${retailer_arr.join("','")}') and emp_id__c='${emp_id}' and visit_date__c='${pjp_date}';`
                    let check_visit_retailer_sql_res = await client.query(check_visit_retailer_sql);
                    console.log('check_visit_dealer_retailer_sql:::::::', check_visit_retailer_sql)
                    count_retailer_related_visit = check_visit_retailer_sql_res.rows.length
                    console.log('length ---->', check_visit_retailer_sql_res.rows.length)

                    retailer_data_arr = [...retailer_sql_res.rows]

                }
                return [parent_id_arr, territory_id_arr, party_data_arr, count_dealer_related_visit ,retailer_data_arr , count_retailer_related_visit];
            } else {
                return [parent_id_arr, territory_id_arr, party_data_arr, count_dealer_related_visit ,retailer_data_arr , count_retailer_related_visit];
            }
        } else {
            return [parent_id_arr, territory_id_arr, party_data_arr, count_dealer_related_visit ,retailer_data_arr , count_retailer_related_visit];
        }

    } catch (e) {
        console.log('Error In Beat Grid Wise Party Function ------>', e);
    }
}

// async function getRegionWiseData(data){
//     try{
//         let zone_sfid_arr = [];
//         let zone_data = [];
//         let region_sfid_arr = [];
//         let region_data = [];
//         let branch_sfid_arr = [];
//         let branch_data = [];
//         let territory_sfid_arr = [];
//         let territory_data = [];
//         let asm_data = [];
//         let asm_sfid_arr = [];
//         let town_data = [];
//         let town_sfid_arr = [];
//         let beat_data = [];
//         let beat_sfid_arr = [];
//         let grid_data = [];
//         let grid_sfid_arr = [];
//         let se_data = [];
//         let se_sfid_arr = [];

//         let fields = [`${SF_AREA_1_TABLE_NAME}.*, a1.name__c as parent_code_name`]
//         let tableName = SF_AREA_1_TABLE_NAME;
//         var offset='0', limit='100';
//         let WhereClouse = [];
//         //WhereClouse.push({ "fieldName": "town_code__c", "fieldValue": grid_sql_res.rows});  
//         WhereClouse.push({ "fieldName": "area1__c.name__c", "fieldValue": data});  
//         WhereClouse.push({ "fieldName": "area1__c.area_level__c", "fieldValue": 'a050w000002jazWAAQ'});
// //*********************************************** Zone Starts Here *********************************************************/
//         let joins = [
//             {
//                 "type": "LEFT",
//                 "table_name": `${SF_AREA_1_TABLE_NAME} as a1`,
//                 "p_table_field": `${SF_AREA_1_TABLE_NAME}.parent_code__c`,
//                 "s_table_field": `a1.sfid `
//             }
//         ]

//         let zone_sql = qry.fetchAllWithJoinQry(fields, tableName, joins, WhereClouse, offset, limit,' order by createddate desc');
//         console.log("Zone sql ------> ", zone_sql);
//         let zone_sql_res = await client.query(zone_sql);

//         if(zone_sql_res.rows.length > 0){
//             zone_sql_res.rows.map((sfids)=> {
//                 zone_sfid_arr.push(sfids['sfid'])  
//                 let obj = {
//                     zone_id: sfids['sfid'],
//                     zone_name: sfids['name__c'],
//                     zone_parent_code: sfids['parent_code__c'],
//                     zone_parent_code_name: sfids['parent_code_name'],
//                     zone_code :sfids['name']
//                 };
//                 zone_data.push(obj);              
//             });
//         }else{
//             zone_sfid_arr.push(data);
//             zone_sfid_arr = await sort.removeDuplicates(zone_sfid_arr);
//             console.log('inside if condition zone idss ----->',zone_sfid_arr);
//         }
// //*****************************************************  Zone Ends Here   *******************************************************/
//         if(zone_sql_res.rows.length > 0 || (data.length > 0 && data != undefined)){
            
//             let fields = [`${SF_AREA_1_TABLE_NAME}.*, a2.name__c as parent_code_name`]
//             let tableName = SF_AREA_1_TABLE_NAME;
//             var offset='0', limit='100';
//             let WhereClouse = [];
//             //WhereClouse.push({ "fieldName": "town_code__c", "fieldValue": grid_sql_res.rows});  
//             WhereClouse.push({ "fieldName": "area1__c.parent_code__c", "fieldValue": zone_sfid_arr ,"type": 'IN' });  
//             WhereClouse.push({ "fieldName": "area1__c.area_level__c", "fieldValue": 'a050w000002jazbAAA'});
            
//             let joins = [
//                 {
//                     "type": "LEFT",
//                     "table_name": `${SF_AREA_1_TABLE_NAME} as a2`,
//                     "p_table_field": `${SF_AREA_1_TABLE_NAME}.parent_code__c`,
//                     "s_table_field": `a2.sfid `
//                 }
//             ]

//             let region_sql = qry.fetchAllWithJoinQry(fields, tableName, joins, WhereClouse, offset, limit,' order by createddate desc');
//             console.log("Region sql ------> ", region_sql);
//             let region_sql_res = await client.query(region_sql);

//             if(region_sql_res.rows.length > 0){
//                 region_sql_res.rows.map((sfids)=> {
//                     region_sfid_arr.push(sfids['sfid'])   
//                     let obj = {
//                         region_id: sfids['sfid'],
//                         region_name: sfids['name__c'],
//                         region_parent_code: sfids['parent_code__c'],
//                         region_parent_code_name : sfids['parent_code_name'],
//                         region_code : sfids['name']
//                     };
//                     region_data.push(obj);              
//                 });
//             }else{
//                 region_sfid_arr.push(data);
//                 region_sfid_arr = await sort.removeDuplicates(region_sfid_arr);
//                 console.log('inside if condition region idss ----->',region_sfid_arr);
//             }
// //******************************************* Region Ends Here  **************************************************/
//             if(region_sql_res.rows.length > 0 || (data.length > 0 && data != undefined)){

//                 let fields = [`${SF_AREA_1_TABLE_NAME}.*, a3.name__c as parent_code_name`]
//                 let tableName = SF_AREA_1_TABLE_NAME;
//                 var offset='0', limit='100';
//                 let WhereClouse = [];
//                 //WhereClouse.push({ "fieldName": "town_code__c", "fieldValue": grid_sql_res.rows});  
//                 WhereClouse.push({ "fieldName": "area1__c.parent_code__c", "fieldValue": region_sfid_arr ,"type": 'IN' });  
//                 WhereClouse.push({ "fieldName": "area1__c.area_level__c", "fieldValue": 'a050w000002jazqAAA'});

//                 let joins = [
//                     {
//                         "type": "LEFT",
//                         "table_name": `${SF_AREA_1_TABLE_NAME} as a3`,
//                         "p_table_field": `${SF_AREA_1_TABLE_NAME}.parent_code__c`,
//                         "s_table_field": `a3.sfid `
//                     }
//                 ]

//                 let branch_sql = qry.fetchAllWithJoinQry(fields, tableName, joins, WhereClouse, offset, limit,' order by createddate desc');
//                 console.log("Branch sql ------> ", branch_sql);
//                 let branch_sql_res = await client.query(branch_sql);

//                 if(branch_sql_res.rows.length > 0){
//                     branch_sql_res.rows.map((sfids)=> {
//                         branch_sfid_arr.push(sfids['sfid'])  
//                         let obj = {
//                             branch_id: sfids['sfid'],
//                             branch_name: sfids['name__c'],
//                             branch_parent_code: sfids['parent_code__c'],
//                             branch_parent_code_name: sfids['parent_code_name'],
//                             branch_code :sfids['name']
//                         };
//                         branch_data.push(obj);              
//                     });
//                 }else{
//                     branch_sfid_arr.push(data);
//                     branch_sfid_arr = await sort.removeDuplicates(branch_sfid_arr);
//                     console.log('inside if condition branch idss ----->',branch_sfid_arr);
//                 }
// //********************************** Branch Ends Here ****************************************************/
//                 if(branch_sql_res.rows.length > 0 || (data.length > 0 && data != undefined)){
                    
//                     let fields = [`${SF_AREA_1_TABLE_NAME}.*, ${SF_PICKLIST_TABLE_NAME}.name as area_type_name, a4.name__c as parent_code_name`]
//                     let tableName = SF_AREA_1_TABLE_NAME;
//                     var offset='0', limit='100';
//                     let WhereClouse = [];
//                     WhereClouse.push({ "fieldName": "area1__c.parent_code__c", "fieldValue": branch_sfid_arr ,"type": 'IN' });  
//                     WhereClouse.push({ "fieldName": "area1__c.area_level__c", "fieldValue": 'a050w000002jb05AAA'});

//                     let joins = [
//                         {
//                             "type": "LEFT",
//                             "table_name": `${SF_PICKLIST_TABLE_NAME}`,
//                             "p_table_field": `${SF_AREA_1_TABLE_NAME}.area_type__c`,
//                             "s_table_field": `${SF_PICKLIST_TABLE_NAME}.sfid `
//                         },
//                         {
//                             "type": "LEFT",
//                             "table_name": `${SF_AREA_1_TABLE_NAME} as a4`,
//                             "p_table_field": `${SF_AREA_1_TABLE_NAME}.parent_code__c`,
//                             "s_table_field": `a4.sfid `
//                         }
//                     ]

//                     let asm_sql = qry.fetchAllWithJoinQry(fields, tableName, joins, WhereClouse, offset, limit,' order by createddate desc');
//                     console.log("ASM sql ------> ", asm_sql);
//                     let asm_sql_res = await client.query(asm_sql);

//                     if(asm_sql_res.rows.length > 0){
//                         asm_sql_res.rows.map((sfids)=> {
//                             asm_sfid_arr.push(sfids['sfid'])  
//                             let obj = {
//                                 asm_id: sfids['sfid'],
//                                 asm_name: sfids['name__c'],
//                                 asm_parent_code: sfids['parent_code__c'],
//                                 asm_parent_code_name: sfids['parent_code_name'],
//                                 area_type : sfids['area_type__c'],
//                                 area_type_name : sfids['area_type_name'],
//                                 area_code: sfids['name']
//                             };
//                             asm_data.push(obj);              
//                         });
//                     }else{
//                         asm_sfid_arr.push(data);
//                         asm_sfid_arr = await sort.removeDuplicates(asm_sfid_arr);
//                         console.log('inside if condition asm idss ----->',asm_sfid_arr);
//                     }
//  //*********************************** Asm ENds Here ***************************************************************/ 
//                     if(asm_sql_res.rows.length > 0 || (data.length > 0 && data != undefined)){
//                         let fields = [`${SF_AREA_1_TABLE_NAME}.*, ${SF_PICKLIST_TABLE_NAME}.name as area_type_name, a5.name__c as parent_code_name`]
//                         let tableName = SF_AREA_1_TABLE_NAME;
//                         var offset='0', limit='100';
//                         let WhereClouse = [];
//                         WhereClouse.push({ "fieldName": "area1__c.parent_code__c", "fieldValue": asm_sfid_arr ,"type": 'IN' });  
//                         //WhereClouse.push({ "fieldName": "area1__c.territory_type__c", "fieldValue": 'a050w000002jNn6AAE'});

//                         let joins = [
//                             {
//                                 "type": "LEFT",
//                                 "table_name": `${SF_PICKLIST_TABLE_NAME}`,
//                                 "p_table_field": `${SF_AREA_1_TABLE_NAME}.area_type__c`,
//                                 "s_table_field": `${SF_PICKLIST_TABLE_NAME}.sfid `
//                             },
//                             {
//                                 "type": "LEFT",
//                                 "table_name": `${SF_AREA_1_TABLE_NAME} as a5`,
//                                 "p_table_field": `${SF_AREA_1_TABLE_NAME}.parent_code__c`,
//                                 "s_table_field": `a5.sfid `
//                             }
//                         ]

//                         let se_sql = qry.fetchAllWithJoinQry(fields, tableName, joins, WhereClouse, offset, limit,' order by createddate desc');
//                         console.log("SE sql ------> ", se_sql);
//                         let se_sql_res = await client.query(se_sql);

//                         if(se_sql_res.rows.length > 0){
//                             se_sql_res.rows.map((sfids)=> {
//                                 se_sfid_arr.push(sfids['sfid'])  
//                                 let obj = {
//                                     se_id: sfids['sfid'],
//                                     se_name: sfids['name__c'],
//                                     se_parent_code: sfids['parent_code__c'],
//                                     se_parent_code_name: sfids['parent_code_name'],
//                                     se_code: sfids['name'],
//                                     se_type : sfids['area_type__c'],
//                                     se_type_name : sfids['territory_type_name']
//                                 };
//                                 se_data.push(obj);              
//                             });
//                         }else{
//                             se_sfid_arr.push(data);
//                             se_sfid_arr = await sort.removeDuplicates(se_sfid_arr);
//                             console.log('inside if condition asm idss ----->',se_sfid_arr);
//                         }

//  //*********************************** SE Ends Here ***************************************************************/                   
//                     if(asm_sql_res.rows.length > 0 || (data.length > 0 && data != undefined)){
//                         let fields = [`${SF_AREA_1_TABLE_NAME}.*, ${SF_PICKLIST_TABLE_NAME}.name as area_type_name, a5.name__c as parent_code_name , p1.name as territory_type_name`]
//                         let tableName = SF_AREA_1_TABLE_NAME;
//                         var offset='0', limit='100';
//                         let WhereClouse = [];
//                         WhereClouse.push({ "fieldName": "area1__c.parent_code__c", "fieldValue": se_sfid_arr ,"type": 'IN' });  
//                         WhereClouse.push({ "fieldName": "area1__c.territory_type__c", "fieldValue": 'a050w000002jNn6AAE'});

//                         let joins = [
//                             {
//                                 "type": "LEFT",
//                                 "table_name": `${SF_PICKLIST_TABLE_NAME}`,
//                                 "p_table_field": `${SF_AREA_1_TABLE_NAME}.area_type__c`,
//                                 "s_table_field": `${SF_PICKLIST_TABLE_NAME}.sfid `
//                             },
//                             {
//                                 "type": "LEFT",
//                                 "table_name": `${SF_AREA_1_TABLE_NAME} as a5`,
//                                 "p_table_field": `${SF_AREA_1_TABLE_NAME}.parent_code__c`,
//                                 "s_table_field": `a5.sfid `
//                             },
//                             {
//                                 "type": "LEFT",
//                                 "table_name": `${SF_PICKLIST_TABLE_NAME} as p1`,
//                                 "p_table_field": `${SF_AREA_1_TABLE_NAME}.territory_type__c`,
//                                 "s_table_field": `p1.sfid `
//                             },
//                         ]

//                         let territory_sql = qry.fetchAllWithJoinQry(fields, tableName, joins, WhereClouse, offset, limit,' order by createddate desc');
//                         console.log("Territory sql ------> ", territory_sql);
//                         let territory_sql_res = await client.query(territory_sql);

//                         if(territory_sql_res.rows.length > 0){
//                             territory_sql_res.rows.map((sfids)=> {
//                                 territory_sfid_arr.push(sfids['sfid'])  
//                                 let obj = {
//                                     territory_id: sfids['sfid'],
//                                     territory_name: sfids['name__c'],
//                                     territory_parent_code: sfids['parent_code__c'],
//                                     territory_parent_code_name: sfids['parent_code_name'],
//                                     territory_code: sfids['name'],
//                                     area_type : sfids['area_type__c'],
//                                     area_type_name : sfids['area_type_name'],
//                                     territory_type : sfids['territory_type__c'],
//                                     territory_type_name : sfids['territory_type_name'],
//                                 };
//                                 territory_data.push(obj);              
//                             });
//                         }else{
//                             territory_sfid_arr.push(data);
//                             territory_sfid_arr = await sort.removeDuplicates(territory_sfid_arr);
//                             console.log('inside if condition asm idss ----->',territory_sfid_arr);
//                         }
// //********************************** Territory Ends Here *****************************************************/
//                         if(territory_sql_res.rows.length > 0 || (data.length > 0 && data != undefined)){

//                             let territory_town_data = await territoryWiseTown(territory_sfid_arr);
//                             // console.log('**********IMP DATA*********',territory_town_data);
//                             if(territory_town_data.length > 0){
//                                 territory_town_data.map((sfids)=> {
//                                     town_sfid_arr.push(sfids['town_code__c'])  
//                                     let obj = {
//                                         town_id: sfids['sfid'],
//                                         town_name: sfids['town_name__c'],
//                                         geographical_town_name: sfids['geographical_town_name__c'],
//                                         geographical_town : sfids['geographical_town__c'],
//                                         town_code : sfids['town_code__c'],
//                                         town_parent_code : sfids['territory_code__c'],
//                                         town_code__c : sfids['town_code']
//                                     };
//                                     town_data.push(obj);              
//                                 });
//                             }else{
//                                 town_sfid_arr.push(data);
//                                 town_sfid_arr = await sort.removeDuplicates(town_sfid_arr);
//                                 console.log('inside if condition town idss ----->',town_sfid_arr);
//                             }

//                             if(territory_town_data.length > 0 || (data.length > 0 && data != undefined)){

//                                 let town_wise_beats = await townWiseBeatGrid(town_sfid_arr);
//                                 let town_wise_grid = await townWiseGrid(town_sfid_arr);
//                                 //console.log('**********IMP DATA*********',town_wise_beats);
//                                 if(town_wise_beats.length > 0){
//                                     town_wise_beats.map((sfids)=> {
//                                         beat_sfid_arr.push(sfids['sfid'])  
//                                         let obj = {
//                                             beat_id: sfids['sfid'],
//                                             beat_name: sfids['name'],
//                                             geographical_town_name: sfids['name__c'],
//                                             geographical_town : sfids['geographical_town__c'], 
//                                             beat_parent_code : sfids['parent_code__c'],
//                                             grid_coordinates : sfids['grid_cordinates__c'],
//                                             perimeter : sfids['perimeter__c'],
//                                             border_width : sfids['border_width__c'],
//                                             grid_area : sfids['grid_area__c'],
//                                             grid_frequency : sfids['grid_frequency__c'],
//                                             no_of_days_to_complete_grid : sfids['no_of_days_to_complete_grid__c'],

//                                         };
//                                         beat_data.push(obj);              
//                                     });
//                                 }
//                                 if(town_wise_grid.length > 0){
//                                     town_wise_grid.map((sfids)=> {
//                                         grid_sfid_arr.push(sfids['sfid'])  
//                                         let obj = {
//                                             grid_id: sfids['sfid'],
//                                             grid_name: sfids['name__c'],
//                                             geographical_town_name: sfids['geographical_name'],
//                                             geographical_town : sfids['geographical_town__c'], 
//                                             grid_parent_code : sfids['parent_code__c'],
//                                             grid_coordinates : sfids['grid_cordinates__c'],
//                                             perimeter : sfids['perimeter__c'],
//                                             border_width : sfids['border_width__c'],
//                                             grid_area : sfids['grid_area__c'],
//                                             grid_frequency : sfids['grid_frequency__c'],
//                                             no_of_days_to_complete_grid : sfids['no_of_days_to_complete_grid__c'],
//                                             grid_potential : sfids['grid_potential__c'],


//                                         };
//                                         grid_data.push(obj);              
//                                     });
//                                 }
//                                 return [zone_sfid_arr,zone_data,region_sfid_arr,region_data,branch_sfid_arr,branch_data,territory_sfid_arr,territory_data,asm_sfid_arr,asm_data,town_sfid_arr,town_data,beat_data,beat_sfid_arr,grid_data,grid_sfid_arr];
//                             }else{
//                                 return [zone_sfid_arr,zone_data,region_sfid_arr,region_data,branch_sfid_arr,branch_data,territory_sfid_arr,territory_data,asm_sfid_arr,asm_data,town_sfid_arr,town_data,beat_data,beat_sfid_arr,grid_data,grid_sfid_arr];
//                             }                           
//                         }else{
//                             return [zone_sfid_arr,zone_data,region_sfid_arr,region_data,branch_sfid_arr,branch_data,territory_sfid_arr,territory_data,asm_sfid_arr,asm_data,town_sfid_arr,town_data,beat_data,beat_sfid_arr,grid_data,grid_sfid_arr];                       
//                         }
//                     }else{
//                         return [zone_sfid_arr,zone_data,region_sfid_arr,region_data,branch_sfid_arr,branch_data,territory_sfid_arr,territory_data,asm_sfid_arr,asm_data,town_sfid_arr,town_data,beat_data,beat_sfid_arr,grid_data,grid_sfid_arr];                    
//                     }
//                     }else{
//                         return [zone_sfid_arr,zone_data,region_sfid_arr,region_data,branch_sfid_arr,branch_data,territory_sfid_arr,territory_data,asm_sfid_arr,asm_data,town_sfid_arr,town_data,beat_data,beat_sfid_arr,grid_data,grid_sfid_arr];                    
//                     }
//                 }else{
//                     return [zone_sfid_arr,zone_data,region_sfid_arr,region_data,branch_sfid_arr,branch_data,territory_sfid_arr,territory_data,asm_sfid_arr,asm_data,town_sfid_arr,town_data,beat_data,beat_sfid_arr,grid_data,grid_sfid_arr];               
//                 }
//             }else{
//                 return [zone_sfid_arr,zone_data,region_sfid_arr,region_data,branch_sfid_arr,branch_data,territory_sfid_arr,territory_data,asm_sfid_arr,asm_data,town_sfid_arr,town_data,beat_data,beat_sfid_arr,grid_data,grid_sfid_arr];            
//             }
//         }else{
//             return [zone_sfid_arr,zone_data,region_sfid_arr,region_data,branch_sfid_arr,branch_data,territory_sfid_arr,territory_data,asm_sfid_arr,asm_data,town_sfid_arr,town_data,beat_data,beat_sfid_arr,grid_data,grid_sfid_arr];        
//         }
//     }catch(e){
//         console.log('Error in Get Region Wise Data Function ----->',e);
//     }
// }

//@CHECK
async function getLeadFromTerritory(territory_id_arr){
    try{
        let town_id_arr = [];
        let lead_data = [];
        let fields = ['*']
        let tableName = SF_AREA_1_TABLE_NAME;
        var offset='0', limit='100';
        let WhereClouse = [];
        WhereClouse.push({ "fieldName": "parent_code__c", "fieldValue": territory_id_arr ,"type": 'IN'});   
        let town_sql = qry.SelectAllQry(fields, tableName,WhereClouse, offset, limit,' order by createddate desc');
        console.log("town sql ------> ", town_sql);
        let town_sql_res = await client.query(town_sql);

        if(town_sql_res.rows.length > 0){

            town_sql_res.rows.map((sfids)=> {
                town_id_arr.push(sfids['sfid'])              
            });
            town_id_arr = await sort.removeDuplicates(town_id_arr);

            // let fields = ['*']
            // let tableName = SF_LEAD_TABLE_NAME;
            // var offset='0', limit='100';
            // let WhereClouse = [];
            // WhereClouse.push({ "fieldName": "town_name__c", "fieldValue": town_id_arr ,"type": 'IN' });  
            // let lead_sql = qry.SelectAllQry(fields, tableName,WhereClouse, offset, limit,' order by createddate desc');

            let fields = [`${SF_LEAD_TABLE_NAME}.*,
                            ${SF_PICKLIST_TABLE_NAME}.name as lead_type,
                            T1.town_name__c as town_name_name,
                            pk1.name as site_quality,
                            pk2.name as site_type,
                            pk3.name as drop_reason,
                            pk4.name as site_fir,
                            ${SF_TEAM_TABLE_NAME}.team_member_name__c as lead_assignment,
                            team1.team_member_name__c as lead_created_by
                        `]
            let tableName = SF_LEAD_TABLE_NAME;
            var offset='0', limit='100';
            let WhereClouse = [];
            WhereClouse.push({ "fieldName": `${SF_LEAD_TABLE_NAME}.town_name__c`, "fieldValue": town_id_arr ,"type": 'IN'}); 
            let joins = [
                {
                    "type": "LEFT",
                    "table_name": `${SF_PICKLIST_TABLE_NAME}`,
                    "p_table_field": `${SF_LEAD_TABLE_NAME}.lead_type__c`,
                    "s_table_field": `${SF_PICKLIST_TABLE_NAME}.sfid `
                },
                {
                    "type": "LEFT",
                    "table_name": `${SF_PICKLIST_TABLE_NAME} as pk1`,
                    "p_table_field": `${SF_LEAD_TABLE_NAME}.site_quality__c`,
                    "s_table_field": `pk1.sfid `
                },
                {
                    "type": "LEFT",
                    "table_name": `${SF_PICKLIST_TABLE_NAME} as pk2`,
                    "p_table_field": `${SF_LEAD_TABLE_NAME}.site_type__c`,
                    "s_table_field": `pk2.sfid `
                },
                {
                    "type": "LEFT",
                    "table_name": `${SF_PICKLIST_TABLE_NAME} as pk3`,
                    "p_table_field": `${SF_LEAD_TABLE_NAME}.drop_reason__c`,
                    "s_table_field": `pk3.sfid `
                },
                {
                    "type": "LEFT",
                    "table_name": `${SF_PICKLIST_TABLE_NAME} as pk4`,
                    "p_table_field": `${SF_LEAD_TABLE_NAME}.site_fir__c`,
                    "s_table_field": `pk4.sfid `
                },
                {
                    "type": "LEFT",
                    "table_name": `${SF_TEAM_TABLE_NAME}`,
                    "p_table_field": `${SF_LEAD_TABLE_NAME}.lead_assignment__c`,
                    "s_table_field": `${SF_TEAM_TABLE_NAME}.sfid `
                },
                {
                    "type": "LEFT",
                    "table_name": `${SF_TEAM_TABLE_NAME} as team1`,
                    "p_table_field": `${SF_LEAD_TABLE_NAME}.lead_created_by__c`,
                    "s_table_field": `team1.sfid `
                },
                {
                    "type": "LEFT",
                    "table_name": `${SF_TOWN_TERRITORY_MAPPING_TABLE_NAME} as T1`,
                    "p_table_field": `${SF_LEAD_TABLE_NAME}.town_name__c`,
                    "s_table_field": `T1.town_code__c `
                }
            ]

            let lead_sql = qry.fetchAllWithJoinQry(fields, tableName, joins, WhereClouse,offset,limit,`order by ${SF_LEAD_TABLE_NAME}.createddate desc`);
            console.log("lead sql ------> ", lead_sql);
            let lead_sql_res = await client.query(lead_sql);

            if(lead_sql_res.rows.length > 0){
                
                return lead_sql_res.rows
            }else{
                return lead_data;
            }
        }else{
            return lead_data;
        }
    }catch(e){
        console.log('Error in Get town on lead Data Function ----->',e);
    }
}

async function getTeamManager(team_id){
    try{
        let manager_name = [];
        let fields = ['*']
        let tableName = SF_TEAM_TABLE_NAME;
        var offset='0', limit='1000';
        let WhereClouse = [];
        WhereClouse.push({ "fieldName": "sfid", "fieldValue": team_id}); 
        
        let sql = qry.SelectAllQry(fields, tableName,WhereClouse, offset, limit,' order by createddate desc');
        console.log("sql ---->", sql);
        let sql_res = await client.query(sql); 

        if(sql_res.rows.length > 0){
            sql_res.rows.map((sfids)=> {
                manager_name.push(sfids['team_member_name__c'])              
            });

            return manager_name;
        }else{
            return manager_name
        }
    }catch(e){
        console.log('Error in get Team Manger function ---------->',e);
    }
}

async function getTeamDayType(id,employee_type__c){
    try{
        let unique_gtm_arr = [];
        let teamterritory_type_id_arr = [];
        let gtm_id_arr = [];
        let gtm_arr = [];
        let static_value_arr = [];
        let fields = ['*']
        let tableName = SF_TEAM_TABLE_NAME;
        var offset='0', limit='100';
        let WhereClouse = [];
        WhereClouse.push({ "fieldName": "sfid", "fieldValue": id});   
        WhereClouse.push({ "fieldName": "employee_type__c", "fieldValue": employee_type__c});   
        let team_sql = qry.SelectAllQry(fields, tableName,WhereClouse, offset, limit,' order by createddate desc');
        console.log("team sql ------> ", team_sql);
        let team_sql_res = await client.query(team_sql);
        //Changes :  added one check
        if (team_sql_res.rows.length > 0) {
            team_sql_res.rows.map((sfids) => {
                if (sfids['teamterritory_type__c'] != null) {  //check
                    teamterritory_type_id_arr.push(sfids['teamterritory_type__c'])
                }
            });
            console.log('teamterritory_type__c:::::::::::::',teamterritory_type_id_arr)

            let fields = [`${SF_GTM_TABLE_NAME}.*,
                            ${SF_DAY_TYPE_PLAN_TABLE_NAME}.name as day_type_name, 
                            ${SF_DAY_TYPE_PLAN_TABLE_NAME}.sfid as day_type__id, 
                            ${SF_DAY_TYPE_PLAN_TABLE_NAME}.is_hq_town__c as is_hq_town, 
                            ${SF_DAY_TYPE_PLAN_TABLE_NAME}.is_primary_territory__c as is_primary_territory,
                            ${SF_DAY_TYPE_PLAN_TABLE_NAME}.is_non_hqtown__c as is_non_hqtown__c,
                            ${SF_DAY_TYPE_PLAN_TABLE_NAME}.is_secondary_territory__c as is_secondary_territory__c`]
            let tableName = SF_GTM_TABLE_NAME;
            var offset='0', limit='100';
            let WhereClouse = [];
            //WhereClouse.push({ "fieldName": "town_code__c", "fieldValue": grid_sql_res.rows});
            let joins = [
                {
                    "type": "LEFT",
                    "table_name": `${SF_DAY_TYPE_PLAN_TABLE_NAME}`,
                    "p_table_field": `${SF_GTM_TABLE_NAME}.day_type__c`,
                    "s_table_field": `${SF_DAY_TYPE_PLAN_TABLE_NAME}.sfid `
                }]  
                //check: added 4-03-2022
            if (teamterritory_type_id_arr.length>0) {
                WhereClouse.push({ "fieldName": `${SF_GTM_TABLE_NAME}.territory_type__c`, "fieldValue": teamterritory_type_id_arr, "type": 'IN' });
            }
                WhereClouse.push({ "fieldName": `${SF_GTM_TABLE_NAME}.employee_type__c`, "fieldValue": employee_type__c });  
            let gtm_sql = qry.fetchAllWithJoinQry(fields, tableName, joins, WhereClouse, offset, limit,` order by ${SF_GTM_TABLE_NAME}.createddate desc`);
            console.log("gtm sql ------> ", gtm_sql);
            let gtm_sql_res = await client.query(gtm_sql);
            //console.log(`Gtm Sql Res -----> ${gtm_sql_res.rows.length}`);                        
            if(gtm_sql_res.rows.length > 0){
                //console.log(`inside if`);
                let holiday_leave_sql = `SELECT ${SF_GTM_TABLE_NAME}.*,
                                    ${SF_DAY_TYPE_PLAN_TABLE_NAME}.name as day_type_name, 
                                    ${SF_DAY_TYPE_PLAN_TABLE_NAME}.sfid as day_type__id, 
                                    ${SF_DAY_TYPE_PLAN_TABLE_NAME}.is_hq_town__c as is_hq_town, 
                                    ${SF_DAY_TYPE_PLAN_TABLE_NAME}.is_primary_territory__c as is_primary_territory,
                                    ${SF_DAY_TYPE_PLAN_TABLE_NAME}.is_non_hqtown__c as is_non_hqtown__c,
                                    ${SF_DAY_TYPE_PLAN_TABLE_NAME}.is_secondary_territory__c as is_secondary_territory__c
                                     FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_GTM_TABLE_NAME} 
                                     LEFT JOIN ${process.env.TABLE_SCHEMA_NAME}.${SF_DAY_TYPE_PLAN_TABLE_NAME} ON ${SF_GTM_TABLE_NAME}.day_type__c=${SF_DAY_TYPE_PLAN_TABLE_NAME}.sfid
                                     WHERE ${SF_GTM_TABLE_NAME}.sfid IN ('a0LC7000000EWtjMAG','a0LC7000000EWteMAG','a0LC7000000EqxbMAC','a0LC7000000EqxlMAC','a0LC7000000Er0uMAC')`
                let holiday_leave_sql_res = await client.query(holiday_leave_sql);

                if(holiday_leave_sql_res.rows.length > 0){
                    let count = 0;
                    holiday_leave_sql_res.rows.map((sfids) => {
                        // gtm_sql_res.rows[count] = holiday_leave_sql_res.rows[count]
                        // count ++;
                        let static_obj = {
                            name: sfids['name'],
                            territory_type__c: sfids['territory_type__c'],
                            sfid: sfids['sfid'],
                            day_type_name: sfids['day_type_name'],
                            day_type__id: sfids['day_type__id'],
                            is_hq_town: sfids['is_hq_town'],
                            is_primary_territory: sfids['is_primary_territory'],
                            is_secondary_territory__c: sfids['is_secondary_territory__c'],
                            is_non_hqtown__c: sfids['is_non_hqtown__c']
                        }; 
                        static_value_arr.push(static_obj)
                    })
                }
                gtm_sql_res.rows.map((sfids)=> {
                    gtm_id_arr.push(sfids['territory_type__c'])
                    //gtm_id_arr.push(sfids['day_type_name'])
                    let obj = {
                        name: sfids['name'],
                        territory_type__c: sfids['territory_type__c'],
                        sfid: sfids['sfid'],
                        day_type_name: sfids['day_type_name'],
                        day_type__id: sfids['day_type__id'],
                        is_hq_town: sfids['is_hq_town'],
                        is_primary_territory: sfids['is_primary_territory'],
                        is_secondary_territory__c: sfids['is_secondary_territory__c'],
                        is_non_hqtown__c: sfids['is_non_hqtown__c']
                    }; 
                    gtm_arr.push(obj);               
                });
                // console.log(`Length ---> ${JSON.stringify(gtm_arr)}`)
                //gtm_arr = await sort.removeDuplicates(gtm_arr);
                //console.log('///////////removeDuplicates/////////////',gtm_id_arr)
                let data= await getUniqueValuesForDayType(gtm_arr)
                unique_gtm_arr = [...data,...static_value_arr]
                //console.log(`Unique Gtm Arr ----> ${JSON.stringify(unique_gtm_arr)}`);

                return  [teamterritory_type_id_arr,gtm_id_arr,gtm_arr,unique_gtm_arr]
            }else{
                return [teamterritory_type_id_arr,gtm_id_arr,gtm_arr,unique_gtm_arr];
            }
        }else{
            return [teamterritory_type_id_arr,gtm_id_arr,gtm_arr,unique_gtm_arr];
        }

    }catch(e){
        console.log('Error In Team Day Type Wise Function ------>',e);
    }
}

async function getAllTownFunc(data){
    try{
        let fields = ['*']
        let town_sfid_arr = [];
        let grid_data = [];
        let grid_sfid_arr = [];
        let team_data = [];
        let tableName = SF_TOWN_TERRITORY_MAPPING_TABLE_NAME;
        var offset='0', limit='1000';
        let WhereClouse = []; 
        let getAllTownFunc_sql = qry.SelectAllQry(fields, tableName,WhereClouse, offset, limit,' order by createddate desc');
        console.log("Territory sql ------> ", getAllTownFunc_sql);
        let getAllTownFunc_sql_res = await client.query(getAllTownFunc_sql);

        if(getAllTownFunc_sql_res.rows.length > 0){
            getAllTownFunc_sql_res.rows.map((sfids)=> {
                town_sfid_arr.push(sfids['town_code__c'])  
                    let obj = {
                        town_id : sfids['sfid'],
                        town_code: sfids['town_code__c'],
                        town_name: sfids['town_name__c'],
                        geographical_town: sfids['geographical_town__c'],
                        geographical_town_name : sfids['geographical_town_name__c'],
                        town_parent_code : sfids['territory_code__c'],
                        territory_name: sfids['territory_name__c']
                    };
                team_data.push(obj);              
            });
        }else{
                            territory_sfid_arr.push(data);
                            territory_sfid_arr = await sort.removeDuplicates(territory_sfid_arr);
                            console.log('inside if condition asm idss ----->',town_sfid_arr);
                        }
                        if(team_data.length > 0 || (team_data.length > 0 && team_data != undefined)){

                            let town_wise_grid = await townWiseGrid(town_sfid_arr);
                            console.log('**********IMP DATA*********',town_sfid_arr);

                            if(town_wise_grid.length > 0){
                                town_wise_grid.map((sfids)=> {
                                    grid_sfid_arr.push(sfids['sfid'])  
                                    let obj = {
                                        grid_id: sfids['sfid'],
                                        grid_name: sfids['name'],
                                        geographical_town_name: sfids['geographical_name'],
                                        geographical_town : sfids['geographical_town__c'], 
                                        grid_parent_code : sfids['parent_code__c'],
                                        grid_coordinates : sfids['grid_cordinates__c'],
                                        perimeter : sfids['perimeter__c'],
                                        border_width : sfids['border_width__c'],
                                        grid_area : sfids['grid_area__c'],
                                        grid_frequency : sfids['grid_frequency__c'],
                                        no_of_days_to_complete_grid : sfids['no_of_days_to_complete_grid__c'],
                                        grid_potential : sfids['grid_potential__c'],


                                    };
                                    grid_data.push(obj);              
                                });
                            }
                            // let unique_grid_arr= await getUniqueValuesForDayType(grid_data)
                            unique_grid_arr = await sort.removeDuplicates(grid_data);

                            return [team_data, grid_data, unique_grid_arr];
                        }else{
                            return [team_data, grid_data, unique_grid_arr];
                        }                     

                        // return [team_data];
   }catch(e){
        console.log('Error in Get All town Data Function ----->',e);
    }
}
async function getWorkingHour(team__c, visit_date) {
    try {
        // get visits of the day
        let field = [`*`]
        let visit_table_name = SF_VISIT_TABLE_NAME;
        let offset = '0', limit = '1000';
        let visit_where_clouse = [];
        let update_result = {};
        visit_where_clouse.push({ "fieldName": "visit_date__c", "fieldValue": visit_date });
        visit_where_clouse.push({ "fieldName": "emp_id__c", "fieldValue": team__c });
        visit_where_clouse.push({ "fieldName": "sfid", "type": "NOTNULL" });

        let visit_sql = await qry.SelectAllQry(field, visit_table_name, visit_where_clouse, offset, limit, ` order by check_in_time__c asc`);
        let visit2_sql = await qry.SelectAllQry(field, visit_table_name, visit_where_clouse, offset, limit, ` order by check_out_time__c desc`);
        console.log("Visit SQL ::: ", visit_sql);
        //console.log("Visit2 SQL ::: ", visit2_sql);
        let visit_sql_res = await client.query(visit_sql);
        let visit2_sql_res = await client.query(visit2_sql);
        if (visit_sql_res.rows.length > 0) {
            // console.log(`Visit List for ${visit_date}:::on start basis `, visit_sql_res.rows);
            // console.log(`Visit List for ${visit_date}:::on end basis `, visit2_sql_res.rows);

            // let visit_count = 0;
            // let started_visit_count = 0;
            // let completed_visit_count = 0;
            // if(visit_sql_res.rows.length > 0){
            //     visit_sql_res.rows.forEach((visit)=>{
            //         if(visit['sfid']){
            //             visit_count ++;
            //             if(visit['check_out_time__c'] && visit['check_out_time__c'] != null){
            //                 completed_visit_count ++;
            //             }
            //             if(visit['check_in_time__c'] && visit['check_in_time__c'] != null){
            //                 started_visit_count ++;
            //             }
            //         }
            //     })
            // }
            // console.log(`Visit data logs : visit_count- ${visit_count} : started_visit_count- ${started_visit_count} : completed_visit_count- ${completed_visit_count}`);
            // If visit_count > completed_visit, check for first visit and last visit
            let first_visit, last_visit = '';
            if (visit_sql_res.rows.length > 0) {
                first_visit = visit_sql_res.rows[0];
            }
            if (visit2_sql_res.rows.length > 0) {
                last_visit = visit2_sql_res.rows[0];
            }

            // console.log('First Visit', first_visit);
            // console.log('Last Visit', last_visit);
            let exact_visit_time_exhausted__c = 0;

            let exact_visit_time_paused__c2 = 0;
            visit_sql_res.rows.forEach((visit) => {
                if (visit['check_in_time__c'] & visit['check_out_time__c']) {
                    let check_in_time__c = moment(visit['check_in_time__c']);
                    let check_out_time__c = moment(visit['check_out_time__c']);
                    let pause_in_time__c = moment(visit['pause_in_time__c']);
                    let pause_out_time__c = moment(visit['pause_out_time__c']);
                    let duration = moment.duration(check_out_time__c.diff(check_in_time__c));
                    let pause_duration = moment.duration(pause_out_time__c.diff(pause_in_time__c));
                    //let pause_diff=duration-pause_duration;
                    // console.log('pause_in_time__c::::::::::::::',pause_in_time__c)
                    // console.log('pause_out_time__c::::::::::::::::::',pause_out_time__c)

                    let pause_hour = pause_duration.asHours();
                    let hour = duration.asHours();
                    exact_visit_time_exhausted__c = exact_visit_time_exhausted__c + hour;
                    exact_visit_time_paused__c2 = exact_visit_time_paused__c2 + (pause_hour ? pause_hour : 0);
                }
            })
            // console.log('exact_visit_time_exhausted__c ::: ',exact_visit_time_paused__c2 )


            exact_visit_time_paused__c2 = exact_visit_time_paused__c2 ? exact_visit_time_paused__c2 : 0;
            // let fieldValue = [];
            let visit_working_hour_start = moment(first_visit['check_in_time__c']);
            let visit_working_hour_end = moment(last_visit['check_out_time__c']);
            let working_duration = moment.duration(visit_working_hour_end.diff(visit_working_hour_start));
            let working_duration2 = working_duration.asHours();
            //let pause_duration = moment.duration(working_duration.diff(exact_visit_time_paused__c2));
            let total_visit_working_hour = working_duration2 - exact_visit_time_paused__c2;
            // fieldValue.push({ "field": "working_hours_start__c", "value": moment(first_visit['check_in_time__c']).format('YYYY-MM-DD hh:mm:ss')});
            // fieldValue.push({ "field": "working_hours_end__c", "value": moment(last_visit['check_out_time__c']).format('YYYY-MM-DD hh:mm:ss')});
            // fieldValue.push({ "field": "visit_working_hour__c", "value":  working_duration.asHours()});
            // fieldValue.push({ "field": "exact_visit_time_exhausted__c", "value": exact_visit_time_exhausted__c });
            //console.log('result of pause:::::::::::::::::::::',total_visit_working_hour)

            update_result = {
                "working_hours_start__c": moment(first_visit['check_in_time__c']).format('YYYY-MM-DD hh:mm:ss'),
                "working_hours_end__c": moment(last_visit['check_out_time__c']).format('YYYY-MM-DD hh:mm:ss'),
                "visit_working_hour__c": total_visit_working_hour,
                "exact_visit_time_exhausted__c": exact_visit_time_exhausted__c
            }

            return update_result;

        } else {
            return update_result;
        }   // let update_Sql = await qry.updateRecord(SF_ATTENDANCE_TABLE_NAME, fieldValue, whereClouse);


    } catch (e) {
        console.log('Error in Get All town Data Function ----->', e);
    }
}

async function getWorkingHourInMinutes(team__c, visit_date) {
    try {
        // get visits of the day
        let completed_visit_status = await getPicklistSfid('Visit__c','Visit_Status__c','Completed')
        // let total_visit_working_hour = 0
        let field = [`*`]
        let visit_table_name = SF_VISIT_TABLE_NAME;
        let offset = '0', limit = '1000';
        let visit_where_clouse = [];
        let update_result = {};
        visit_where_clouse.push({ "fieldName": "visit_date__c", "fieldValue": visit_date });
        visit_where_clouse.push({ "fieldName": "emp_id__c", "fieldValue": team__c });
        visit_where_clouse.push({ "fieldName": "sfid", "type": "NOTNULL" });
        visit_where_clouse.push({ "fieldName": "visit_status__c", "fieldValue": completed_visit_status });

        let visit_sql = await qry.SelectAllQry(field, visit_table_name, visit_where_clouse, offset, limit, ` order by check_in_time__c asc`);
        let visit2_sql = await qry.SelectAllQry(field, visit_table_name, visit_where_clouse, offset, limit, ` order by check_out_time__c desc`);
        console.log("Visit SQL ::: ", visit_sql);
        console.log("Visit SQL 2 ::: ", visit2_sql);
        //console.log("Visit2 SQL ::: ", visit2_sql);
        let visit_sql_res = await client.query(visit_sql);
        let visit2_sql_res = await client.query(visit2_sql);
        if (visit_sql_res.rows.length > 0) {
            // console.log(`Visit List for ${visit_date}:::on start basis `, visit_sql_res.rows);
            // console.log(`Visit List for ${visit_date}:::on end basis `, visit2_sql_res.rows);

            // let visit_count = 0;
            // let started_visit_count = 0;
            // let completed_visit_count = 0;
            // if(visit_sql_res.rows.length > 0){
            //     visit_sql_res.rows.forEach((visit)=>{
            //         if(visit['sfid']){
            //             visit_count ++;
            //             if(visit['check_out_time__c'] && visit['check_out_time__c'] != null){
            //                 completed_visit_count ++;
            //             }
            //             if(visit['check_in_time__c'] && visit['check_in_time__c'] != null){
            //                 started_visit_count ++;
            //             }
            //         }
            //     })
            // }
            // console.log(`Visit data logs : visit_count- ${visit_count} : started_visit_count- ${started_visit_count} : completed_visit_count- ${completed_visit_count}`);
            // If visit_count > completed_visit, check for first visit and last visit
            let first_visit, last_visit = '';
            if (visit_sql_res.rows.length > 0) {
                first_visit = visit_sql_res.rows[0];
            }
            if (visit2_sql_res.rows.length > 0) {
                last_visit = visit2_sql_res.rows[0];
            }

            // console.log('First Visit', first_visit);
            // console.log('Last Visit', last_visit);
            let exact_visit_time_exhausted__c = 0;

            let exact_visit_time_paused__c2 = 0;
            visit_sql_res.rows.forEach((visit) => {
                if (visit['check_in_time__c'] & visit['check_out_time__c']) {
                    let check_in_time__c = moment(visit['check_in_time__c']);
                    let check_out_time__c = moment(visit['check_out_time__c']);
                    let pause_in_time__c = moment(visit['pause_in_time__c']);
                    let pause_out_time__c = moment(visit['pause_out_time__c']);
                    let duration = moment.duration(check_out_time__c.diff(check_in_time__c));
                    let pause_duration = moment.duration(pause_out_time__c.diff(pause_in_time__c));
                    //let pause_diff=duration-pause_duration;
                    // console.log('pause_in_time__c::::::::::::::',pause_in_time__c)
                    // console.log('pause_out_time__c::::::::::::::::::',pause_out_time__c)

                    let pause_hour = pause_duration.asMinutes();
                    let hour = duration.asMinutes();
                    exact_visit_time_exhausted__c = exact_visit_time_exhausted__c + hour;
                    exact_visit_time_paused__c2 = exact_visit_time_paused__c2 + (pause_hour ? pause_hour : 0);
                }
            })
            // console.log('exact_visit_time_exhausted__c ::: ',exact_visit_time_paused__c2 )


            exact_visit_time_paused__c2 = exact_visit_time_paused__c2 ? exact_visit_time_paused__c2 : 0;
            // let fieldValue = [];
            let visit_working_hour_start = moment(first_visit['check_in_time__c']);
            // if(visit_working_hour_start){
            //     update_result = {
            //         "working_hours_start__c": moment(first_visit['check_in_time__c']).format('YYYY-MM-DD hh:mm:ss')
            //     }
            // }else{
            //     update_result = {
            //         "working_hours_start__c": null
            //     }
            // }
            let visit_working_hour_end = moment(last_visit['check_out_time__c']);
            // if(visit_working_hour_end){
            //     update_result = {
            //         "working_hours_end__c": moment(last_visit['check_out_time__c']).format('YYYY-MM-DD hh:mm:ss')
            //     }
            // }else{
            //     update_result = {
            //         "working_hours_end__c":null
            //     }
            // }
            let working_duration = moment.duration(visit_working_hour_end.diff(visit_working_hour_start));
            let working_duration2 = working_duration.asMinutes();
            //let pause_duration = moment.duration(working_duration.diff(exact_visit_time_paused__c2));
            let total_visit_working_hour = working_duration2 - exact_visit_time_paused__c2;

            // fieldValue.push({ "field": "working_hours_start__c", "value": moment(first_visit['check_in_time__c']).format('YYYY-MM-DD hh:mm:ss')});
            // fieldValue.push({ "field": "working_hours_end__c", "value": moment(last_visit['check_out_time__c']).format('YYYY-MM-DD hh:mm:ss')});
            // fieldValue.push({ "field": "visit_working_hour__c", "value":  working_duration.asHours()});
            // fieldValue.push({ "field": "exact_visit_time_exhausted__c", "value": exact_visit_time_exhausted__c });
            //console.log('result of pause:::::::::::::::::::::',total_visit_working_hour)
            update_result = {
                "visit_working_hour__c": total_visit_working_hour,
                "exact_visit_time_exhausted__c": exact_visit_time_exhausted__c,
                "working_hours_start__c": moment(first_visit['check_in_time__c']).format('YYYY-MM-DD hh:mm:ss'),
                "working_hours_end__c": moment(last_visit['check_out_time__c']).format('YYYY-MM-DD hh:mm:ss')

            }

            return update_result;

        } else {
            return update_result;
        }   // let update_Sql = await qry.updateRecord(SF_ATTENDANCE_TABLE_NAME, fieldValue, whereClouse);


    } catch (e) {
        console.log('Error in Get All town Data Function ----->', e);
    }
}


async function getVisitDay(date,emp_id__c,grid){
    try{
        let grid_id_arr = [];
        let grid_data = [];
        let fields = ['*']
        let tableName = SF_VISIT_TABLE_NAME;
        var offset='0', limit='100';
        let WhereClouse = [];
        if(date){
             WhereClouse.push({ "fieldName": "visit_date__c", "fieldValue": date}); 
        }
        else{
            let date = new Date();
            date = moment(date).format('YYYY-MM-DD');
            WhereClouse.push({ "fieldName": "visit_date__c", "fieldValue": date}); 
        }
        WhereClouse.push({ "fieldName": "emp_id__c", "fieldValue": emp_id__c});  
        WhereClouse.push({ "fieldName": "grid__c", "fieldValue": grid});
        let visit_sql = qry.SelectAllQry(fields, tableName,WhereClouse, offset, limit,' order by createddate desc');
        console.log("visit_sql ------> ", visit_sql);
        let visit_sql_res = await client.query(visit_sql);

        if(visit_sql_res.rows.length > 0){
            visit_sql_res.rows.map((sfids)=> {
                grid_id_arr.push(sfids['name'])  
                let obj = {
                    grid__c: sfids['grid__c'],
                    name:sfids['name'],
                    visit_date__c:sfids['visit_date__c'],
                    sfid:sfids['sfid'],

                };
                grid_data.push(obj); 
            });

            return [grid_data];
        }

    }catch(e){
        console.log('Error In Team Date Type Wise Function ------>',e);
    }
}
async function mailErrorLog(data,url,body_params,query_params,header_params,header_params2){
    let env1=process.env.ENVIRONMENT_NAME;
    if(!data){
        console.log('If Part');
        return 'No Data';
    }else{      
        if(!process.argv[2] ){
        let subject = `Error log for CenturyPly(${env1}) in API  ${url}`
        let text = ` API Name :  ` + url + '\n' + '\n' + `Request Body : ` + JSON.stringify(body_params) + '\n' + '\n' + `Query Params : ` + JSON.stringify(query_params) + '\n' + '\n' + `MESSAGE : ` + data + '\n' + '\n' + "Header Token --->" + header_params + '\n' + '\n' + "Header CLIENT-NAME --->" + header_params2 +"\n\nENVIRONMENT NAME--->"+process.env.ENVIRONMENT_NAME
        email.email_error_log(subject, text)
        }
    }
}

async function leadDropMailAlert(data,drop_reason,product_rate,brand){
    if(!data){
        console.log('If Part');
        return 'No Data';
    }else{      
        let subject = drop_reason;
        let text = `Drop Reason -> ${drop_reason} , Product Rate -> ${product_rate} , Brand -> ${brand}`
        let id = data;
        console.log(`Id ----> ${id}`);
        email.email_lead_drop_log(subject, text ,id)
    }
}

async function getRegionWiseData(data){
    try{

        let area_level_sql = `SELECT sfid FROM salesforce.picklist__c where field_name__c = 'Area_Level__c' order by name desc`
        let area_level_res = await client.query(area_level_sql)

        let level_9 = area_level_res.rows[0]['sfid']
        let level_8 = area_level_res.rows[1]['sfid']
        let level_7= area_level_res.rows[2]['sfid']
        let level_6= area_level_res.rows[3]['sfid']
        let level_5= area_level_res.rows[4]['sfid']
        let level_4= area_level_res.rows[5]['sfid']
        let level_3= area_level_res.rows[6]['sfid']
        let level_2= area_level_res.rows[7]['sfid']
        let level_1= area_level_res.rows[8]['sfid']

        let territory_type_sql = `SELECT sfid FROM salesforce.picklist__c where field_name__c = 'Territory_Type__c' order by name desc`
        let territory_type_res = await client.query(territory_type_sql)

        let primary_territory = territory_type_res.rows[1]['sfid']
        let secondary_territory = territory_type_res.rows[0]['sfid']


        let zone_sfid_arr = [];
        let zone_data = [];
        let region_sfid_arr = [];
        let region_data = [];
        let branch_sfid_arr = [];
        let branch_data = [];
        let territory_sfid_arr = [];
        let territory_data = [];
        let asm_data = [];
        let asm_sfid_arr = [];
        let town_data = [];
        let town_sfid_arr = [];
        let beat_data = [];
        let beat_sfid_arr = [];
        let grid_data = [];
        let grid_sfid_arr = [];
        let se_data = [];
        let se_sfid_arr = [];

        let fields = [`${SF_AREA_1_TABLE_NAME}.*, a1.name__c as parent_code_name`]
        let tableName = SF_AREA_1_TABLE_NAME;
        var offset='0', limit='100';
        let WhereClouse = [];
        //WhereClouse.push({ "fieldName": "town_code__c", "fieldValue": grid_sql_res.rows});  
        WhereClouse.push({ "fieldName": "area1__c.name__c", "fieldValue": data});  
        WhereClouse.push({ "fieldName": "area1__c.area_level__c", "fieldValue": level_2});
//*********************************************** Zone Starts Here *********************************************************/
        let joins = [
            {
                "type": "LEFT",
                "table_name": `${SF_AREA_1_TABLE_NAME} as a1`,
                "p_table_field": `${SF_AREA_1_TABLE_NAME}.parent_code__c`,
                "s_table_field": `a1.sfid `
            }
        ]

        let zone_sql = qry.fetchAllWithJoinQry(fields, tableName, joins, WhereClouse, offset, limit,' order by createddate desc');
        //console.log("Zone sql ------> ", zone_sql);
        let zone_sql_res = await client.query(zone_sql);

        if(zone_sql_res.rows.length > 0){
            zone_sql_res.rows.map((sfids)=> {
                zone_sfid_arr.push(sfids['sfid'])  
                let obj = {
                    zone_id: sfids['sfid'],
                    zone_name: sfids['name__c'],
                    zone_parent_code: sfids['parent_code__c'],
                    zone_parent_code_name: sfids['parent_code_name'],
                    zone_code :sfids['name']
                };
                zone_data.push(obj);              
            });
        }else{
            zone_sfid_arr.push(data);
            zone_sfid_arr = await sort.removeDuplicates(zone_sfid_arr);
            console.log('inside if condition zone idss ----->',zone_sfid_arr);
        }
//*****************************************************  Zone Ends Here   *******************************************************/
        if(zone_sql_res.rows.length > 0 || (data.length > 0 && data != undefined)){
            
            let fields = [`${SF_AREA_1_TABLE_NAME}.*, a2.name__c as parent_code_name`]
            let tableName = SF_AREA_1_TABLE_NAME;
            var offset='0', limit='100';
            let WhereClouse = [];
            //WhereClouse.push({ "fieldName": "town_code__c", "fieldValue": grid_sql_res.rows});  
            WhereClouse.push({ "fieldName": "area1__c.parent_code__c", "fieldValue": zone_sfid_arr ,"type": 'IN' });  
            WhereClouse.push({ "fieldName": "area1__c.area_level__c", "fieldValue": level_3});
            
            let joins = [
                {
                    "type": "LEFT",
                    "table_name": `${SF_AREA_1_TABLE_NAME} as a2`,
                    "p_table_field": `${SF_AREA_1_TABLE_NAME}.parent_code__c`,
                    "s_table_field": `a2.sfid `
                }
            ]

            let region_sql = qry.fetchAllWithJoinQry(fields, tableName, joins, WhereClouse, offset, limit,' order by createddate desc');
            //console.log("Region sql ------> ", region_sql);
            let region_sql_res = await client.query(region_sql);

            if(region_sql_res.rows.length > 0){
                region_sql_res.rows.map((sfids)=> {
                    region_sfid_arr.push(sfids['sfid'])   
                    let obj = {
                        region_id: sfids['sfid'],
                        region_name: sfids['name__c'],
                        region_parent_code: sfids['parent_code__c'],
                        region_parent_code_name : sfids['parent_code_name'],
                        region_code : sfids['name']
                    };
                    region_data.push(obj);              
                });
            }else{
                region_sfid_arr.push(data);
                region_sfid_arr = await sort.removeDuplicates(region_sfid_arr);
                console.log('inside if condition region idss ----->',region_sfid_arr);
            }
//******************************************* Region Ends Here  **************************************************/
            if(region_sql_res.rows.length > 0 || (data.length > 0 && data != undefined)){

                let fields = [`${SF_AREA_1_TABLE_NAME}.*, a3.name__c as parent_code_name`]
                let tableName = SF_AREA_1_TABLE_NAME;
                var offset='0', limit='100';
                let WhereClouse = [];
                //WhereClouse.push({ "fieldName": "town_code__c", "fieldValue": grid_sql_res.rows});  
                WhereClouse.push({ "fieldName": "area1__c.parent_code__c", "fieldValue": region_sfid_arr ,"type": 'IN' });  
                WhereClouse.push({ "fieldName": "area1__c.area_level__c", "fieldValue": level_4});

                let joins = [
                    {
                        "type": "LEFT",
                        "table_name": `${SF_AREA_1_TABLE_NAME} as a3`,
                        "p_table_field": `${SF_AREA_1_TABLE_NAME}.parent_code__c`,
                        "s_table_field": `a3.sfid `
                    }
                ]

                let branch_sql = qry.fetchAllWithJoinQry(fields, tableName, joins, WhereClouse, offset, limit,' order by createddate desc');
                //console.log("Branch sql ------> ", branch_sql);
                let branch_sql_res = await client.query(branch_sql);

                if(branch_sql_res.rows.length > 0){
                    branch_sql_res.rows.map((sfids)=> {
                        branch_sfid_arr.push(sfids['sfid'])  
                        let obj = {
                            branch_id: sfids['sfid'],
                            branch_name: sfids['name__c'],
                            branch_parent_code: sfids['parent_code__c'],
                            branch_parent_code_name: sfids['parent_code_name'],
                            branch_code :sfids['name']
                        };
                        branch_data.push(obj);              
                    });
                }else{
                    branch_sfid_arr.push(data);
                    branch_sfid_arr = await sort.removeDuplicates(branch_sfid_arr);
                    console.log('inside if condition branch idss ----->',branch_sfid_arr);
                }
//********************************** Branch Ends Here ****************************************************/
                if(branch_sql_res.rows.length > 0 || (data.length > 0 && data != undefined)){
                    
                    let fields = [`${SF_AREA_1_TABLE_NAME}.*, ${SF_PICKLIST_TABLE_NAME}.name as area_type_name, a4.name__c as parent_code_name`]
                    let tableName = SF_AREA_1_TABLE_NAME;
                    var offset='0', limit='100';
                    let WhereClouse = [];
                    WhereClouse.push({ "fieldName": "area1__c.parent_code__c", "fieldValue": branch_sfid_arr ,"type": 'IN' });  
                    WhereClouse.push({ "fieldName": "area1__c.area_level__c", "fieldValue": level_5});

                    let joins = [
                        {
                            "type": "LEFT",
                            "table_name": `${SF_PICKLIST_TABLE_NAME}`,
                            "p_table_field": `${SF_AREA_1_TABLE_NAME}.area_type__c`,
                            "s_table_field": `${SF_PICKLIST_TABLE_NAME}.sfid `
                        },
                        {
                            "type": "LEFT",
                            "table_name": `${SF_AREA_1_TABLE_NAME} as a4`,
                            "p_table_field": `${SF_AREA_1_TABLE_NAME}.parent_code__c`,
                            "s_table_field": `a4.sfid `
                        }
                    ]

                    let asm_sql = qry.fetchAllWithJoinQry(fields, tableName, joins, WhereClouse, offset, limit,' order by createddate desc');
                    //console.log("ASM sql ------> ", asm_sql);
                    let asm_sql_res = await client.query(asm_sql);

                    if(asm_sql_res.rows.length > 0){
                        asm_sql_res.rows.map((sfids)=> {
                            asm_sfid_arr.push(sfids['sfid'])  
                            let obj = {
                                asm_id: sfids['sfid'],
                                asm_name: sfids['name__c'],
                                asm_parent_code: sfids['parent_code__c'],
                                asm_parent_code_name: sfids['parent_code_name'],
                                area_type : sfids['area_type__c'],
                                area_type_name : sfids['area_type_name'],
                                area_code: sfids['name']
                            };
                            asm_data.push(obj);              
                        });
                    }else{
                        asm_sfid_arr.push(data);
                        asm_sfid_arr = await sort.removeDuplicates(asm_sfid_arr);
                        console.log('inside if condition asm idss ----->',asm_sfid_arr);
                    }
 //*********************************** Asm ENds Here ***************************************************************/ 
                    if(asm_sql_res.rows.length > 0 || (data.length > 0 && data != undefined)){
                        let fields = [`${SF_AREA_1_TABLE_NAME}.*, ${SF_PICKLIST_TABLE_NAME}.name as area_type_name, a5.name__c as parent_code_name`]
                        let tableName = SF_AREA_1_TABLE_NAME;
                        var offset='0', limit='100';
                        let WhereClouse = [];
                        WhereClouse.push({ "fieldName": "area1__c.parent_code__c", "fieldValue": asm_sfid_arr ,"type": 'IN' });  
                        //WhereClouse.push({ "fieldName": "area1__c.territory_type__c", "fieldValue": 'a050w000002jNn6AAE'});

                        let joins = [
                            {
                                "type": "LEFT",
                                "table_name": `${SF_PICKLIST_TABLE_NAME}`,
                                "p_table_field": `${SF_AREA_1_TABLE_NAME}.area_type__c`,
                                "s_table_field": `${SF_PICKLIST_TABLE_NAME}.sfid `
                            },
                            {
                                "type": "LEFT",
                                "table_name": `${SF_AREA_1_TABLE_NAME} as a5`,
                                "p_table_field": `${SF_AREA_1_TABLE_NAME}.parent_code__c`,
                                "s_table_field": `a5.sfid `
                            }
                        ]

                        let se_sql = qry.fetchAllWithJoinQry(fields, tableName, joins, WhereClouse, offset, limit,' order by createddate desc');
                        //console.log("SE sql ------> ", se_sql);
                        let se_sql_res = await client.query(se_sql);

                        if(se_sql_res.rows.length > 0){
                            se_sql_res.rows.map((sfids)=> {
                                se_sfid_arr.push(sfids['sfid'])  
                                let obj = {
                                    se_id: sfids['sfid'],
                                    se_name: sfids['name__c'],
                                    se_parent_code: sfids['parent_code__c'],
                                    se_parent_code_name: sfids['parent_code_name'],
                                    se_code: sfids['name'],
                                    se_type : sfids['area_type__c'],
                                    se_type_name : sfids['territory_type_name']
                                };
                                se_data.push(obj);              
                            });
                        }else{
                            se_sfid_arr.push(data);
                            se_sfid_arr = await sort.removeDuplicates(se_sfid_arr);
                            console.log('inside if condition asm idss ----->',se_sfid_arr);
                        }

 //*********************************** SE Ends Here ***************************************************************/                   
                    if(asm_sql_res.rows.length > 0 || (data.length > 0 && data != undefined)){
                        let fields = [`${SF_AREA_1_TABLE_NAME}.*, ${SF_PICKLIST_TABLE_NAME}.name as area_type_name, a5.name__c as parent_code_name , p1.name as territory_type_name`]
                        let tableName = SF_AREA_1_TABLE_NAME;
                        var offset='0', limit='100';
                        let WhereClouse = [];
                        WhereClouse.push({ "fieldName": "area1__c.parent_code__c", "fieldValue": se_sfid_arr ,"type": 'IN' });  
                        WhereClouse.push({ "fieldName": "area1__c.territory_type__c", "fieldValue": secondary_territory});

                        let joins = [
                            {
                                "type": "LEFT",
                                "table_name": `${SF_PICKLIST_TABLE_NAME}`,
                                "p_table_field": `${SF_AREA_1_TABLE_NAME}.area_type__c`,
                                "s_table_field": `${SF_PICKLIST_TABLE_NAME}.sfid `
                            },
                            {
                                "type": "LEFT",
                                "table_name": `${SF_AREA_1_TABLE_NAME} as a5`,
                                "p_table_field": `${SF_AREA_1_TABLE_NAME}.parent_code__c`,
                                "s_table_field": `a5.sfid `
                            },
                            {
                                "type": "LEFT",
                                "table_name": `${SF_PICKLIST_TABLE_NAME} as p1`,
                                "p_table_field": `${SF_AREA_1_TABLE_NAME}.territory_type__c`,
                                "s_table_field": `p1.sfid `
                            },
                        ]

                        let territory_sql = qry.fetchAllWithJoinQry(fields, tableName, joins, WhereClouse, offset, limit,' order by createddate desc');
                        //console.log("Territory sql ------> ", territory_sql);
                        let territory_sql_res = await client.query(territory_sql);

                        if(territory_sql_res.rows.length > 0){
                            territory_sql_res.rows.map((sfids)=> {
                                territory_sfid_arr.push(sfids['sfid'])  
                                let obj = {
                                    territory_id: sfids['sfid'],
                                    territory_name: sfids['name__c'],
                                    territory_parent_code: sfids['parent_code__c'],
                                    territory_parent_code_name: sfids['parent_code_name'],
                                    territory_code: sfids['name'],
                                    area_type : sfids['area_type__c'],
                                    area_type_name : sfids['area_type_name'],
                                    territory_type : sfids['territory_type__c'],
                                    territory_type_name : sfids['territory_type_name'],
                                };
                                territory_data.push(obj);              
                            });
                        }else{
                            territory_sfid_arr.push(data);
                            territory_sfid_arr = await sort.removeDuplicates(territory_sfid_arr);
                            console.log('inside if condition asm idss ----->',territory_sfid_arr);
                        }
//********************************** Territory Ends Here *****************************************************/
                        if(territory_sql_res.rows.length > 0 || (data.length > 0 && data != undefined)){

                            let fields = [`${SF_AREA_1_TABLE_NAME}.*, ${SF_PICKLIST_TABLE_NAME}.name as area_type_name, a5.name__c as parent_code_name,${SF_GEOGRAPHICAL_TOWN_TABLE_NAME}.name as geographical_town_name__c`]
                            let tableName = SF_AREA_1_TABLE_NAME;
                            var offset='0', limit='100';
                            let WhereClouse = [];
                            WhereClouse.push({ "fieldName": "area1__c.parent_code__c", "fieldValue": territory_sfid_arr ,"type": 'IN' });  
                            //WhereClouse.push({ "fieldName": "area1__c.territory_type__c", "fieldValue": 'a050w000002jNn6AAE'});
    
                            let joins = [
                                {
                                    "type": "LEFT",
                                    "table_name": `${SF_PICKLIST_TABLE_NAME}`,
                                    "p_table_field": `${SF_AREA_1_TABLE_NAME}.area_type__c`,
                                    "s_table_field": `${SF_PICKLIST_TABLE_NAME}.sfid `
                                },
                                {
                                    "type": "LEFT",
                                    "table_name": `${SF_AREA_1_TABLE_NAME} as a5`,
                                    "p_table_field": `${SF_AREA_1_TABLE_NAME}.parent_code__c`,
                                    "s_table_field": `a5.sfid `
                                },
                                {
                                    "type": "LEFT",
                                    "table_name": `${SF_GEOGRAPHICAL_TOWN_TABLE_NAME}`,
                                    "p_table_field": `${SF_AREA_1_TABLE_NAME}.geographical_town__c`,
                                    "s_table_field": `${SF_GEOGRAPHICAL_TOWN_TABLE_NAME}.sfid `
                                },
                            ]
    
                            let town_sql = qry.fetchAllWithJoinQry(fields, tableName, joins, WhereClouse, offset, limit,' order by createddate desc');
                            //console.log("Town sql ------> ", town_sql);
                            let town_sql_res = await client.query(town_sql);
                            //console.log(town_sql_res.rows)

                            //let territory_town_data = await territoryWiseTown(territory_sfid_arr);
                            // console.log('**********IMP DATA*********',territory_town_data);
                            if(town_sql_res.rows.length > 0){
                                town_sql_res.rows.map((sfids)=> {
                                    town_sfid_arr.push(sfids['sfid'])  
                                    let obj = {
                                        town_id: sfids['sfid'], ///changed  used or not
                                        town_name: sfids['name__c'],  ///changed
                                        geographical_town_name: sfids['geographical_town_name__c'], //changed
                                        geographical_town : sfids['geographical_town__c'], //changed
                                        town_code : sfids['town_code__c'], //need to pass id
                                        town_parent_code : sfids['parent_code__c'], ///changed
                                        town_code__c : sfids['name'] ///changed
                                    };
                                    town_data.push(obj);              
                                });
                            }else{
                                town_sfid_arr.push(data);
                                town_sfid_arr = await sort.removeDuplicates(town_sfid_arr);
                                console.log('inside if condition town idss ----->',town_sfid_arr);
                            }
//********************************************** Beat And Grid Logic ********************************************************************/
                            if(town_sql_res.rows.length > 0 || (data.length > 0 && data != undefined)){

                                let town_wise_beats = await townWiseBeatGrid(town_sfid_arr);
                                let town_wise_grid = await townWiseGrid(town_sfid_arr);
                                //console.log('**********IMP DATA*********',town_wise_grid[0]);
                                if(town_wise_beats.length > 0){
                                    town_wise_beats.map((sfids)=> {
                                        beat_sfid_arr.push(sfids['sfid'])  
                                        let obj = {
                                            beat_id: sfids['sfid'],
                                            beat_name: sfids['name'],
                                            geographical_town_name: sfids['name__c'],
                                            geographical_town : sfids['geographical_town__c'], 
                                            beat_parent_code : sfids['parent_code__c'],
                                            grid_coordinates : sfids['grid_cordinates__c'],
                                            perimeter : sfids['perimeter__c'],
                                            border_width : sfids['border_width__c'],
                                            grid_area : sfids['grid_area__c'],
                                            grid_frequency : sfids['grid_frequency__c'],
                                            no_of_days_to_complete_grid : sfids['no_of_days_to_complete_grid__c'],

                                        };
                                        beat_data.push(obj);              
                                    });
                                }
                                if(town_wise_grid.length > 0){
                                    town_wise_grid.map((sfids)=> {
                                        grid_sfid_arr.push(sfids['sfid'])  
                                        let obj = {
                                            grid_id: sfids['sfid'],
                                            grid_name: sfids['name__c'],
                                            geographical_town_name: sfids['geographical_name'],
                                            geographical_town : sfids['geographical_town__c'], 
                                            grid_parent_code : sfids['parent_code__c'],
                                            grid_coordinates : sfids['grid_cordinates__c'],
                                            perimeter : sfids['perimeter__c'],
                                            border_width : sfids['border_width__c'],
                                            grid_area : sfids['grid_area__c'],
                                            grid_frequency : sfids['grid_frequency__c'],
                                            no_of_days_to_complete_grid : sfids['no_of_days_to_complete_grid__c'],
                                            grid_potential : sfids['grid_potential__c'],


                                        };
                                        grid_data.push(obj);              
                                    });
                                }
                                
                                return [zone_sfid_arr,zone_data,region_sfid_arr,region_data,branch_sfid_arr,branch_data,territory_sfid_arr,territory_data,asm_sfid_arr,asm_data,town_sfid_arr,town_data,beat_data,beat_sfid_arr,grid_data,grid_sfid_arr,(territory_sql_res.rows.map((team) => team['sfid']))];
                            }else{
                                return [zone_sfid_arr,zone_data,region_sfid_arr,region_data,branch_sfid_arr,branch_data,territory_sfid_arr,territory_data,asm_sfid_arr,asm_data,town_sfid_arr,town_data,beat_data,beat_sfid_arr,grid_data,grid_sfid_arr,(territory_sql_res.rows.map((team) => team['sfid']))];
                            }                           
                        }else{
                            return [zone_sfid_arr,zone_data,region_sfid_arr,region_data,branch_sfid_arr,branch_data,territory_sfid_arr,territory_data,asm_sfid_arr,asm_data,town_sfid_arr,town_data,beat_data,beat_sfid_arr,grid_data,grid_sfid_arr,(territory_sql_res.rows.map((team) => team['sfid']))];                       
                        }
                    }else{
                        return [zone_sfid_arr,zone_data,region_sfid_arr,region_data,branch_sfid_arr,branch_data,territory_sfid_arr,territory_data,asm_sfid_arr,asm_data,town_sfid_arr,town_data,beat_data,beat_sfid_arr,grid_data,grid_sfid_arr,(territory_sql_res.rows.map((team) => team['sfid']))];                    
                    }
                    }else{
                        return [zone_sfid_arr,zone_data,region_sfid_arr,region_data,branch_sfid_arr,branch_data,territory_sfid_arr,territory_data,asm_sfid_arr,asm_data,town_sfid_arr,town_data,beat_data,beat_sfid_arr,grid_data,grid_sfid_arr,(territory_sql_res.rows.map((team) => team['sfid']))];                    
                    }
                }else{
                    return [zone_sfid_arr,zone_data,region_sfid_arr,region_data,branch_sfid_arr,branch_data,territory_sfid_arr,territory_data,asm_sfid_arr,asm_data,town_sfid_arr,town_data,beat_data,beat_sfid_arr,grid_data,grid_sfid_arr,(territory_sql_res.rows.map((team) => team['sfid']))];               
                }
            }else{
                return [zone_sfid_arr,zone_data,region_sfid_arr,region_data,branch_sfid_arr,branch_data,territory_sfid_arr,territory_data,asm_sfid_arr,asm_data,town_sfid_arr,town_data,beat_data,beat_sfid_arr,grid_data,grid_sfid_arr,(territory_sql_res.rows.map((team) => team['sfid']))];            
            }
        }else{
            return [zone_sfid_arr,zone_data,region_sfid_arr,region_data,branch_sfid_arr,branch_data,territory_sfid_arr,territory_data,asm_sfid_arr,asm_data,town_sfid_arr,town_data,beat_data,beat_sfid_arr,grid_data,grid_sfid_arr,(territory_sql_res.rows.map((team) => team['sfid']))];        
        }
    }catch(e){
        console.log('Error in Get Region Wise Data Function ----->',e);
    }
}

async function townWiseBeatGrid(town_id_arr,area_type){
    try{
        let fields = [`${SF_AREA_1_TABLE_NAME}.*,
        ${SF_GEOGRAPHICAL_TOWN_TABLE_NAME}.town_name__c as geographical_name,
        a1.name__c as parent_name, pl.name as area_type_name,
        pk2.name as grid_potential_name,
        pk2.picklist_detail2__c as grid_potential_color,
        pk2.picklistpicklist1__c as grid_potential_code
        `
    ]
        let tableName = SF_AREA_1_TABLE_NAME;
        var offset='0', limit='1000';
        let WhereClouse = [];
        if(area_type != undefined && area_type.length > 0){
            WhereClouse.push({ "fieldName":`pl.name`, "fieldValue":area_type}); 
        }
        WhereClouse.push({ "fieldName": `${SF_AREA_1_TABLE_NAME}.parent_code__c`, "fieldValue": town_id_arr ,"type": 'IN'}); 
        // WhereClouse.push({ "fieldName": `${SF_AREA_2_TABLE_NAME}.area_type__c`, "fieldValue": 'a050w000003QLJHAA4' });
        WhereClouse.push({ "fieldName": `${SF_AREA_1_TABLE_NAME}.sfid`, "type": 'NOTNULL' });
        // WhereClouse.push({ "fieldName": `${SF_GEOGRAPHICAL_TOWN_TABLE_NAME}.sfid`, "type": 'NOTNULL' });



        let joins = [
            {
                "type": "LEFT",
                "table_name": `${SF_GEOGRAPHICAL_TOWN_TABLE_NAME}`,
                "p_table_field": `${SF_AREA_1_TABLE_NAME}.geographical_town__c`,
                "s_table_field": `${SF_GEOGRAPHICAL_TOWN_TABLE_NAME}.sfid `
            },
            {
                "type": "LEFT",
                "table_name": `${SF_AREA_1_TABLE_NAME} as a1`,
                "p_table_field": `${SF_AREA_1_TABLE_NAME}.parent_code__c`,
                "s_table_field": `a1.sfid `
            },
            {
                "type": "LEFT",
                "table_name": `${SF_PICKLIST_TABLE_NAME} as pl`,
                "p_table_field": `${SF_AREA_1_TABLE_NAME}.area_type__c`,
                "s_table_field": `pl.sfid `
            },
            {
                "type": "LEFT",
                "table_name": `${SF_PICKLIST_TABLE_NAME} as pk2`,
                "p_table_field": `${SF_AREA_1_TABLE_NAME}.grid_potential__c`,
                "s_table_field": `pk2.sfid`
            }
        ]

        let beat_grid_from_town_sql = qry.fetchAllWithJoinQry(fields, tableName, joins, WhereClouse,offset,limit,`order by ${SF_AREA_1_TABLE_NAME}.createddate desc`);
        //let beat_grid_from_town_sql = qry.SelectAllQry(fields, tableName,WhereClouse, offset, limit,' order by createddate desc');
        console.log("beats and grids from town sql ----> ", beat_grid_from_town_sql);
        let beat_grid_from_town_sql_res = await client.query(beat_grid_from_town_sql);

        let visit_completed_status_id = await getPicklistSfid('Visit__c','Visit_Status__c','Completed')

        for (let i = 0; i < beat_grid_from_town_sql_res.rows.length; i++) {
            beat_grid_from_town_sql_res.rows[i]['grid_last_visit_date__c'] = dtUtil.ISOtoLocal(beat_grid_from_town_sql_res.rows[i]['grid_last_visit_date__c']);
    
            ////// order by last visited date which is in descending order /////
            const fields = ['visit_date__c'];
            const tableName = SF_VISIT_TABLE_NAME;
            let offset = '0', limit = '1';
            let orderBy = 'ORDER BY visit_date__c desc'

            const WhereClouse = [];

            // WhereClouse.push({fieldValue: req.query.team__c, fieldName: 'team_id__c'});
            WhereClouse.push({fieldValue: visit_completed_status_id, fieldName: 'visit_status__c'});
            WhereClouse.push({ fieldValue: beat_grid_from_town_sql_res.rows[i]['sfid'], fieldName: 'grid__c' });

            let visit_sql1 = qry.SelectAllQry(fields, tableName, WhereClouse, offset, limit, orderBy);
            console.log('visit sql for date ------->', visit_sql1);

            let visit_result = await client.query(visit_sql1);
            // console.log('test for visit date --->',visit_result.rows);
            // console.log('last visit date ---> ', dtUtil.ISOtoLocal(visit_result.rows[0].visit_date__c));
            if (visit_result.rows.length > 0) {
                beat_grid_from_town_sql_res.rows[i]['last_visit_date'] = dtUtil.ISOtoLocal(visit_result.rows[0]['visit_date__c'])
            }
            else{
                beat_grid_from_town_sql_res.rows[i]['last_visit_date'] = null
            }
        }

        if(beat_grid_from_town_sql_res.rows.length > 0){

            if(area_type == 'Beat'){
                for(let i = 0 ; i < beat_grid_from_town_sql_res.rows.length ; i++){
                    let account_coordinate_sql = `SELECT geo_location__latitude__s,geo_location__longitude__s FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_ACCOUNT_TABLE_NAME} where town__c = '${beat_grid_from_town_sql_res.rows[i]['parent_code__c']}'`
                    let result = await client.query(account_coordinate_sql);

                    if(result.rows.length > 0){
                        beat_grid_from_town_sql_res.rows[i]['account_info'] = result.rows

                    }else{
                        beat_grid_from_town_sql_res.rows[i]['account_info'] = []
                    }
                }
            }
            return beat_grid_from_town_sql_res.rows
        }else{
            return [];
        }
    }catch(e){
        console.log('Error in Town Wise Beat/Grid Function --->',e);
    }
}

async function townWiseBeatGridV2(town_id_arr,area_type,from_date,to_date){
    try{
        let grid_data =[];
        let fields = [`${SF_AREA_1_TABLE_NAME}.*,
        ${SF_GEOGRAPHICAL_TOWN_TABLE_NAME}.town_name__c as geographical_name,
        a1.name__c as parent_name, pl.name as area_type_name,
        pk2.name as grid_potential_name,
        pk2.picklist_detail2__c as grid_potential_color,
        pk2.picklistpicklist1__c as grid_potential_code
        `
    ]
        let tableName = SF_AREA_1_TABLE_NAME;
        var offset='0', limit='1000';
        let WhereClouse = [];
        if(area_type != undefined && area_type.length > 0){
            WhereClouse.push({ "fieldName":`pl.name`, "fieldValue":area_type}); 
        }
        WhereClouse.push({ "fieldName": `${SF_AREA_1_TABLE_NAME}.parent_code__c`, "fieldValue": town_id_arr ,"type": 'IN'}); 
        // WhereClouse.push({ "fieldName": `${SF_AREA_2_TABLE_NAME}.area_type__c`, "fieldValue": 'a050w000003QLJHAA4' });
        WhereClouse.push({ "fieldName": `${SF_AREA_1_TABLE_NAME}.sfid`, "type": 'NOTNULL' });
        // WhereClouse.push({ "fieldName": `${SF_GEOGRAPHICAL_TOWN_TABLE_NAME}.sfid`, "type": 'NOTNULL' });



        let joins = [
            {
                "type": "LEFT",
                "table_name": `${SF_GEOGRAPHICAL_TOWN_TABLE_NAME}`,
                "p_table_field": `${SF_AREA_1_TABLE_NAME}.geographical_town__c`,
                "s_table_field": `${SF_GEOGRAPHICAL_TOWN_TABLE_NAME}.sfid `
            },
            {
                "type": "LEFT",
                "table_name": `${SF_AREA_1_TABLE_NAME} as a1`,
                "p_table_field": `${SF_AREA_1_TABLE_NAME}.parent_code__c`,
                "s_table_field": `a1.sfid `
            },
            {
                "type": "LEFT",
                "table_name": `${SF_PICKLIST_TABLE_NAME} as pl`,
                "p_table_field": `${SF_AREA_1_TABLE_NAME}.area_type__c`,
                "s_table_field": `pl.sfid `
            },
            {
                "type": "LEFT",
                "table_name": `${SF_PICKLIST_TABLE_NAME} as pk2`,
                "p_table_field": `${SF_AREA_1_TABLE_NAME}.grid_potential__c`,
                "s_table_field": `pk2.sfid`
            }
        ]

        let beat_grid_from_town_sql = qry.fetchAllWithJoinQry(fields, tableName, joins, WhereClouse,offset,limit,`order by ${SF_AREA_1_TABLE_NAME}.createddate desc`);
        //let beat_grid_from_town_sql = qry.SelectAllQry(fields, tableName,WhereClouse, offset, limit,' order by createddate desc');
        console.log("beats and grids from town sql ----> ", beat_grid_from_town_sql);
        let beat_grid_from_town_sql_res = await client.query(beat_grid_from_town_sql);
        let visit_completed_status_id = await getPicklistSfid('Visit__c','Visit_Status__c','Completed')

        for (let i = 0; i < beat_grid_from_town_sql_res.rows.length; i++) {
            beat_grid_from_town_sql_res.rows[i]['grid_last_visit_date__c'] = dtUtil.ISOtoLocal(beat_grid_from_town_sql_res.rows[i]['grid_last_visit_date__c']);
    
            ////// order by last visited date which is in descending order /////
            const fields = ['*'];
            const tableName = SF_VISIT_TABLE_NAME;
            let offset = '0', limit = '100';
            let orderBy = 'ORDER BY visit_date__c desc'

            const WhereClouse = [];

            // WhereClouse.push({fieldValue: req.query.team__c, fieldName: 'team_id__c'});
            WhereClouse.push({fieldValue: visit_completed_status_id, fieldName: 'visit_status__c'});
            WhereClouse.push({ fieldValue: beat_grid_from_town_sql_res.rows[i]['sfid'], fieldName: 'grid__c' });

            let visit_sql1 = qry.SelectAllQry(fields, tableName, WhereClouse, offset, limit, orderBy);
            // console.log('visit sql for date ------->', visit_sql1);

            let visit_result = await client.query(visit_sql1);
            // console.log('test for visit date --->',visit_result.rows);
            // console.log('last visit date ---> ', dtUtil.ISOtoLocal(visit_result.rows[0].visit_date__c));
            if (visit_result.rows.length > 0) {
                beat_grid_from_town_sql_res.rows[i]['last_visit_date'] = dtUtil.ISOtoLocal(visit_result.rows[0]['visit_date__c'])
            }
            else{
                beat_grid_from_town_sql_res.rows[i]['last_visit_date'] = null
            }

            // @DOUBT - if a grid have 2 grid_working_status as ongoing then both the record will come out or single latest  
            const fields_grid_work = [`${SF_GRID_WORKING_TABLE_NAME}.*,ar_town.name__c as town_name, ar_grid.name__c as grid_name, ${SF_PICKLIST_TABLE_NAME}.name as grid_working_status_name`];
            const tableName_grid_work = SF_GRID_WORKING_TABLE_NAME;
            const WhereClouse_grid_work = [];
            let d = new Date();
            let current_month = d.getMonth() + 1;
            let prev_month = d.getMonth();
            // console.log("current month an prev month",current_month,prev_month,d)
            //console.log('PGID -->', i, '---VALUE--', expected_order_sql_res.rows[i]['sfid']);
            if (beat_grid_from_town_sql_res.rows[i]['sfid']) {
                WhereClouse_grid_work.push({ 'fieldName': `${SF_GRID_WORKING_TABLE_NAME}.grid__c`, 'fieldValue': beat_grid_from_town_sql_res.rows[i]['sfid'] });
            }
            if(from_date && to_date){
                WhereClouse_grid_work.push({ "fieldName":  `${SF_GRID_WORKING_TABLE_NAME}.planned_date__c`, "fieldValue": from_date, type:'GTE'});
                WhereClouse_grid_work.push({ "fieldName": `${SF_GRID_WORKING_TABLE_NAME}.planned_date__c`, "fieldValue": to_date, type:'LTE'});
            }
            else{
                WhereClouse_grid_work.push({ 'fieldName': `EXTRACT(MONTH FROM planned_date__c)`, 'fieldValue': current_month , type : 'LTE' });
                WhereClouse_grid_work.push({ 'fieldName': `EXTRACT(MONTH FROM planned_date__c)`, 'fieldValue': prev_month , type : 'GTE' });

            }
            //WhereClouse.push({ 'fieldName': `${SF_SUPPLY_TABLE_NAME}.expected_order_pgid__c`, 'type': "NOTNULL" });
            let offset_grid = '0', limit_grid = '1000';

            let grid_joins = [
                {
                    "type": "LEFT",
                    "table_name": `${SF_AREA_1_TABLE_NAME} as ar_town`,
                    "p_table_field": `${SF_GRID_WORKING_TABLE_NAME}.town__c`,
                    "s_table_field": `ar_town.sfid `
                },
                {
                    "type": "LEFT",
                    "table_name": `${SF_AREA_1_TABLE_NAME} as ar_grid`,
                    "p_table_field": `${SF_GRID_WORKING_TABLE_NAME}.grid__c`,
                    "s_table_field": `ar_grid.sfid `
                },
                {
                    "type": "LEFT",
                    "table_name": `${SF_PICKLIST_TABLE_NAME}`,
                    "p_table_field": `${SF_GRID_WORKING_TABLE_NAME}.grid_working_status__c`,
                    "s_table_field": `${SF_PICKLIST_TABLE_NAME}.sfid `
                }
            ]


            let working_hour_sql = qry.fetchAllWithJoinQry(fields_grid_work, tableName_grid_work, grid_joins, WhereClouse_grid_work, offset_grid, limit_grid, ` order by ${SF_GRID_WORKING_TABLE_NAME}.createddate desc`);
            console.log(`working grid SQL ----> ${working_hour_sql}`)
            // let orderLine_sql = db.SelectAllQry(fields, tableName,orderLine_WhereClouse, offset, limit,' order by createddate desc');
            let working_hour_sql_res = await client.query(working_hour_sql);

            if (beat_grid_from_town_sql_res.rows[i]['no_of_days_to_complete_grid__c'] != null) {
                let update_no_of_days = `update salesforce.${SF_GRID_WORKING_TABLE_NAME} set no_of_days_for_the_completion__c = ${beat_grid_from_town_sql_res.rows[i]['no_of_days_to_complete_grid__c']} where grid__c = '${beat_grid_from_town_sql_res.rows[i]['sfid']}'`
                let update_no_of_days_res = await client.query(update_no_of_days);
                // console.log("update_no_of_days_res-------------", update_no_of_days_res)
            }

            if(working_hour_sql_res.rows.length > 0){
            beat_grid_from_town_sql_res.rows[i]['proposed_visit_date'] = dtUtil.addDays(dtUtil.ISOtoLocal(working_hour_sql_res.rows[0]['planned_date__c']), working_hour_sql_res.rows[0]['no_of_days_for_the_completion__c'])
            // console.log("=====================",dtUtil.addDays(dtUtil.ISOtoLocal(working_hour_sql_res.rows[0]['visit_completion_date__c']), working_hour_sql_res.rows[0]['no_of_days_for_the_completion__c']))
            }else{
                beat_grid_from_town_sql_res.rows[i]['proposed_visit_date'] = null;
            }


            let temp_arr = []
            let final_index = 0;
            let no_value_pushed_decider = 0;
            if (working_hour_sql_res.rows.length > 0) {

                const ongoing_index = working_hour_sql_res.rows.findIndex(data => data.grid_working_status_name === "Ongoing");
                temp_arr.push(ongoing_index)
                const planned_index = working_hour_sql_res.rows.findIndex(data => data.grid_working_status_name === "Planned");
                temp_arr.push(planned_index)
                const completed_index = working_hour_sql_res.rows.findIndex(data => data.grid_working_status_name === "Completed");
                temp_arr.push(completed_index)


                for (let j = 0; j < temp_arr.length; j++) {
                    if (temp_arr[j] > -1) {
                        no_value_pushed_decider = 1
                        final_index = temp_arr[j]
                        break;
                    }
                }
                console.log(`Temp Array ====> ${temp_arr}`);

                if (final_index > -1 && no_value_pushed_decider == 1) {
                    beat_grid_from_town_sql_res.rows[i]['grid_working_data'] = [working_hour_sql_res.rows[final_index]]
                } else {
                    beat_grid_from_town_sql_res.rows[i]['grid_working_data'] = []
                }
            } else {
                beat_grid_from_town_sql_res.rows[i]['grid_working_data'] = []
            }

            // if (working_hour_sql_res.rows.length > 0) {
            //     beat_grid_from_town_sql_res.rows[i]['grid_working_data'] = working_hour_sql_res.rows;

            // }
            // else {
            //     beat_grid_from_town_sql_res.rows[i] = {};
            //     // delete beat_grid_from_town_sql_res.rows[i];

            // }
        }

        if(beat_grid_from_town_sql_res.rows.length > 0){

            if(area_type == 'Beat'){
                for(let i = 0 ; i < beat_grid_from_town_sql_res.rows.length ; i++){
                    let account_coordinate_sql = `SELECT geo_location__latitude__s,geo_location__longitude__s FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_ACCOUNT_TABLE_NAME} where town__c = '${beat_grid_from_town_sql_res.rows[i]['parent_code__c']}'`
                    let result = await client.query(account_coordinate_sql);

                    if(result.rows.length > 0){
                        beat_grid_from_town_sql_res.rows[i]['account_info'] = result.rows

                    }else{
                        beat_grid_from_town_sql_res.rows[i]['account_info'] = []
                    }
                }
            }
            return beat_grid_from_town_sql_res.rows
        }else{
            return [];
        }
    }catch(e){
        console.log('Error in Town Wise Beat/Grid Function --->',e);
    }
}

async function townWiseGrid(town_id_arr,area_type){
    try{
        let grid_id = await getPicklistSfid('Area2__c','Area_Type__c','Grid')

        let fields = [`${SF_AREA_1_TABLE_NAME}.*,
        ${SF_GEOGRAPHICAL_TOWN_TABLE_NAME}.town_name__c as geographical_name,
        a1.name__c as parent_name`
    ]
        let tableName = SF_AREA_1_TABLE_NAME;
        var offset='0', limit='1000';
        let WhereClouse = [];
        if(area_type != undefined && area_type.length > 0){
            WhereClouse.push({ "fieldName":`${SF_AREA_1_TABLE_NAME}.area_type__c`, "fieldValue":area_type}); 
        }
        WhereClouse.push({ "fieldName": `${SF_AREA_1_TABLE_NAME}.parent_code__c`, "fieldValue": town_id_arr ,"type": 'IN'}); 
        WhereClouse.push({ "fieldName": `${SF_AREA_1_TABLE_NAME}.area_type__c`, "fieldValue": grid_id }); 
        WhereClouse.push({ "fieldName": `${SF_AREA_1_TABLE_NAME}.sfid`, "type": 'NOTNULL' }); 
        // WhereClouse.push({ "fieldName": `${SF_GEOGRAPHICAL_TOWN_TABLE_NAME}.sfid`, "type": 'NOTNULL' }); 



        let joins = [
            {
                "type": "LEFT",
                "table_name": `${SF_GEOGRAPHICAL_TOWN_TABLE_NAME}`,
                "p_table_field": `${SF_AREA_1_TABLE_NAME}.geographical_town__c`,
                "s_table_field": `${SF_GEOGRAPHICAL_TOWN_TABLE_NAME}.sfid `
            },
            {
                "type": "LEFT",
                "table_name": `${SF_AREA_1_TABLE_NAME} as a1`,
                "p_table_field": `${SF_AREA_1_TABLE_NAME}.parent_code__c`,
                "s_table_field": `a1.sfid `
            },
        ]

        let grid_from_town_sql = qry.fetchAllWithJoinQry(fields, tableName, joins, WhereClouse,offset,limit,`order by ${SF_AREA_1_TABLE_NAME}.createddate desc`);
        //let beat_grid_from_town_sql = qry.SelectAllQry(fields, tableName,WhereClouse, offset, limit,' order by createddate desc');
        console.log("grids from town sql ----> ", grid_from_town_sql);
        let grid_from_town_sql_res = await client.query(grid_from_town_sql);

        if(grid_from_town_sql_res.rows.length > 0){
            return grid_from_town_sql_res.rows
        }else{
            return [];
        }
    }catch(e){
        console.log('Error in Town Wise Grid Function --->',e);
    }
}

async function getTeamAreaWiseFunc(data){
    try{
            let team_data_map = {};
            let team_data_level_wise = {};
            let area1_sfid = [];
            let area_arr = [];
            let area_arr_tmp = [];
            
            let team_sql = `SELECT * FROM ${process.env.TABLE_SCHEMA_NAME}.team__c where sfid='${data}' `;
            let team_sql_data = await client.query(team_sql);
            if(team_sql_data.rows.length > 0){
              team_data_map[team_sql_data.rows[0]['sfid']] = team_sql_data.rows[0];
              team_data_level_wise[team_sql_data.rows[0]['designation__c']] = team_sql_data.rows;
              let lob__c = team_sql_data.rows[0]['lob__c'];
              let division__c = team_sql_data.rows[0]['division__c'];
              let branch__c = team_sql_data.rows[0]['branch__c'];
              let designation__c = team_sql_data.rows[0]['designation__c'];
              let area1_designation = [''];
              
              // Get Area from team territory mapping table
              let fields = ['team_territory_mapping__c.territory_code__c as team_territory_sfid, team_territory_mapping__c.territory_code__c, area1.*']
              let tableName = 'team_territory_mapping__c';
              let offset='0', limit='1000';
              let WhereClouse = [];
              WhereClouse.push({ "fieldName": "team_territory_mapping__c.team_member_id__c", "fieldValue": Object.keys(team_data_map), "type": "IN"});
              if(lob__c){
                WhereClouse.push({ "fieldName": "team_territory_mapping__c.lob__c", "fieldValue": lob__c});
              }

              let joins = [
                {
                  "type": "LEFT",
                  "table_name": `area1__c as area1`,
                  "p_table_field": `team_territory_mapping__c.territory_code__c`,
                  "s_table_field": `area1.sfid`
                }
              ]
              let territory_team_mapping_sql = qry.fetchAllWithJoinQry(fields, tableName, joins, WhereClouse, offset, limit,' order by team_territory_mapping__c.createddate desc');
              //console.log("territory_team_mapping_sql ------> ", territory_team_mapping_sql);
              let territory_team_mapping_sql_data = await client.query(territory_team_mapping_sql);
              
              if(territory_team_mapping_sql_data.rows.length > 0){
                territory_team_mapping_sql_data.rows.map((area)=> {
                  area_arr.push(area['team_territory_sfid'])
                  area_arr_tmp.push(area['team_territory_sfid'])
                })
              }
              let exit = false;
              for(let i=0; !exit; i++){
                console.log(`Iteration ${i}`)
                area_arr = sort.removeDuplicates(area_arr);
                area_arr_tmp = sort.removeDuplicates(area_arr_tmp);
                // get sfid of territory_team mapping
                let area_sql = `SELECT sfid FROM ${process.env.TABLE_SCHEMA_NAME}.area1__c where parent_code__c IN ('${area_arr_tmp.join("','")}'); `;
                let area_sql_data = await client.query(area_sql);
                //console.log('area_sql :::: -', area_sql)
                
                if(area_sql_data.rows.length > 0){
                    area_arr_tmp = [];
                    area_sql_data.rows.map((area)=> {
                        area_arr.push(area['sfid'])
                        area_arr_tmp.push(area['sfid'])
                    })
                }else {
                    exit = true;
                }
              }
              //console.log(area_arr)
              if(area_arr.length > 0){
                area_arr = sort.removeDuplicates(area_arr);
                // Area found
                let fields_team = ['team__c.sfid']
                let tableName_team = 'team__c';
                let offset_team='0', limit_team='1000';
                let WhereClouse_team = [];
                // team_member_id__c
                WhereClouse_team.push({ "fieldName": "team_territory_mapping__c.territory_code__c", "fieldValue": area_arr, "type": "IN"});
                if(lob__c){
                  WhereClouse_team.push({ "fieldName": "team_territory_mapping__c.lob__c", "fieldValue": lob__c});
                }
             
                let joins_team = [
                  {
                    "type": "LEFT",
                    "table_name": `team_territory_mapping__c`,
                    "p_table_field": `team__c.sfid`,
                    "s_table_field": `team_territory_mapping__c.team_member_id__c`
                  }
                ]
                let territory_team_mapping_sql_team = qry.fetchAllWithJoinQry(fields_team, tableName_team, joins_team, WhereClouse_team, offset_team, limit_team,' order by team_territory_mapping__c.createddate desc');
                //console.log("Fetching Team from Area Data ------> ", territory_team_mapping_sql_team);
                let territory_team_mapping_sql_team_data = await client.query(territory_team_mapping_sql_team);
                if(territory_team_mapping_sql_team_data.rows.length > 0){
                    return sort.removeDuplicates(territory_team_mapping_sql_team_data.rows.map((team) => team['sfid']));
                }  
                }else {
                    return [];
                }
            }else {
              response.response = { 'success': false, "data": validationError, "message": "Invalid Team member sfid" };
              response.status = 200;
              return response;
            }
            
          
    }catch(e){
      console.log("Error in getTeamAreaWiseFunc:::::>>>>>>> 007 :::::::", e);
    }
  }
  
async function getGeographicalTownFromTown(town_id){
    try{
        let array_check = Array.isArray(town_id)
        console.log(`ARRAY CHECK ===> ${array_check}`);
        let fields = ['*']
        let tableName = SF_AREA_1_TABLE_NAME;
        let offset='0', limit='100';
        let WhereClouse = [];
        //WhereClouse.push({ "fieldName": "town_code__c", "fieldValue": grid_sql_res.rows});  
        if(array_check == true){
            WhereClouse.push({ "fieldName": "sfid", "fieldValue": town_id ,"type": 'IN' });  
        }else{
            WhereClouse.push({ "fieldName": "sfid", "fieldValue": town_id });  
        }
        let town_sql = qry.SelectAllQry(fields, tableName,WhereClouse, offset, limit,' order by createddate desc');
        console.log(`Town Sql ----->  ${town_sql}`);
        let town_sql_res = await client.query(town_sql)
        if(town_sql_res.rows.length > 0){
            return town_sql_res.rows
        }else{
            return [];
        }
    }catch(e){
        console.log(`Error In Get Geographical Town from Town -----> ${e}`);
    }
}
async function getTeamRegionWiseData(data){
    try{
        let area_level_sql = `SELECT sfid FROM salesforce.picklist__c where field_name__c = 'Area_Level__c' order by name desc`
        let area_level_res = await client.query(area_level_sql)

        let level_9 = area_level_res.rows[0]['sfid']
        let level_8 = area_level_res.rows[1]['sfid']
        // let level_7= area_level_res.rows[2]['sfid']
        // let level_6= area_level_res.rows[3]['sfid']
        // let level_5= area_level_res.rows[4]['sfid']
        // let level_4= area_level_res.rows[5]['sfid']
        // let level_3= area_level_res.rows[6]['sfid']
        // let level_2= area_level_res.rows[7]['sfid']
        // let level_1= area_level_res.rows[8]['sfid']

        let nation_sfid_arr = [];
        let nation_data = [];
        let zone_sfid_arr = [];
        let zone_data = [];
        let region_sfid_arr = [];
        let region_data = [];
        let branch_sfid_arr = [];
        let branch_data = [];
        let territory_sfid_arr = [];
        let territory_data = [];
        let asm_data = [];
        let asm_sfid_arr = [];
        let town_data = [];
        let town_sfid_arr = [];
        let grid_beat_data = [];
        let grid_beat_sfid_arr = [];
        // let grid_data = [];
        // let grid_sfid_arr = [];
        let se_data = [];
        let se_sfid_arr = [];

        let fields = [`${SF_AREA_1_TABLE_NAME}.*, a1.name__c as parent_code_name`]
        let tableName = SF_AREA_1_TABLE_NAME;
        var offset='0', limit='1000';
        let WhereClouse = [];
        //for picklist value a050w000002jazRAAQ:area_level__c
        let area_picklist_sql = `SELECT * FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_PICKLIST_TABLE_NAME} 
        WHERE ${SF_PICKLIST_TABLE_NAME}.object_name__c='Area1__c' AND ${SF_PICKLIST_TABLE_NAME}.field_name__c= 'Area_Level__c'AND ${SF_PICKLIST_TABLE_NAME}.name= '1';`;
        let area_picklist = await client.query(area_picklist_sql);
        area_picklist = area_picklist.rows[0]['sfid'];
        //console.log("???????????????????????????????????",area_picklist)
        //WhereClouse.push({ "fieldName": "town_code__c", "fieldValue": grid_sql_res.rows});  
        WhereClouse.push({ "fieldName": `${SF_TEAM_TERRITORY_MAPPING_TABLE_NAME}.team_member_id__c`, "fieldValue": data});  
        WhereClouse.push({ "fieldName": "area1__c.area_level__c", "fieldValue":area_picklist });//a050w000002jazRAAQ
//*********************************************** nation Starts Here *********************************************************/
        let joins = [
            
            {
                "type": "LEFT",
                "table_name": `${SF_TEAM_TERRITORY_MAPPING_TABLE_NAME}`,
                "p_table_field": `${SF_AREA_1_TABLE_NAME}.sfid`,
                "s_table_field": `${SF_TEAM_TERRITORY_MAPPING_TABLE_NAME}.territory_code__c `
            },
            {
                "type": "LEFT",
                "table_name": `${SF_AREA_1_TABLE_NAME} as a1`,
                "p_table_field": `${SF_AREA_1_TABLE_NAME}.parent_code__c`,
                "s_table_field": `a1.sfid `
            }
        ]

        let nation_sql = qry.fetchAllWithJoinQry(fields, tableName, joins, WhereClouse, offset, limit,` order by ${SF_AREA_1_TABLE_NAME}.createddate desc`);
        console.log("nation_sql ::::::::::::::::::::::::::: ------> ", nation_sql);
        let nation_sql_res = await client.query(nation_sql);

        if(nation_sql_res.rows.length > 0){
            nation_sql_res.rows.map((sfids)=> {
                nation_sfid_arr.push(sfids['sfid'])  
                //console.log('lllllllllllllllllllllllllll',nation_sfid_arr)
                let obj = {
                    nation_id: sfids['sfid'],
                    nation_name: sfids['name__c'],
                    nation_parent_code: sfids['parent_code__c'],
                    nation_parent_code_name: sfids['parent_code_name'],
                    nation_code :sfids['name']
                };
                nation_data.push(obj); 

            });
        }else{
            nation_sfid_arr.push(nation_sql_res['parent_code__c']);
            nation_sfid_arr = await sort.removeDuplicates(nation_sfid_arr);
            console.log('inside if condition nation idss ----->',nation_sfid_arr);
        }
//*****************************************************  nation Ends Here   *******************************************************/
    if(nation_sql_res.rows.length > 0 || (data.length > 0 && data != undefined)){
            
         let fields = [`${SF_AREA_1_TABLE_NAME}.*, a2.name__c as parent_code_name`]
         let tableName = SF_AREA_1_TABLE_NAME;
          var offset='0', limit='100';
         let WhereClouse = [];
        //for picklist value a050w000002jazWAAQ:area_level__c
        let area_picklist_sql = `SELECT * FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_PICKLIST_TABLE_NAME} 
        WHERE ${SF_PICKLIST_TABLE_NAME}.object_name__c='Area1__c' AND ${SF_PICKLIST_TABLE_NAME}.field_name__c= 'Area_Level__c'AND ${SF_PICKLIST_TABLE_NAME}.name= '2';`;
        let area_picklist = await client.query(area_picklist_sql);
        area_picklist = area_picklist.rows[0]['sfid'];
         if(nation_sfid_arr.length>0 && nation_sfid_arr[0]!=undefined)  {
             WhereClouse.push({ "fieldName": `${SF_AREA_1_TABLE_NAME}.parent_code__c`, "fieldValue": nation_sfid_arr ,"type": 'IN' });  
             WhereClouse.push({ "fieldName":   `${SF_AREA_1_TABLE_NAME}.area_level__c`, "fieldValue": area_picklist});//a050w000002jazWAAQ
            //WhereClouse.push({ "fieldName": `${SF_TEAM_TERRITORY_MAPPING_TABLE_NAME}.team_member_id__c`, "fieldValue": data});  
        }else{
                     
            //WhereClouse.push({ "fieldName": `${SF_AREA_1_TABLE_NAME}.parent_code__c`, "fieldValue": nation_sfid_arr  });  
            WhereClouse.push({ "fieldName":   `${SF_AREA_1_TABLE_NAME}.area_level__c`, "fieldValue": area_picklist});
            WhereClouse.push({ "fieldName": `${SF_TEAM_TERRITORY_MAPPING_TABLE_NAME}.team_member_id__c`, "fieldValue": data});  
                }
    let joins = [
        {
            "type": "LEFT",
            "table_name": `${SF_TEAM_TERRITORY_MAPPING_TABLE_NAME}`,
            "p_table_field": `${SF_AREA_1_TABLE_NAME}.sfid`,
            "s_table_field": `${SF_TEAM_TERRITORY_MAPPING_TABLE_NAME}.territory_code__c `
        },
        {
            "type": "LEFT",
            "table_name": `${SF_AREA_1_TABLE_NAME} as a2`,
            "p_table_field": `${SF_AREA_1_TABLE_NAME}.parent_code__c`,
            "s_table_field": `a2.sfid `
        },
        
    ]

    let zone_sql = qry.fetchAllWithJoinQry(fields, tableName, joins, WhereClouse, offset, limit,` order by ${SF_AREA_1_TABLE_NAME}.createddate desc`);
    console.log("zone_sql:::::::::::::::::::::::::::::::::::::::::::::::::::::  ------> ", zone_sql);
    let zone_sql_res = await client.query(zone_sql);

    if(zone_sql_res.rows.length > 0){
        zone_sql_res.rows.map((sfids)=> {
            zone_sfid_arr.push(sfids['sfid'])   
            let obj = {
                zone_id: sfids['sfid'],
                    zone_name: sfids['name__c'],
                    zone_parent_code: sfids['parent_code__c'],
                    zone_parent_code_name: sfids['parent_code_name'],
                    zone_code :sfids['name']
            };
            zone_data.push(obj); 

        });
    }else{
        zone_sfid_arr.push(zone_sql_res['parent_code__c']);
        zone_sfid_arr = await sort.removeDuplicates(zone_sfid_arr);
        console.log('inside if condition zone idss ----->',zone_sfid_arr);
    }
//******************************************* zone Ends Here  **************************************************/
            if(zone_sql_res.rows.length > 0 || (data.length > 0 && data != undefined)){

                let fields = [`${SF_AREA_1_TABLE_NAME}.*, a3.name__c as parent_code_name`]
                let tableName = SF_AREA_1_TABLE_NAME;
                var offset='0', limit='100';
                let WhereClouse = [];
                //for picklist value a050w000002jazbAAA:area_level__c
                let area_picklist_sql = `SELECT * FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_PICKLIST_TABLE_NAME} 
                 WHERE ${SF_PICKLIST_TABLE_NAME}.object_name__c='Area1__c' AND ${SF_PICKLIST_TABLE_NAME}.field_name__c= 'Area_Level__c'AND ${SF_PICKLIST_TABLE_NAME}.name= '3';`;
                let area_picklist = await client.query(area_picklist_sql);
                area_picklist = area_picklist.rows[0]['sfid'];
            if(zone_sfid_arr.length>0 &&   zone_sfid_arr[0]!=undefined )  {
                //WhereClouse.push({ "fieldName": "town_code__c", "fieldValue": grid_sql_res.rows});  
                WhereClouse.push({ "fieldName": `${SF_AREA_1_TABLE_NAME}.parent_code__c`, "fieldValue": zone_sfid_arr ,"type": 'IN' });  
                WhereClouse.push({ "fieldName": `${SF_AREA_1_TABLE_NAME}.area_level__c`, "fieldValue": area_picklist});//a050w000002jazbAAA
            }else{
                     
                //WhereClouse.push({ "fieldName": `${SF_AREA_1_TABLE_NAME}.parent_code__c`, "fieldValue": nation_sfid_arr  });  
                WhereClouse.push({ "fieldName":   `${SF_AREA_1_TABLE_NAME}.area_level__c`, "fieldValue": area_picklist});
                WhereClouse.push({ "fieldName": `${SF_TEAM_TERRITORY_MAPPING_TABLE_NAME}.team_member_id__c`, "fieldValue": data});  
                    }
                let joins = [
                    
                    {
                        "type": "LEFT",
                        "table_name": `${SF_TEAM_TERRITORY_MAPPING_TABLE_NAME}`,
                        "p_table_field": `${SF_AREA_1_TABLE_NAME}.sfid`,
                        "s_table_field": `${SF_TEAM_TERRITORY_MAPPING_TABLE_NAME}.territory_code__c `
                    },
                    {
                        "type": "LEFT",
                        "table_name": `${SF_AREA_1_TABLE_NAME} as a3`,
                        "p_table_field": `${SF_AREA_1_TABLE_NAME}.parent_code__c`,
                        "s_table_field": `a3.sfid `
                    }
                ]

                let region_sql = qry.fetchAllWithJoinQry(fields, tableName, joins, WhereClouse, offset, limit,` order by ${SF_AREA_1_TABLE_NAME}.createddate desc`);
                console.log(" region_sql ------> ", region_sql);
                let region_sql_res = await client.query(region_sql);

                if(region_sql_res.rows.length > 0){
                    region_sql_res.rows.map((sfids)=> {
                        region_sfid_arr.push(sfids['sfid'])  
                        let obj = {
                            region_id: sfids['sfid'],
                            region_name: sfids['name__c'],
                            region_parent_code: sfids['parent_code__c'],
                            region_parent_code_name : sfids['parent_code_name'],
                            region_code : sfids['name']
                        };
                        region_data.push(obj);   

                    });
                }else{
                    region_sfid_arr.push(region_sql_res['parent_code__c']);
                    region_sfid_arr = await sort.removeDuplicates(region_sfid_arr);
                    console.log('inside if condition region idss ----->',region_sfid_arr);
                }
//********************************** region Ends Here ****************************************************/
                if(region_sql_res.rows.length > 0 || (data.length > 0 && data != undefined)){
                    
                    let fields = [`${SF_AREA_1_TABLE_NAME}.*, ${SF_PICKLIST_TABLE_NAME}.name as area_type_name, a4.name__c as parent_code_name`]
                    let tableName = SF_AREA_1_TABLE_NAME;
                    var offset='0', limit='100';
                    let WhereClouse = [];
                    //for picklist value a050w000002jazqAAA:area_level__c
                    let area_picklist_sql = `SELECT * FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_PICKLIST_TABLE_NAME} 
                    WHERE ${SF_PICKLIST_TABLE_NAME}.object_name__c='Area1__c' AND ${SF_PICKLIST_TABLE_NAME}.field_name__c= 'Area_Level__c'AND ${SF_PICKLIST_TABLE_NAME}.name= '4';`;
                    let area_picklist = await client.query(area_picklist_sql);
                         area_picklist = area_picklist.rows[0]['sfid'];
                    if(region_sfid_arr.length>0 &&   region_sfid_arr[0]!=undefined )  {
                    WhereClouse.push({ "fieldName": "area1__c.parent_code__c", "fieldValue": region_sfid_arr ,"type": 'IN' });  
                    WhereClouse.push({ "fieldName": "area1__c.area_level__c", "fieldValue": area_picklist});//a050w000002jazqAAA
                    }else{
                    WhereClouse.push({ "fieldName": `${SF_TEAM_TERRITORY_MAPPING_TABLE_NAME}.team_member_id__c`, "fieldValue": data}); 
                    WhereClouse.push({ "fieldName": "area1__c.area_level__c", "fieldValue": area_picklist});
                    }
                    let joins = [
                        {
                            "type": "LEFT",
                            "table_name": `${SF_TEAM_TERRITORY_MAPPING_TABLE_NAME}`,
                            "p_table_field": `${SF_AREA_1_TABLE_NAME}.sfid`,
                            "s_table_field": `${SF_TEAM_TERRITORY_MAPPING_TABLE_NAME}.territory_code__c `
                        },
                        {
                            "type": "LEFT",
                            "table_name": `${SF_PICKLIST_TABLE_NAME}`,
                            "p_table_field": `${SF_AREA_1_TABLE_NAME}.area_type__c`,
                            "s_table_field": `${SF_PICKLIST_TABLE_NAME}.sfid`
                        },
                        {
                            "type": "LEFT",
                            "table_name": `${SF_AREA_1_TABLE_NAME} as a4`,
                            "p_table_field": `${SF_AREA_1_TABLE_NAME}.parent_code__c`,
                            "s_table_field": `a4.sfid `
                        }
                    ]

                    let branch_sql = qry.fetchAllWithJoinQry(fields, tableName, joins, WhereClouse, offset, limit,`order by ${SF_AREA_1_TABLE_NAME}.createddate desc`);
                    console.log("branch_sql  ------> ", branch_sql);
                    let branch_sql_res = await client.query(branch_sql);

                    if(branch_sql_res.rows.length > 0){
                        branch_sql_res.rows.map((sfids)=> {
                            branch_sfid_arr.push(sfids['sfid'])  
                            let obj = {
                                branch_id: sfids['sfid'],
                                branch_name: sfids['name__c'],
                                branch_parent_code: sfids['parent_code__c'],
                                branch_parent_code_name: sfids['parent_code_name'],
                                branch_code :sfids['name']
                            };
                            branch_data.push(obj); 
                        });
                    }else{
                        branch_sfid_arr.push(branch_sql_res['parent_code__c']);
                        branch_sfid_arr = await sort.removeDuplicates(branch_sfid_arr);
                        console.log('inside if condition branch idss ----->',branch_sfid_arr);
                    }
 //*********************************** branch ENds Here ***************************************************************/ 
                    if(branch_sql_res.rows.length > 0 || (data.length > 0 && data != undefined)){
                        let fields = [`${SF_AREA_1_TABLE_NAME}.*, ${SF_PICKLIST_TABLE_NAME}.name as area_type_name, a5.name__c as parent_code_name,${SF_AREA_1_TABLE_NAME}.parent_code__c`]
                        let tableName = SF_AREA_1_TABLE_NAME;
                        var offset='0', limit='100';
                        let WhereClouse = [];
                        //for picklist value a050w000002jb05AAA:area_level__c
                        let area_picklist_sql = `SELECT * FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_PICKLIST_TABLE_NAME} 
                        WHERE ${SF_PICKLIST_TABLE_NAME}.object_name__c='Area1__c' AND ${SF_PICKLIST_TABLE_NAME}.field_name__c= 'Area_Level__c'AND ${SF_PICKLIST_TABLE_NAME}.name= '5';`;
                        let area_picklist = await client.query(area_picklist_sql);
                            area_picklist = area_picklist.rows[0]['sfid'];
                    if(branch_sfid_arr.length>0 &&   branch_sfid_arr[0]!=undefined )  {
                        WhereClouse.push({ "fieldName": "area1__c.parent_code__c", "fieldValue": branch_sfid_arr ,"type": 'IN' });  
                        WhereClouse.push({ "fieldName": "area1__c.area_level__c", "fieldValue": area_picklist});//a050w000002jb05AAA
                    }else{
                        WhereClouse.push({ "fieldName": "area1__c.area_level__c", "fieldValue": area_picklist});
                        WhereClouse.push({ "fieldName": `${SF_TEAM_TERRITORY_MAPPING_TABLE_NAME}.team_member_id__c`, "fieldValue": data}); 


                    }
                        let joins = [
                           
                            {
                                "type": "LEFT",
                                "table_name": `${SF_TEAM_TERRITORY_MAPPING_TABLE_NAME}`,
                                "p_table_field": `${SF_AREA_1_TABLE_NAME}.sfid`,
                                "s_table_field": `${SF_TEAM_TERRITORY_MAPPING_TABLE_NAME}.territory_code__c `
                            },
                            {
                                "type": "LEFT",
                                "table_name": `${SF_PICKLIST_TABLE_NAME}`,
                                "p_table_field": `${SF_AREA_1_TABLE_NAME}.area_type__c`,
                                "s_table_field": `${SF_PICKLIST_TABLE_NAME}.sfid `
                            },
                            {
                                "type": "LEFT",
                                "table_name": `${SF_AREA_1_TABLE_NAME} as a5`,
                                "p_table_field": `${SF_AREA_1_TABLE_NAME}.parent_code__c`,
                                "s_table_field": `a5.sfid `
                            }
                        ]

                        let asm_sql = qry.fetchAllWithJoinQry(fields, tableName, joins, WhereClouse, offset, limit,`order by ${SF_AREA_1_TABLE_NAME}.createddate desc`);
                        console.log("asm_sql  ------> ", asm_sql);
                        let asm_sql_res = await client.query(asm_sql);
                        let asm_uniquedata= await getUniqueValuesFromArrayOfObject(asm_sql_res.rows)

                        if(asm_sql_res.rows.length > 0){
                            asm_uniquedata.map((sfids)=> {
                                asm_sfid_arr.push(sfids['sfid'])  
                                let obj = {
                                asm_id: sfids['sfid'],
                                asm_name: sfids['name__c'],
                                asm_parent_code: sfids['parent_code__c'],
                                asm_parent_code_name: sfids['parent_code_name'],
                                area_type : sfids['area_type__c'],
                                area_type_name : sfids['area_type_name'],
                                area_code: sfids['name']
                                };
                                asm_data.push(obj);

                            });
                    

                        }else{
                            asm_sfid_arr.push(asm_uniquedata['parent_code__c']);
                            asm_sfid_arr = await sort.removeDuplicates(asm_sfid_arr);
                            console.log('inside if condition asm idss ----->',asm_sfid_arr);
                        }

 //*********************************** ASM Ends Here ***************************************************************/                   
                    if(asm_sql_res.rows.length > 0 || (data.length > 0 && data != undefined)){
                        //let fields = [`${SF_TEAM_TERRITORY_MAPPING_TABLE_NAME}.*, ${SF_PICKLIST_TABLE_NAME}.name as area_type_name, a6.name__c as parent_code_name , p1.name as territory_type_name`]
                        let fields = [`${SF_AREA_1_TABLE_NAME}.*,${SF_TEAM_TERRITORY_MAPPING_TABLE_NAME}.territory_type__c as parent_code_name,${SF_PICKLIST_TABLE_NAME}.name as area_type_name, a6.name__c as parent_code_name , p1.name as territory_type_name`]
                        let tableName = SF_AREA_1_TABLE_NAME;
                        var offset='0', limit='100';
                        let WhereClouse = [];
                        //for picklist value a050w000002jb0AAAQ:area_level__c
                        let area_picklist_sql = `SELECT * FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_PICKLIST_TABLE_NAME} 
                        WHERE ${SF_PICKLIST_TABLE_NAME}.object_name__c='Area1__c' AND ${SF_PICKLIST_TABLE_NAME}.field_name__c= 'Area_Level__c'AND ${SF_PICKLIST_TABLE_NAME}.name= '6';`;
                        let area_picklist = await client.query(area_picklist_sql);
                           area_picklist = area_picklist.rows[0]['sfid'];
                    if(asm_sfid_arr.length>0 &&   asm_sfid_arr[0]!=undefined )  {
                        WhereClouse.push({ "fieldName": `${SF_AREA_1_TABLE_NAME}.parent_code__c`, "fieldValue": asm_sfid_arr ,"type": 'IN' });  
                        WhereClouse.push({ "fieldName": `${SF_AREA_1_TABLE_NAME}.area_level__c`, "fieldValue": area_picklist});//a050w000002jb0AAAQ
                    }else{
                        WhereClouse.push({ "fieldName": `${SF_AREA_1_TABLE_NAME}.area_level__c`, "fieldValue": area_picklist});
                        WhereClouse.push({ "fieldName": `${SF_TEAM_TERRITORY_MAPPING_TABLE_NAME}.team_member_id__c`, "fieldValue": data}); 
                        
                    }
                        let joins = [
                            {
                                "type": "LEFT",
                                "table_name": `${SF_TEAM_TERRITORY_MAPPING_TABLE_NAME}`,
                                "p_table_field": `${SF_AREA_1_TABLE_NAME}.sfid`,
                                "s_table_field": `${SF_TEAM_TERRITORY_MAPPING_TABLE_NAME}.territory_code__c `
                            },
                            {
                                "type": "LEFT",
                                "table_name": `${SF_PICKLIST_TABLE_NAME}`,
                                "p_table_field": `${SF_AREA_1_TABLE_NAME}.area_type__c`,
                                "s_table_field": `${SF_PICKLIST_TABLE_NAME}.sfid `
                            },
                            {
                                "type": "LEFT",
                                "table_name": `${SF_AREA_1_TABLE_NAME} as a6`,
                                "p_table_field": `${SF_AREA_1_TABLE_NAME}.parent_code__c`,
                                "s_table_field": `a6.sfid `
                            },
                            {
                                "type": "LEFT",
                                "table_name": `${SF_PICKLIST_TABLE_NAME} as p1`,
                                "p_table_field": `${SF_AREA_1_TABLE_NAME}.territory_type__c`,
                                "s_table_field": `p1.sfid `
                            },
                        ]

                        let se_sql = qry.fetchAllWithJoinQry(fields, tableName, joins, WhereClouse, offset, limit,`order by ${SF_TEAM_TERRITORY_MAPPING_TABLE_NAME}.createddate desc`);
                        console.log("se sql ------> ", se_sql);
                        let se_sql_res = await client.query(se_sql);

                        if(se_sql_res.rows.length > 0){
                            se_sql_res.rows.map((sfids)=> {
                                se_sfid_arr.push(sfids['sfid'])  
                                let obj = {
                                    se_id: sfids['sfid'],
                                    se_name: sfids['name__c'],
                                    se_parent_code: sfids['parent_code__c'],
                                    se_parent_code_name: sfids['parent_code_name'],
                                    se_code: sfids['name'],
                                    se_type : sfids['area_type__c'],
                                    se_type_name : sfids['territory_type_name']
                                };
                                se_data.push(obj);
                                
                            });
                        }else{
                            se_sfid_arr.push(se_sql_res['parent_code__c']);
                            se_sfid_arr = await sort.removeDuplicates(se_sfid_arr);
                            console.log('inside if condition se idss ----->',se_sfid_arr);
                        }
 //*********************************** SE Ends Here ***************************************************************/                   
                        if(se_sql_res.rows.length > 0 || (data.length > 0 && data != undefined)){
                            let fields = [`${SF_AREA_1_TABLE_NAME}.*, ${SF_AREA_1_TABLE_NAME}.name__c as parent_code_name, p1.name as territory_type_name,${SF_PICKLIST_TABLE_NAME}.name as area_type_name`]
                            //let fields = [`${SF_TEAM_TERRITORY_MAPPING_TABLE_NAME}.*, ${SF_PICKLIST_TABLE_NAME}.name as area_type_name, ${SF_AREA_1_TABLE_NAME}.name__c as parent_code_name , p1.name as territory_type_name`]
                            let tableName = SF_AREA_1_TABLE_NAME;
                            var offset='0', limit='100';
                            let WhereClouse = [];
                            //for picklist value a050w000003RLNfAAO:area_level__c
                            let area_picklist_sql = `SELECT * FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_PICKLIST_TABLE_NAME} 
                            WHERE ${SF_PICKLIST_TABLE_NAME}.object_name__c='Area1__c' AND ${SF_PICKLIST_TABLE_NAME}.field_name__c= 'Area_Level__c'AND ${SF_PICKLIST_TABLE_NAME}.name= '7';`;
                            let area_picklist = await client.query(area_picklist_sql);
                           area_picklist = area_picklist.rows[0]['sfid'];
                         if(se_sfid_arr.length>0 &&   se_sfid_arr[0]!=undefined )  {
                            WhereClouse.push({ "fieldName": "area1__c.parent_code__c", "fieldValue": se_sfid_arr ,"type": 'IN' });  
                            WhereClouse.push({ "fieldName": "area1__c.area_level__c", "fieldValue": area_picklist});
                         }else{
                            //WhereClouse.push({ "fieldName": "area1__c.area_level__c", "fieldValue": 'a050w000003RLNfAAO'});
                            WhereClouse.push({ "fieldName": `${SF_TEAM_TERRITORY_MAPPING_TABLE_NAME}.team_member_id__c`, "fieldValue": data}); 
                            WhereClouse.push({ "fieldName": "area1__c.area_level__c", "fieldValue": 'area_picklist'});//a050w000003RLNfAAO

                         }
    
                            let joins = [
                                {
                                    "type": "LEFT",
                                    "table_name": `${SF_TEAM_TERRITORY_MAPPING_TABLE_NAME}`,
                                    "p_table_field": `${SF_AREA_1_TABLE_NAME}.sfid`,
                                    "s_table_field": `${SF_TEAM_TERRITORY_MAPPING_TABLE_NAME}.territory_code__c `
                                },
                                {
                                    "type": "LEFT",
                                    "table_name": `${SF_PICKLIST_TABLE_NAME}`,
                                    "p_table_field": `${SF_AREA_1_TABLE_NAME}.area_type__c`,
                                    "s_table_field": `${SF_PICKLIST_TABLE_NAME}.sfid `
                                },
                                {
                                    "type": "LEFT",
                                    "table_name": `${SF_AREA_1_TABLE_NAME} as a5`,
                                    "p_table_field": `${SF_AREA_1_TABLE_NAME}.parent_code__c`,
                                    "s_table_field": `a5.sfid `
                                },
                                {
                                    "type": "LEFT",
                                    "table_name": `${SF_PICKLIST_TABLE_NAME} as p1`,
                                    "p_table_field": `${SF_AREA_1_TABLE_NAME}.territory_type__c`,
                                    "s_table_field": `p1.sfid `
                                },
                            ]
    
                            let territory_sql = qry.fetchAllWithJoinQry(fields, tableName, joins, WhereClouse, offset, limit,`order by ${SF_AREA_1_TABLE_NAME}.createddate desc`);
                            console.log("Territory sql ------> ", territory_sql);
                            let territory_sql_res = await client.query(territory_sql);
                            let territory_uniquedata= await getUniqueValuesFromArrayOfObject(territory_sql_res.rows)
                            
                            if(territory_uniquedata.length > 0){
                                territory_uniquedata.map((sfids)=> {
                                    territory_sfid_arr.push(sfids['sfid'])  
                                    let obj = {
                                    territory_id: sfids['sfid'],
                                    territory_name: sfids['name__c'],
                                    territory_parent_code: sfids['parent_code__c'],
                                    territory_parent_code_name: sfids['parent_code_name'],
                                    territory_code: sfids['name'],
                                    area_type : sfids['area_type__c'],
                                    area_type_name : sfids['area_type_name'],
                                    territory_type : sfids['territory_type__c'],
                                    territory_type_name : sfids['territory_type_name'],
                                    };
                                    territory_data.push(obj);              
                                });
                            }else{
                                territory_sfid_arr.push(territory_sql_res['parent_code__c']);
                                territory_sfid_arr = await sort.removeDuplicates(territory_sfid_arr);
                                console.log('inside if condition territory idss ----->',territory_sfid_arr);
                            }
//********************************** Territory Ends Here *****************************************************/
                        if(territory_sql_res.rows.length > 0 || (data.length > 0 && data != undefined)){

                            let fields = [`${SF_AREA_1_TABLE_NAME}.*, ${SF_PICKLIST_TABLE_NAME}.name as area_type_name, a5.name__c as parent_code_name , p1.name as territory_type_name`]
                            let tableName = SF_AREA_1_TABLE_NAME;
                            var offset='0', limit='1000';
                            let WhereClouse = [];
                         if(territory_sfid_arr.length>0 &&   territory_sfid_arr[0]!=undefined )  {
                            WhereClouse.push({ "fieldName": "area1__c.parent_code__c", "fieldValue": territory_sfid_arr ,"type": 'IN' });  
                            WhereClouse.push({ "fieldName": "area1__c.area_level__c", "fieldValue": level_8});
                         }else{
                            WhereClouse.push({ "fieldName": "area1__c.area_level__c", "fieldValue": level_8});
                            WhereClouse.push({ "fieldName": `${SF_TEAM_TERRITORY_MAPPING_TABLE_NAME}.team_member_id__c`, "fieldValue": data}); 

                         }
    
                            let joins = [
                                {
                                    "type": "LEFT",
                                    "table_name": `${SF_TEAM_TERRITORY_MAPPING_TABLE_NAME}`,
                                    "p_table_field": `${SF_AREA_1_TABLE_NAME}.sfid`,
                                    "s_table_field": `${SF_TEAM_TERRITORY_MAPPING_TABLE_NAME}.territory_code__c `
                                },
                                {
                                    "type": "LEFT",
                                    "table_name": `${SF_PICKLIST_TABLE_NAME}`,
                                    "p_table_field": `${SF_AREA_1_TABLE_NAME}.area_type__c`,
                                    "s_table_field": `${SF_PICKLIST_TABLE_NAME}.sfid `
                                },
                                {
                                    "type": "LEFT",
                                    "table_name": `${SF_AREA_1_TABLE_NAME} as a5`,
                                    "p_table_field": `${SF_AREA_1_TABLE_NAME}.parent_code__c`,
                                    "s_table_field": `a5.sfid `
                                },
                                {
                                    "type": "LEFT",
                                    "table_name": `${SF_PICKLIST_TABLE_NAME} as p1`,
                                    "p_table_field": `${SF_AREA_1_TABLE_NAME}.territory_type__c`,
                                    "s_table_field": `p1.sfid `
                                },
                            ]
    
                            let town_sql = qry.fetchAllWithJoinQry(fields, tableName, joins, WhereClouse, offset, limit,`order by ${SF_AREA_1_TABLE_NAME}.createddate desc`);
                            console.log("town_sql  ------> ", town_sql);
                            let town_sql_res = await client.query(town_sql);
    
                            if(town_sql_res.rows.length > 0){
                                town_sql_res.rows.map((sfids)=> {
                                    town_sfid_arr.push(sfids['sfid']) 
                                    let obj = {
                                        town_id: sfids['sfid'],
                                        town_name: sfids['name__c'],
                                        town_type: sfids['territory_type_name'],
                                        geographical_town : sfids['geographical_town__c'],
                                        //town_code : sfids['town_code__c'],
                                        town_area_type : sfids['area_type_name'],
                                        town_code__c : sfids['town_parent__code']
                                    };
                                    town_data.push(obj);              
                                });


                            }else{
                                town_sfid_arr.push(town_sql_res['parent_code__c']);
                                console.log('.....................................',town_sfid_arr)
                                town_sfid_arr = await sort.removeDuplicates(town_sfid_arr);
                                console.log('inside if condition town idss ----->',town_sfid_arr);
                            }
 //*********************************** TOWN Ends Here ***************************************************************/                   
                            if(town_sql_res.length > 0 || (data.length > 0 && data != undefined)){
                                let fields = [`${SF_AREA_1_TABLE_NAME}.*, ${SF_PICKLIST_TABLE_NAME}.name as area_type_name, a5.name__c as parent_code_name , p1.name as territory_type_name,${SF_TEAM_TERRITORY_MAPPING_TABLE_NAME}.territory_type__c as parent_code_name`]
                                let tableName = SF_AREA_1_TABLE_NAME;
                                var offset='0', limit='1000';
                                let WhereClouse = [];
                            if(town_sfid_arr.length>0 &&   town_sfid_arr[0]!=undefined )  {
                                WhereClouse.push({ "fieldName": "area1__c.parent_code__c", "fieldValue": town_sfid_arr ,"type": 'IN' });  
                                WhereClouse.push({ "fieldName": "area1__c.area_level__c", "fieldValue": level_9});
                            }else{
                                WhereClouse.push({ "fieldName": "area1__c.area_level__c", "fieldValue": level_9});
                                WhereClouse.push({ "fieldName": `${SF_TEAM_TERRITORY_MAPPING_TABLE_NAME}.team_member_id__c`, "fieldValue": data}); 

                         }
    
                            let joins = [
                                {
                                    "type": "LEFT",
                                    "table_name": `${SF_TEAM_TERRITORY_MAPPING_TABLE_NAME}`,
                                    "p_table_field": `${SF_AREA_1_TABLE_NAME}.sfid`,
                                    "s_table_field": `${SF_TEAM_TERRITORY_MAPPING_TABLE_NAME}.territory_code__c `
                                },
                                {
                                    "type": "LEFT",
                                    "table_name": `${SF_PICKLIST_TABLE_NAME}`,
                                    "p_table_field": `${SF_AREA_1_TABLE_NAME}.area_type__c`,
                                    "s_table_field": `${SF_PICKLIST_TABLE_NAME}.sfid `
                                },
                                {
                                    "type": "LEFT",
                                    "table_name": `${SF_AREA_1_TABLE_NAME} as a5`,
                                    "p_table_field": `${SF_AREA_1_TABLE_NAME}.parent_code__c`,
                                    "s_table_field": `a5.sfid `
                                },
                                {
                                    "type": "LEFT",
                                    "table_name": `${SF_PICKLIST_TABLE_NAME} as p1`,
                                    "p_table_field": `${SF_AREA_1_TABLE_NAME}.territory_type__c`,
                                    "s_table_field": `p1.sfid `
                                },
                            ]
    
                            let grid_sql = qry.fetchAllWithJoinQry(fields, tableName, joins, WhereClouse, offset, limit,' order by createddate desc');
                            console.log("grid_sql ------> ", grid_sql);
                            let grid_sql_res = await client.query(grid_sql);
    
                            if(grid_sql_res.rows.length > 0){
                                grid_sql_res.rows.map((sfids)=> {
                                    grid_beat_sfid_arr.push(sfids['sfid'])  
                                    let obj = {
                                        grid_id: sfids['sfid'],
                                        grid_name: sfids['name__c'],
                                        geographical_town_name: sfids['geographical_name'],
                                        geographical_town : sfids['geographical_town__c'], 
                                        grid_parent_code : sfids['parent_code__c'],
                                        grid_coordinates : sfids['grid_cordinates__c'],
                                        perimeter : sfids['perimeter__c'],
                                        border_width : sfids['border_width__c'],
                                        grid_area : sfids['grid_area__c'],
                                        grid_frequency : sfids['grid_frequency__c'],
                                        no_of_days_to_complete_grid : sfids['no_of_days_to_complete_grid__c'],
                                        grid_potential : sfids['grid_potential__c'],
                                    };
                                    grid_beat_data.push(obj);              
                                });
//*********************************************** Grids ends Here *********************************************************/
                            }
                                return [nation_sfid_arr,nation_data,zone_sfid_arr,zone_data,region_sfid_arr,region_data,branch_sfid_arr,branch_data,territory_sfid_arr,territory_data,asm_sfid_arr,asm_data,se_sfid_arr,se_data,town_sfid_arr,town_data,grid_beat_data,grid_beat_sfid_arr];
                            }else{
                                return [nation_sfid_arr,nation_data,zone_sfid_arr,zone_data,region_sfid_arr,region_data,branch_sfid_arr,branch_data,territory_sfid_arr,territory_data,asm_sfid_arr,asm_data,se_sfid_arr,se_data,town_sfid_arr,town_data,grid_beat_data,grid_beat_sfid_arr];
                            }                           
                        }else{
                            return [nation_sfid_arr,nation_data,zone_sfid_arr,zone_data,region_sfid_arr,region_data,branch_sfid_arr,branch_data,territory_sfid_arr,territory_data,asm_sfid_arr,asm_data,se_sfid_arr,se_data,town_sfid_arr,town_data,grid_beat_data,grid_beat_sfid_arr];                       
                        }
                    }else{
                        return [nation_sfid_arr,nation_data,zone_sfid_arr,zone_data,region_sfid_arr,region_data,branch_sfid_arr,branch_data,territory_sfid_arr,territory_data,asm_sfid_arr,asm_data,se_sfid_arr,se_data,town_sfid_arr,town_data,grid_beat_data,grid_beat_sfid_arr];                    
                    }
                    }else{
                        return [nation_sfid_arr,nation_data,zone_sfid_arr,zone_data,region_sfid_arr,region_data,branch_sfid_arr,branch_data,territory_sfid_arr,territory_data,asm_sfid_arr,asm_data,se_sfid_arr,se_data,town_sfid_arr,town_data,grid_beat_data,grid_beat_sfid_arr];                    
                    }
                }else{
                    return [nation_sfid_arr,nation_data,zone_sfid_arr,zone_data,region_sfid_arr,region_data,branch_sfid_arr,branch_data,territory_sfid_arr,territory_data,asm_sfid_arr,asm_data,se_sfid_arr,se_data,town_sfid_arr,town_data,grid_beat_data,grid_beat_sfid_arr];               
                }
            }else{
                return [nation_sfid_arr,nation_data,zone_sfid_arr,zone_data,region_sfid_arr,region_data,branch_sfid_arr,branch_data,territory_sfid_arr,territory_data,asm_sfid_arr,asm_data,se_sfid_arr,se_data,town_sfid_arr,town_data,grid_beat_data,grid_beat_sfid_arr];            
            }
        }else{
            return [nation_sfid_arr,nation_data,zone_sfid_arr,zone_data,region_sfid_arr,region_data,branch_sfid_arr,branch_data,territory_sfid_arr,territory_data,asm_sfid_arr,asm_data,se_sfid_arr,se_data,town_sfid_arr,town_data,grid_beat_data,grid_beat_sfid_arr];        
        
        }
        }else{
            return [nation_sfid_arr,nation_data,zone_sfid_arr,zone_data,region_sfid_arr,region_data,branch_sfid_arr,branch_data,territory_sfid_arr,territory_data,asm_sfid_arr,asm_data,se_sfid_arr,se_data,town_sfid_arr,town_data,grid_beat_data,grid_beat_sfid_arr];        
      }
    }catch(e){
        console.log('Error in Get Region Wise Data Function ----->',e);
    }
}

async function getTeamFromTerritory(territory_id){
    try{
        //FROM BOTTOM TO TOP TRAVERSING i.e FINDING BRANCH FROM TERRITORY
        let total_ids = [];
        let temp_id = [];
        let territory_sql = `SELECT parent_code__c FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_AREA_1_TABLE_NAME} WHERE sfid = '${territory_id}'`
        //console.log('Territory SQL ---->',territory_sql );
        total_ids.push(territory_id) //pushing se id
        let territory_sql_res = await client.query(territory_sql);
        if(territory_sql_res.rows.length > 0){
            total_ids.push(territory_sql_res.rows[0]['parent_code__c']) //pushing se id
            let se_sql = `SELECT * FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_AREA_1_TABLE_NAME} WHERE sfid = '${territory_sql_res.rows[0]['parent_code__c']}'`
            console.log('SE SQL ---->',se_sql);
            let se_sql_res = await client.query(se_sql);
            if(se_sql_res.rows.length > 0){
                total_ids.push(se_sql_res.rows[0]['parent_code__c']) // pushing asm id
                let asm_sql = `SELECT * FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_AREA_1_TABLE_NAME} WHERE sfid = '${se_sql_res.rows[0]['parent_code__c']}'`
                console.log('ASM SQL ---->',asm_sql);
                let asm_sql_res = await client.query(asm_sql);
                if(asm_sql_res.rows.length > 0){
                    let branch_id = asm_sql_res.rows[0]['parent_code__c'];
                    total_ids.push(branch_id) //pushing branch id
                    console.log(`Total Ids From Bottom To Up Traversal =====> ${total_ids}`);
                    let asm_data_sql = `SELECT * FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_AREA_1_TABLE_NAME} WHERE parent_code__c = '${branch_id}'`
                    console.log('ASM DATA SQL ---->',asm_data_sql);
                    let asm_data_res = await client.query(asm_data_sql);
                    if(asm_data_res.rows.length > 0){
                        //let total_ids = [];
                        let asm_ids = [];
                        asm_data_res.rows.map((sfids) => {
                            asm_ids.push(sfids['sfid'])
                            total_ids.push(sfids['sfid'])
                            temp_id.push(sfids['sfid'])
                        })
                        //console.log(`Total Ids From Up To Bottom Traversal =====> ${total_ids} , Temp Arr ==> ${temp_id}`);
                        let se_data_sql = `SELECT * FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_AREA_1_TABLE_NAME} WHERE parent_code__c IN ('${asm_ids.join("','")}')`
                        console.log('SE DATA SQL ---->',se_data_sql);
                        let se_data_res = await client.query(se_data_sql);
                        if(se_data_res.rows.length > 0){
                            let se_ids = []
                            se_data_res.rows.map((sfids) => {
                                se_ids.push(sfids['sfid'])
                                total_ids.push(sfids['sfid'])
                            })
                            total_ids = sort.removeDuplicates(total_ids);
                            console.log(`Total Ids From Up To Bottom Traversal =====> ${total_ids}`);
                            //NOW FINDING TEAM ON THAT ASM & SE ID's
                            let team_sql = `SELECT * FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_TEAM_TERRITORY_MAPPING_TABLE_NAME} WHERE territory_code__c IN ('${total_ids.join("','")}')`
                            console.log('Team FROM ASM AND SE DATA SQL ---->',team_sql);
                            let team_sql_res = await client.query(team_sql);
                            if(team_sql_res.rows.length > 0){
                                let team_id_arr = [];
                                team_sql_res.rows.map((sfids) => {
                                    let obj = {
                                        team_id : sfids['team_member_id__c'],
                                        team_name : sfids['team_member_name__c'],
                                        territory_id : sfids['territory_code__c'],
                                        territory_name : sfids['territory_name__c']
                                    }
                                    team_id_arr.push(obj)
                                })

                                return team_id_arr;
                            }else{
                                console.log(`No Team Member Found`);
                                return []
                            }
                        }else{
                            //se data condition
                            console.log(`No SE data Found In Top Down traverse`);
                            return [];
                        }
                    }else{
                        //asm data condition
                        console.log(`No asm data Found In Top Down traverse`);
                        return [];
                    }
                }else{
                    //asm condition
                    console.log(`No asm data Found In Bottom Up traverse`);
                    return [];
                }
            }else{
                //se condition
                console.log(`No se data Found In Bottom Up traverse`);
                return []
            }
        }else{
            //terittory condition
            console.log(`No territory data Found In Bottom Up traverse`);
            return []
        }
    }catch(e){
        console.log(`Error In Get Team From Territory Function -------> ${e}`);
    }
}

async function getAllAreaFromTeam(team_id){
    try{
        let area_type_sql = `SELECT sfid FROM salesforce.picklist__c where field_name__c = 'Area_Type__c' order by name desc`
        let area_type_res = await client.query(area_type_sql)

        let nation = area_type_res.rows[5]['sfid']
        let zone = area_type_res.rows[0]['sfid']
        let region= area_type_res.rows[4]['sfid']
        let branch= area_type_res.rows[7]['sfid']
        let asm= area_type_res.rows[9]['sfid']
        let se= area_type_res.rows[3]['sfid']
        let territory= area_type_res.rows[2]['sfid']
        let town= area_type_res.rows[1]['sfid']


        let data_obj = {
            nation: [],
            zone: [],
            region: [],
            branch: [],
            asm: [],
            se: [],
            territory: [],
            town: []
        }
        let team_area_map_sql = `SELECT * FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_TEAM_TERRITORY_MAPPING_TABLE_NAME} where team_member_id__c = '${team_id}'`
        let team_area_result = await client.query(team_area_map_sql)
        if(team_area_result.rows.length > 0){
            let area_code = team_area_result.rows[0]['territory_code__c']
            let area_sql = `SELECT ${SF_AREA_1_TABLE_NAME}.*,${SF_PICKLIST_TABLE_NAME}.name as area_type_name
                            FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_AREA_1_TABLE_NAME} 
                            LEFT JOIN ${process.env.TABLE_SCHEMA_NAME}.${SF_PICKLIST_TABLE_NAME} ON ${SF_AREA_1_TABLE_NAME}.area_type__c=${SF_PICKLIST_TABLE_NAME}.sfid 
                            where ${SF_AREA_1_TABLE_NAME}.sfid = '${area_code}'`
            console.log(`Area SQL =====> ${area_sql}`);
            let area_sql_res = await client.query(area_sql);
            if(area_sql_res.rows.length > 0){
                let area_type = area_sql_res.rows[0]['area_type__c']

                let area_id = area_sql_res.rows[0]['sfid']
                let lower_area_id = []
                lower_area_id.push(area_id);

                let area_parent_id = area_sql_res.rows[0]['parent_code__c']
                let upper_area_id = area_parent_id

                switch(area_type) {
                    case nation:   //NATION
                        console.log('NATION');
                        data_obj.nation.push(area_sql_res.rows[0])
                        break;
                    case zone:   //ZONE
                        console.log('ZONE');
                        data_obj.zone.push(area_sql_res.rows[0])
                        break;
                    case region:   //REGION
                        console.log('REGION');
                        data_obj.region.push(area_sql_res.rows[0])
                        break;
                    case branch:   //BRANCH
                        console.log('BRANCH');
                        data_obj.branch.push(area_sql_res.rows[0])
                        break;
                    case asm:   //ASM
                        console.log('ASM');
                        data_obj.asm.push(area_sql_res.rows[0])
                        break;
                    case se:   //SE
                        console.log('SE');
                        data_obj.se.push(area_sql_res.rows[0])
                        break;
                    case territory:   //TERRITORY
                        console.log('TERRITORY');
                        data_obj.territory.push(area_sql_res.rows[0])
                        break;
                    case town:   //TOWN
                        console.log('TOWN');
                        data_obj.town.push(area_sql_res.rows[0])
                        break;
                    default:
                }

                for(let i = 0 ; i < 9 ; i++){
                    if(area_type == nation){
                        console.log(`BREAK DUE TO AREA TYPE`);
                        break;
                    }else{
                        let upper_area_sql = `SELECT ${SF_AREA_1_TABLE_NAME}.*,${SF_PICKLIST_TABLE_NAME}.name as area_type_name
                                         FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_AREA_1_TABLE_NAME} 
                                         LEFT JOIN ${process.env.TABLE_SCHEMA_NAME}.${SF_PICKLIST_TABLE_NAME} ON ${SF_AREA_1_TABLE_NAME}.area_type__c=${SF_PICKLIST_TABLE_NAME}.sfid 
                                         where ${SF_AREA_1_TABLE_NAME}.sfid = '${upper_area_id}'`
                        console.log(`AREA UPPER SQL IN ${i} ====> ${upper_area_sql}`);
                        let upper_area_sql_res = await client.query(upper_area_sql);
                        if(upper_area_sql_res.rows.length > 0){
                            upper_area_id =  upper_area_sql_res.rows[0]['parent_code__c'];
                            area_type = upper_area_sql_res.rows[0]['area_type__c']

                            switch(area_type) {
                                case nation:   //NATION
                                    console.log('NATION');
                                    data_obj.nation.push(upper_area_sql_res.rows[0])
                                    break;
                                case zone:   //ZONE
                                    console.log('ZONE');
                                    data_obj.zone.push(upper_area_sql_res.rows[0])
                                    break;
                                case region:   //REGION
                                    console.log('REGION');
                                    data_obj.region.push(upper_area_sql_res.rows[0])
                                    break;
                                case branch:   //BRANCH
                                    console.log('BRANCH');
                                    data_obj.branch.push(upper_area_sql_res.rows[0])
                                    break;
                                case asm:   //ASM
                                    console.log('ASM');
                                    data_obj.asm.push(upper_area_sql_res.rows[0])
                                    break;
                                case se:   //SE
                                    console.log('SE');
                                    data_obj.se.push(upper_area_sql_res.rows[0])
                                    break;
                                case territory:   //TERRITORY
                                    console.log('TERRITORY');
                                    data_obj.territory.push(upper_area_sql_res.rows[0])
                                    break;
                                case town:   //TOWN
                                    console.log('TOWN');
                                    data_obj.town.push(upper_area_sql_res.rows[0])
                                    break;
                                default:
                            }
                        }else{
                            console.log(`BREAK DUE TO NO DATA`);
                            break;
                        }
                    }
                }

                for(let i = 0 ; i < 9 ; i++){
                    if(area_type == town){  //area type of town
                        console.log(`BREAK DUE TO AREA TYPE`);
                        break;
                    }else{
                        let lower_area_sql = `SELECT ${SF_AREA_1_TABLE_NAME}.*,${SF_PICKLIST_TABLE_NAME}.name as area_type_name
                                         FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_AREA_1_TABLE_NAME} 
                                         LEFT JOIN ${process.env.TABLE_SCHEMA_NAME}.${SF_PICKLIST_TABLE_NAME} ON ${SF_AREA_1_TABLE_NAME}.area_type__c=${SF_PICKLIST_TABLE_NAME}.sfid 
                                         where ${SF_AREA_1_TABLE_NAME}.parent_code__c IN ('${lower_area_id.join("','")}')`
                        console.log(`AREA LOWER SQL IN ${i} ====> ${lower_area_sql}`);
                        let lower_area_sql_res = await client.query(lower_area_sql);
                        if(lower_area_sql_res.rows.length > 0){
                            upper_area_id =  lower_area_sql_res.rows[0]['parent_code__c'];
                            area_type = lower_area_sql_res.rows[0]['area_type__c']
                            lower_area_id.length = 0;
                            lower_area_sql_res.rows.map((ids) => {
                                lower_area_id.push(ids['sfid']);
                            })
                            let count = 0;
                            switch(area_type) {
                                case nation:   //NATION
                                    area_sql_res.rows.map((data) => {
                                        data_obj.nation.push(area_sql_res.rows[count])
                                        console.log(`COUNT ===> ${count}`);
                                    })
                                    break;
                                case zone:   //ZONE
                                    area_sql_res.rows.map((data) => {
                                        data_obj.zone.push(area_sql_res.rows[count])
                                        console.log(`COUNT ===> ${count}`);
                                    })                                    
                                    break;
                                case region:   //REGION
                                    area_sql_res.rows.map((data) => {
                                        data_obj.region.push(area_sql_res.rows[count])
                                        console.log(`COUNT ===> ${count}`);
                                    })
                                    break;
                                case branch:   //BRANCH
                                    area_sql_res.rows.map((data) => {
                                        data_obj.branch.push(area_sql_res.rows[count])
                                        console.log(`COUNT ===> ${count}`);
                                    })
                                    break;
                                case asm:   //ASM
                                    area_sql_res.rows.map((data) => {
                                        data_obj.asm.push(area_sql_res.rows[count])
                                        console.log(`COUNT ===> ${count}`);
                                    })
                                    break;
                                case se:   //SE
                                    area_sql_res.rows.map((data) => {
                                        data_obj.se.push(area_sql_res.rows[count])
                                        console.log(`COUNT ===> ${count}`);
                                    })
                                    break;
                                case territory:   //TERRITORY
                                    area_sql_res.rows.map((data) => {
                                        data_obj.territory.push(area_sql_res.rows[count])
                                        console.log(`COUNT ===> ${count}`);
                                    })
                                    break;
                                case town:   //TOWN
                                    area_sql_res.rows.map((data) => {
                                        data_obj.town.push(area_sql_res.rows[count])
                                        console.log(`COUNT ===> ${count}`);
                                    })
                                    break;
                                default:
                            }
                        }else{
                            console.log(`BREAK DUE TO NO DATA`);
                            break;
                        }
                    }
                }
                return data_obj;
            }else{
                //area part
                console.log(`No Data Of Area For Selected Team Member`);
            }
        }else{
            //team_area part
            console.log(`No Data Of Territory Area Mapping For Selected Team Member`);
        }
    }catch(e){
        console.log(`Error In Function Get All Area From Team  :: ${e}`);
    }
}
async function getDayWorkingHour(team__c, attendance_date, attendance_time) {
    try {
        let field = [`*`]
        let table_name = SF_ATTENDANCE_TABLE_NAME;
        let offset = '0', limit = '1000';
        let where_clouse = [];

        where_clouse.push({ "fieldName": "attendance_date__c", "fieldValue": attendance_date });
        where_clouse.push({ "fieldName": "emp_id__c", "fieldValue": team__c });
        where_clouse.push({ "fieldName": "sfid", "type": "NOTNULL" });

        let attendence_sql = await qry.SelectAllQry(field, table_name, where_clouse, offset, limit, ` order by createddate asc`);
        console.log("attendence_sql ::: ", attendence_sql);

        let attendence_sql_res = await client.query(attendence_sql);
        let start_time = moment(attendence_sql_res.rows[0]['start_time__c'], 'YYYY-MM-DD HH:mm:ss')
        let end_time = moment(attendance_time, 'YYYY-MM-DD HH:mm:ss')
        let pause_in_time__c = moment(attendence_sql_res.rows[0]['pause_start__c'], 'YYYY-MM-DD HH:mm:ss');
        let pause_out_time__c = moment(attendence_sql_res.rows[0]['pause_end__c'], 'YYYY-MM-DD HH:mm:ss');
        let working_duration = moment.duration(end_time.diff(start_time))
        let working_duration2 = working_duration.asHours().toFixed(2);
        //console.log('working_duration2',working_duration2)
        let totalHour = Math.floor(working_duration2 / 60) //total hours user have worked includinf pause
        let totalMinutes = working_duration2 % 60 //total hours user have worked includinf minutes
        const total = {
            "totalHour": totalHour,
            "totalMinutes": totalMinutes
        }
        //console.log("total",total)
        let pause = moment.duration(pause_out_time__c.diff(pause_in_time__c));
        let pause2 = pause.asHours();
        //console.log('pause2::::',pause2)
        let pauseHour = Math.floor(pause2 / 60) //total hours user have  paused
        let pauseMinutes = pause2 % 60 //total hours user have  minutes
        let diff_pause = working_duration2 - pause2;
        //console.log('diff_pause::::::::::::::::',diff_pause)
        const pauseTime = {
            "pauseHour": pauseHour,
            "pauseMinutes": pauseMinutes
        }
        console.log("pauseTime", pauseTime)
        let update_result;
        if (pause2) {
            console.log('if 13333')
            update_result = {
                "working_hour__c": diff_pause
            }
            console.log('pause:::::::::::', update_result)
        } else {
            console.log('else 13333')
            update_result = {
                "working_hour__c": working_duration2
            }
        }
        return update_result;
    } catch (e) {
        console.log('Error in Get working hour Calculation ----->', e);
    }
}

async function getDayWorkingHourInMinutes(team__c, attendance_date, attendance_time) {
    try {
        let field = [`*`]
        let table_name = SF_ATTENDANCE_TABLE_NAME;
        let offset = '0', limit = '1000';
        let where_clouse = [];
        let update_result;

        where_clouse.push({ "fieldName": "attendance_date__c", "fieldValue": attendance_date });
        where_clouse.push({ "fieldName": "emp_id__c", "fieldValue": team__c });
        where_clouse.push({ "fieldName": "sfid", "type": "NOTNULL" });

        let attendence_sql = await qry.SelectAllQry(field, table_name, where_clouse, offset, limit, ` order by createddate asc`);
        console.log("attendence_sql ::: ", attendence_sql);

        let attendence_sql_res = await client.query(attendence_sql);
        let start_time = moment(attendence_sql_res.rows[0]['start_time__c'], 'YYYY-MM-DD HH:mm:ss')
        let end_time = moment(attendance_time, 'YYYY-MM-DD HH:mm:ss')
        let pause_in_time__c = moment(attendence_sql_res.rows[0]['pause_start__c'], 'YYYY-MM-DD HH:mm:ss');
        let pause_out_time__c = moment(attendence_sql_res.rows[0]['pause_end__c'], 'YYYY-MM-DD HH:mm:ss');
        let working_duration = moment.duration(end_time.diff(start_time))
        let working_duration2 = working_duration.asMinutes().toFixed(2);
        //console.log('working_duration2',working_duration2)
        let totalHour = Math.floor(working_duration2 / 60) //total hours user have worked includinf pause
        let totalMinutes = working_duration2 % 60 //total hours user have worked includinf minutes
        const total = {
            "totalHour": totalHour,
            "totalMinutes": totalMinutes
        }
        console.log(`Pause In ----> ${pause_in_time__c} , Puse Out -----> ${pause_out_time__c}`);
        console.log(`Total ----> ${JSON.stringify(total)}`);
        update_result = {
            "working_hour__c": total.totalMinutes
        }
        //console.log("total",total)
        if(attendence_sql_res.rows[0]['pause_start__c'] != null && attendence_sql_res.rows[0]['pause_end__c'] != null){
            let pause = moment.duration(pause_out_time__c.diff(pause_in_time__c));
            let pause2 = pause.asMinutes();
            //console.log('pause2::::',pause2)
            let pauseHour = Math.floor(pause2 / 60) //total hours user have  paused
            let pauseMinutes = pause2 % 60 //total hours user have  minutes
            let diff_pause = working_duration2 - pause2;
            //console.log('diff_pause::::::::::::::::',diff_pause)
            const pauseTime = {
                "pauseHour": pauseHour,
                "pauseMinutes": pauseMinutes
            }
            console.log("pauseTime", pauseTime)
            if (pause2) {
                console.log('if Pause2')
                update_result = {
                    "working_hour__c": diff_pause
                }
                console.log('pause:::::::::::', update_result)
            } else {
                console.log('else Pause 2')
                update_result = {
                    "working_hour__c": working_duration2
                }
            }
        }  
        return update_result;
    } catch (e) {
        console.log('Error in Get working hour Calculation ----->', e);
    }
}

async function getSubordinatesOnArea(team_id){
    try{
        //@Notice Removed the Secondary area logic from this api 30-03-2022
        let area_id_arr = [];
        let team_id_arr = [];
        let temp_arr = [];
        let temp_area_arr = [];
        let lob_sql = `SELECT * FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_TEAM_TABLE_NAME} where sfid = '${team_id}'`
        console.log(`Lob SQl ----> ${lob_sql}`);
        let lob_sql_res = await client.query(lob_sql);
        let lob_value = lob_sql_res.rows[0]['lob__c']
        let territory_from_team_sql = `SELECT * FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_TEAM_TERRITORY_MAPPING_TABLE_NAME} where team_member_id__c = '${team_id}'`
        console.log(territory_from_team_sql)
        let result = await client.query(territory_from_team_sql);
        result.rows.map((ids)=>{
            area_id_arr.push(ids['territory_code__c'])
            temp_area_arr.push(ids['territory_code__c'])
        })
        console.log(`Territory Code ---> ${area_id_arr}`);
        // let temp_count = 0;
        // let area_check_sql = `SELECT * FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_AREA_1_TABLE_NAME} where sfid IN ('${area_id_arr.join("','")}')`
        // let area_check_result =  await client.query(area_check_sql);
        // if(area_check_result.rows.length > 0){
        //     area_check_result.rows.map((data) => {
        //         if(data['area_type__c'] == ){
        //             temp_count ++
        //         }
        //     })
        // }

        if(result.rows.length > 0){
            let territory_type_sql = `SELECT sfid FROM salesforce.picklist__c where field_name__c = 'Territory_Type__c' order by name desc`
            let territory_type_res = await client.query(territory_type_sql)

            let primary_territory = territory_type_res.rows[1]['sfid']
            let secondary_territory = territory_type_res.rows[0]['sfid']

            let area_level_7_id = await getPicklistSfid('Area1__c','Area_Level__c','7')

            for(let i = 0 ; i < 7 ; i++){
                let set_variable = 0;
                let area_sql = `SELECT * FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_AREA_1_TABLE_NAME} where parent_code__c IN ('${area_id_arr.join("','")}');`
                console.log(`For Iteration i = ${i}   SQl is -----> ${area_sql}`);
                let area_sql_res = await client.query(area_sql)
                if(area_sql_res.rows.length > 0 ){
                    area_id_arr.length = 0
                    area_sql_res.rows.map((id) => {
                        area_id_arr.push(id['sfid'])
                        temp_area_arr.push(id['sfid'])
                        if(id['territory_type__c'] == primary_territory || id['territory_type__c'] == secondary_territory){
                            area_id_arr.push(id['sfid']);     
                            temp_area_arr.push(id['sfid']);     
                        }
                        if(id['area_level__c'] == area_level_7_id ){
                            set_variable = 1;
                        }
                    })
                    if(set_variable == 1){
                        break ;
                    }
                }else{
                    break;
                }
                
                //console.log(`Area Id Array in ${i}th iteration is -----> ${temp_area_arr}`);           
            }
            temp_area_arr = sort.removeDuplicates(temp_area_arr);
            console.log(`Area Id Array In The End ----> ${temp_area_arr}`);
            if(temp_area_arr.length > 0){
                let team_sql_on_area = `SELECT ${SF_TEAM_TERRITORY_MAPPING_TABLE_NAME}.*, team__c.designation__c as designation
                                        FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_TEAM_TERRITORY_MAPPING_TABLE_NAME} 
                                        LEFT JOIN salesforce.team__c ON team_territory_mapping__c.team_member_id__c=team__c.sfid 
                                        where ${SF_TEAM_TERRITORY_MAPPING_TABLE_NAME}.territory_code__c IN ('${temp_area_arr.join("','")}')
                                        and ${SF_TEAM_TERRITORY_MAPPING_TABLE_NAME}.lob__c = '${lob_value}'`
                let sql_result = await client.query(team_sql_on_area)
                if(sql_result.rows.length > 0){
                    sql_result.rows.map((data) => {
                        team_id_arr.push(data['team_member_id__c'])
                        let obj = {
                            emp_name : data['team_member_name__c'],
                            id : data['team_member_id__c'],
                            designation : data['designation']
                        }
                        temp_arr.push(obj)
                    })
                }
                team_id_arr = sort.removeDuplicates(team_id_arr);
                console.log(`Team ids ----> ${team_id_arr}`);
                console.log(`Team data ----> ${temp_arr}`);
                return team_id_arr;
            }
        }else{
            console.log(`No Territory Mapped With This User`);
        }
    }catch(e){
        console.log(`Error In Function Get Subordinate On Area -----> ${e}`);
    }
}

async function getUpperAndLowerHerrarichy(team_id){
    try{
        let area_id;
        let lower_area_id = [];
        let team_id_arr = [];
        let temp_arr = [];
        let team_sql = `SELECT * FROM salesforce.team__c where sfid = '${team_id}'`
        let team_sql_res = await client.query(team_sql);
        let lob_value = team_sql_res.rows[0]['lob__c']
        console.log(`Lob Value ----> ${lob_value}`);
        let territory_from_team_sql = `SELECT * FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_TEAM_TERRITORY_MAPPING_TABLE_NAME} where team_member_id__c = '${team_id}' and lob__c = '${lob_value}'`
        let result = await client.query(territory_from_team_sql);
        let territory_code = result.rows[0]['territory_code__c']
        let area_arr = [];
        if(team_sql_res.rows.length > 0){
            // if(team_sql_res.rows[0]['designation__c'] == 'SE' || team_sql_res.rows[0]['designation__c'] == 'SSE' || team_sql_res.rows[0]['designation__c'] == 'SSA'){
            //     let parent_id;
            //     let area_parent_sql = `SELECT parent_code__c FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_AREA_1_TABLE_NAME} where sfid = '${territory_code}' `
            //     let area_parent_sql_res = await client.query(area_parent_sql)
            //     parent_id = area_parent_sql_res[0]['parent_code__c']
            //     area_arr.push(parent_id);
            //     for(let i = 0 ; i < 6 ; i++){
            //         let area_sql = `SELECT * FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_AREA_1_TABLE_NAME} where sfid = '${parent_id}' `
            //         let area_sql_res = await client.query(area_sql);
            //         if(area_sql_res.rows[0]['area_level__c'] == 'a050w000002jazqAAA'){
            //             area_arr.push(area_sql_res.rows[0]['parent_code__c']);
            //             break;
            //         }else{
            //             area_arr.push(area_sql_res.rows[0]['parent_code__c']);
            //             parent_id = area_sql_res.rows[0]['parent_code__c'];
            //         }
            //     }
            // }            
            // if(team_sql_res.rows[0]['designation__c'] == 'ASM'){
                //for finding upper area
                let upper_area_arr = []
                area_id = territory_code;
                let area_level_4_id = await getPicklistSfid('Area1__c','Area_Level__c','4')
                for(let i = 0 ; i < 6 ; i++){
                    let upper_area_sql = `SELECT * FROM salesforce.area1__c where sfid = '${area_id}'`
                    let upper_area_res = await client.query(upper_area_sql);
                    if(upper_area_res.rows.length > 0){
                        if(upper_area_res.rows[0]['area_level__c'] == area_level_4_id){
                            upper_area_arr.push(upper_area_res.rows[0]['sfid'])
                            break;
                        }else{
                            area_id = upper_area_res.rows[0]['parent_code__c'];
                            upper_area_arr.push(upper_area_res.rows[0]['sfid'])
                        }
                    }
                }

                //For Finding Lower Area
                let lower_area_arr = []
                lower_area_id.push(territory_code)
                let area_level_7_id = await getPicklistSfid('Area1__c','Area_Level__c','7')
                for(let i = 0 ; i < 6 ; i++){
                    let lower_area_sql = `SELECT * FROM salesforce.area1__c where parent_code__c IN ('${lower_area_id.join("','")}')`
                    console.log(`Lower Area Sql In ${i}th iteration ---> ${lower_area_sql}`);
                    let lower_area_res = await client.query(lower_area_sql);
                    if(lower_area_res.rows.length > 0){
                        if(lower_area_res.rows[0]['area_level__c'] == area_level_7_id){
                            lower_area_res.rows.map((data) => {
                                lower_area_arr.push(data['sfid'])
                            })
                            break;
                        }else{
                            lower_area_id.length = 0
                            lower_area_res.rows.map((data) => {
                                lower_area_arr.push(data['sfid'])
                                lower_area_id.push(data['sfid'])
                            })
                        }
                    }
                }

                upper_area_arr = sort.removeDuplicates(upper_area_arr);
                lower_area_arr = sort.removeDuplicates(lower_area_arr);

                console.log(`Upper Area Arr ----> ${upper_area_arr}`);
                console.log(`Lower Area Arr ----> ${lower_area_arr}`);
            // }
            // if(team_sql_res.rows[0]['designation__c'] == 'BSM'){
            //     let area_id = [];
            //     area_id.push(territory_code)
            //     area_arr.push(territory_code)
            //     for(let i = 0 ; i < 6 ; i++){
            //         let area_sql = `SELECT * 
            //                     FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_AREA_1_TABLE_NAME} 
            //                     where parent_code__c IN ('${area_id.join("','")}') `
            //         let area_sql_res = await client.query(area_sql)
            //         if(area_sql_res.rows[0]['area_level__c'] == 'a050w000002jazqAAA'){
            //             area_sql_res.rows.map((ids) => {
            //                 area_arr.push(ids['sfid'])
            //             })
            //             break;
            //         }else{
            //             area_sql_res.rows.map((ids) => {
            //                 area_arr.push(ids['sfid'])
            //                 area_id.length = 0;
            //                 area_id.push(ids['sfid'])
            //             })
            //         }
            //     }
                
            // }

            let all_area = [...upper_area_arr,...lower_area_arr]
            all_area = sort.removeDuplicates(all_area);
            console.log(`All Area Array ----> ${all_area}`);

            let team_sql_on_area = `SELECT ${SF_TEAM_TERRITORY_MAPPING_TABLE_NAME}.*, team__c.designation__c as designation,team__c.hierarchylevel__c,picklist__c.name as level_name
                                    FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_TEAM_TERRITORY_MAPPING_TABLE_NAME} 
                                    LEFT JOIN salesforce.team__c ON team_territory_mapping__c.team_member_id__c=team__c.sfid 
                                    LEFT JOIN salesforce.picklist__c ON picklist__c.sfid=team__c.hierarchylevel__c 
                                    where ${SF_TEAM_TERRITORY_MAPPING_TABLE_NAME}.territory_code__c IN ('${all_area.join("','")}')
                                    and (territory_type__c = 'Secondary' or territory_type__c is null)
                                    and ${SF_TEAM_TERRITORY_MAPPING_TABLE_NAME}.lob__c = '${lob_value}'
                                    `
            let sql_result = await client.query(team_sql_on_area)
            if(sql_result.rows.length > 0){
                sql_result.rows.map((data) => {
                    team_id_arr.push(data['team_member_id__c'])
                    let obj = {
                        team_name : data['team_member_name__c'],
                        team_id : data['team_member_id__c'],
                        designation : data['designation'],
                        level__c : data['hierarchylevel__c'],
                        employee_level : data['level_name']
                    }
                    temp_arr.push(obj)
                })
            }
            team_id_arr = sort.removeDuplicates(team_id_arr);
            console.log(`Team ids ----> ${team_id_arr}`);
            console.log(`Team data ----> ${temp_arr}`);
            return temp_arr;

        }
    }catch(e){
        console.log(`Error in Get Upper Lower Herrarichy ----> ${e}`);
    }
}

async function getBranchFromTown(town_id){
    try{
        let upper_area_arr = []
        let bsm_id = 'no team member'
        area_id = town_id;
        let area_level_4_id = await getPicklistSfid('Area1__c','Area_Level__c','4')
        for(let i = 0 ; i < 6 ; i++){
            let upper_area_sql = `SELECT * FROM salesforce.area1__c where sfid = '${area_id}'`
            let upper_area_res = await client.query(upper_area_sql);
            if(upper_area_res.rows.length > 0){
                if(upper_area_res.rows[0]['area_level__c'] == area_level_4_id){
                    upper_area_arr.push(upper_area_res.rows[0]['sfid'])
                    break;
                }else{
                    area_id = upper_area_res.rows[0]['parent_code__c'];
                    //upper_area_arr.push(upper_area_res.rows[0]['sfid'])
                }
            }
        }

        let branch_manager_sql = `SELECT ${SF_TEAM_TERRITORY_MAPPING_TABLE_NAME}.team_member_id__c as team_id,${SF_TEAM_TERRITORY_MAPPING_TABLE_NAME}.team_member_name__c as team_name, team__c.designation__c as designation,team__c.email__c as member_email
                                    FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_TEAM_TERRITORY_MAPPING_TABLE_NAME} 
                                    LEFT JOIN salesforce.team__c ON team_territory_mapping__c.team_member_id__c=team__c.sfid 
                                    where ${SF_TEAM_TERRITORY_MAPPING_TABLE_NAME}.territory_code__c IN ('${upper_area_arr.join("','")}')
                                    and team__c.designation__c = 'BSM'
                                    `
        console.log(`Branch Manger Sql ------> ${branch_manager_sql}`);
        let branch_manager_sql_res = await client.query(branch_manager_sql)

        if(branch_manager_sql_res.rows.length > 0){
            bsm_id = branch_manager_sql_res.rows[0]['member_email']
            return bsm_id
        }else{
            return bsm_id
        }
    }catch(e){
        console.log(`Error In Get Branch From Town Function -------> ${e}`);
    }
}

async function getLowerHerrarichyAndCC(team_id){
    try{
        //and (territory_type__c = 'Secondary' or territory_type__c is null)
        //removed this line of code from 3 sql cc sql , territory from teamsql , team sql on area
        let area_id;
        let lower_area_id = [];
        let team_id_arr = [];
        let temp_arr = [];
        let team_sql = `SELECT * FROM salesforce.team__c where sfid = '${team_id}'`
        let team_sql_res = await client.query(team_sql);
        let lob_value = team_sql_res.rows[0]['lob__c']
        console.log(`Lob Value ----> ${lob_value}`);
        let territory_from_team_sql = `SELECT * 
                                        FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_TEAM_TERRITORY_MAPPING_TABLE_NAME} 
                                        where team_member_id__c = '${team_id}' 
                                        and lob__c = '${lob_value}'`
        console.log(`Team From Territory Sql ----> ${territory_from_team_sql}`);
        let result = await client.query(territory_from_team_sql);
        let territory_code = result.rows[0]['territory_code__c']
        let area_arr = [];
        if(team_sql_res.rows.length > 0){
                let upper_area_arr = []
                area_id = territory_code;
                let area_level_4_id = await getPicklistSfid('Area1__c','Area_Level__c','4')
                for(let i = 0 ; i < 6 ; i++){
                    let upper_area_sql = `SELECT * FROM salesforce.area1__c where sfid = '${area_id}'`
                    let upper_area_res = await client.query(upper_area_sql);
                    if(upper_area_res.rows.length > 0){
                        if(upper_area_res.rows[0]['area_level__c'] == area_level_4_id){
                            upper_area_arr.push(upper_area_res.rows[0]['sfid'])
                            break;
                        }else{
                            area_id = upper_area_res.rows[0]['parent_code__c'];
                            //upper_area_arr.push(upper_area_res.rows[0]['sfid'])
                        }
                    }
                }

                let cc_sql_on_area = `SELECT ${SF_TEAM_TERRITORY_MAPPING_TABLE_NAME}.team_member_id__c as team_id,${SF_TEAM_TERRITORY_MAPPING_TABLE_NAME}.team_member_name__c as team_name, team__c.designation__c as designation,team__c.hierarchylevel__c,picklist__c.name as level_name
                                        FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_TEAM_TERRITORY_MAPPING_TABLE_NAME} 
                                        LEFT JOIN salesforce.team__c ON team_territory_mapping__c.team_member_id__c=team__c.sfid 
                                        LEFT JOIN salesforce.picklist__c ON picklist__c.sfid=team__c.hierarchylevel__c 
                                        where ${SF_TEAM_TERRITORY_MAPPING_TABLE_NAME}.territory_code__c IN ('${upper_area_arr.join("','")}')
                                        and ${SF_TEAM_TERRITORY_MAPPING_TABLE_NAME}.lob__c = '${lob_value}'
                                        and (team__c.designation__c = 'Sales Support  Desk' or team__c.designation__c = 'Call Center')
                                        `
                console.log(`cc_sql -------> ${cc_sql_on_area}`);
                let cc_sql_result = await client.query(cc_sql_on_area)



                //For Finding Lower Area
                let lower_area_arr = []
                lower_area_arr.push(territory_code)
                lower_area_id.push(territory_code)
                let area_level_7_id = await getPicklistSfid('Area1__c','Area_Level__c','7')
                for(let i = 0 ; i < 6 ; i++){
                    let lower_area_sql = `SELECT * FROM salesforce.area1__c where parent_code__c IN ('${lower_area_id.join("','")}')`
                    console.log(`Lower Area Sql In ${i}th iteration ---> ${lower_area_sql}`);
                    let lower_area_res = await client.query(lower_area_sql);
                    if(lower_area_res.rows.length > 0){
                        if(lower_area_res.rows[0]['area_level__c'] == area_level_7_id){
                            lower_area_res.rows.map((data) => {
                                lower_area_arr.push(data['sfid'])
                            })
                            break;
                        }else{
                            lower_area_id.length = 0
                            lower_area_res.rows.map((data) => {
                                lower_area_arr.push(data['sfid'])
                                lower_area_id.push(data['sfid'])
                            })
                        }
                    }
                }

                upper_area_arr = sort.removeDuplicates(upper_area_arr);
                lower_area_arr = sort.removeDuplicates(lower_area_arr);

                console.log(`Upper Area Arr ----> ${upper_area_arr}`);
                console.log(`Lower Area Arr ----> ${lower_area_arr}`);

                let all_area = [...lower_area_arr]
                all_area = sort.removeDuplicates(all_area);
                console.log(`All Area Array ----> ${all_area}`);

                let team_sql_on_area = `SELECT ${SF_TEAM_TERRITORY_MAPPING_TABLE_NAME}.*, team__c.designation__c as designation,team__c.hierarchylevel__c,picklist__c.name as level_name
                                        FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_TEAM_TERRITORY_MAPPING_TABLE_NAME} 
                                        LEFT JOIN salesforce.team__c ON team_territory_mapping__c.team_member_id__c=team__c.sfid 
                                        LEFT JOIN salesforce.picklist__c ON picklist__c.sfid=team__c.hierarchylevel__c 
                                        where ${SF_TEAM_TERRITORY_MAPPING_TABLE_NAME}.territory_code__c IN ('${all_area.join("','")}')
                                        and ${SF_TEAM_TERRITORY_MAPPING_TABLE_NAME}.lob__c = '${lob_value}'
                                        and team__c.designation__c NOT IN ('Sales Support  Desk','Call Center')
                                        `
                console.log(`Team SQL ON AREA -----> ${team_sql_on_area}`);
                let sql_result = await client.query(team_sql_on_area)
                if(sql_result.rows.length > 0){
                    sql_result.rows.map((data) => {
                        team_id_arr.push(data['team_member_id__c'])
                        let obj = {
                            team_name : data['team_member_name__c'],
                            team_id : data['team_member_id__c'],
                            designation : data['designation'],
                            level__c : data['hierarchylevel__c'],
                            employee_level : data['level_name']
                        }
                        temp_arr.push(obj)
                    })
                }
                team_id_arr = sort.removeDuplicates(team_id_arr);
                console.log(`Team ids ----> ${team_id_arr}`);
                temp_arr = [...temp_arr,...cc_sql_result.rows]
                console.log(`Team data ----> ${temp_arr}`);
                temp_arr = sort.removeDuplicates(temp_arr);
                return temp_arr;

        }
    }catch(e){
        console.log(`Error in Get Upper Lower Herrarichy ----> ${e}`);
    }
}

async function getBranchFromLowerArea(area_id){
    try{
        let branch_level_id;
        let branch_picklist = `SELECT * FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_PICKLIST_TABLE_NAME} WHERE field_name__c = 'Area_Level__c' and object_name__c = 'Area1__c' and name = '4'`
        let branch_picklist_res = await client.query(branch_picklist);
        if(branch_picklist_res.rows.length > 0){
            branch_level_id = branch_picklist_res.rows[0]['sfid']
        }else{
            branch_level_id = 'a01C7000000WYtxIAG'
        }
        let upper_area_arr;
        area_id = area_id;
        for(let i = 0 ; i < 6 ; i++){
            let upper_area_sql = `SELECT * FROM salesforce.area1__c where sfid = '${area_id}'`
            console.log(`Upper Area Sql -----> ${upper_area_sql}`);
            let upper_area_res = await client.query(upper_area_sql);
            if(upper_area_res.rows.length > 0){
                if(upper_area_res.rows[0]['area_level__c'] == branch_level_id){
                    //upper_area_arr.push(upper_area_res.rows[0]['sfid'])
                    upper_area_arr = upper_area_res.rows[0]['sfid']
                    break;
                }else{
                    area_id = upper_area_res.rows[0]['parent_code__c'];
                }
            }
        }
        console.log(`Branch id -------> ${upper_area_arr}`);
        return upper_area_arr;

    }catch(e){
        console.log(`Error In Get Branch From Lower Area ------> ${e}`);
    }
}

async function getCCFromTown(town_id){
    try{
        let upper_area_arr = []
        let cc_id = null
        area_id = town_id;
        let area_level_4_id = await getPicklistSfid('Area1__c','Area_Level__c','4')
        for(let i = 0 ; i < 6 ; i++){
            let upper_area_sql = `SELECT * FROM salesforce.area1__c where sfid = '${area_id}'`
            let upper_area_res = await client.query(upper_area_sql);
            if(upper_area_res.rows.length > 0){
                if(upper_area_res.rows[0]['area_level__c'] == area_level_4_id){
                    upper_area_arr.push(upper_area_res.rows[0]['sfid'])
                    break;
                }else{
                    area_id = upper_area_res.rows[0]['parent_code__c'];
                    //upper_area_arr.push(upper_area_res.rows[0]['sfid'])
                }
            }
        }

        let cc_sql = `SELECT ${SF_TEAM_TERRITORY_MAPPING_TABLE_NAME}.team_member_id__c as team_id,${SF_TEAM_TERRITORY_MAPPING_TABLE_NAME}.team_member_name__c as team_name, team__c.designation__c as designation,team__c.email__c as member_email
                                    FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_TEAM_TERRITORY_MAPPING_TABLE_NAME} 
                                    LEFT JOIN salesforce.team__c ON team_territory_mapping__c.team_member_id__c=team__c.sfid 
                                    where ${SF_TEAM_TERRITORY_MAPPING_TABLE_NAME}.territory_code__c IN ('${upper_area_arr.join("','")}')
                                    and team__c.designation__c = 'Call Center'
                                    `
        // console.log(`CC Sql ------> ${cc_sql}`);
        let cc_sql_res = await client.query(cc_sql)

        if(cc_sql_res.rows.length > 0){
            bsm_id = cc_sql_res.rows[0]['member_email']
            return cc_sql_res.rows[0]['team_id']
        }else{
            return cc_id
        }
    }catch(e){
        console.log(`Error In Get Branch From Town Function -------> ${e}`);
    }
}

async function getTownFromUpperArea(area_id,is_primary){
    try{
        let town_level_id;
        let town_picklist = `SELECT * FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_PICKLIST_TABLE_NAME} WHERE field_name__c = 'Area_Level__c' and name = '8'`
        let town_picklist_res = await client.query(town_picklist);
        if(town_picklist_res.rows.length > 0){
            town_level_id = town_picklist_res.rows[0]['sfid']
        }else{
            town_level_id = 'a01C7000000WYsDIAW'
        }

        let secondary_picklist = `Select * from ${process.env.TABLE_SCHEMA_NAME}.${SF_PICKLIST_TABLE_NAME} WHERE field_name__c = 'Territory_Type__c' and object_name__c = 'Area1__c' and name = 'Secondary'`
        let secondary_picklist_res = await client.query(secondary_picklist);
        if(secondary_picklist_res.rows.length > 0){
            secondary_id = secondary_picklist_res.rows[0]['sfid']
        }else{
            secondary_id = 'a01C7000000WYspIAG'
        }

        // console.log("town_level iddididi",town_level_id)
        // let upper_area_arr;
        // area_id = ;
        // for(let i = 0 ; i < 10 ; i++){
        //     let upper_area_sql = `SELECT * FROM salesforce.area1__c where parent_code__c IN ('${area_id.join("','")}')`
        //     let upper_area_res = await client.query(upper_area_sql);
        //     console.log(`Upper Area Sql -----> ${upper_area_sql}`);

        //     if(upper_area_res.rows.length > 0){
        //         if(upper_area_res.rows[i]['area_level__c'] == town_level_id){
        //             //upper_area_arr.push(upper_area_res.rows[0]['sfid'])
        //             upper_area_arr = upper_area_res.rows[0]['sfid']
        //             break;
        //         }else{
        //             area_id = upper_area_res.rows[0]['sfid'];
        //         }
        //     }
        // }

        let lower_area_arr = []
        let lower_area_id = []
        let temp_town_arr = []

        lower_area_arr.push(area_id)
        lower_area_id.push(area_id)
        for(let i = 0 ; i < 10 ; i++){
            let lower_area_sql ;

            if(is_primary){
                lower_area_sql = `SELECT * 
                                    FROM salesforce.area1__c 
                                    where parent_code__c IN ('${lower_area_id.join("','")}') 
                                    ` 
            }else{
                lower_area_sql = `SELECT * 
                                    FROM salesforce.area1__c 
                                    where parent_code__c IN ('${lower_area_id.join("','")}') 
                                    and (territory_type__c = '${secondary_id}' or territory_type__c is null)`
            }
            // console.log(`Lower Area Sql In ${i}th iteration ---> ${lower_area_sql}`);
            let lower_area_res = await client.query(lower_area_sql);
            if(lower_area_res.rows.length > 0){
                if(lower_area_res.rows[0]['area_level__c'] == town_level_id){
                    lower_area_res.rows.map((data) => {
                        lower_area_arr.push(data['sfid'])
                        temp_town_arr.push(data['sfid'])
                    })
                    break;
                }else{
                    lower_area_id.length = 0
                    lower_area_res.rows.map((data) => {
                        lower_area_arr.push(data['sfid'])
                        lower_area_id.push(data['sfid'])
                    })
                }
            }
        }
        // console.log(`Town id -------> ${temp_town_arr}`);
        temp_town_arr = await sort.removeDuplicates(temp_town_arr);

        return temp_town_arr;

    }catch(e){
        console.log(`Error In Get Branch From Lower Area ------> ${e}`);
    }
}
async function getTeamFromLobDivision(id) {
    try {
        let lob_data = {};
        let division_data = {};
        let fields = ['*']
        let tableName = SF_TEAM_TABLE_NAME;
        var offset = '0', limit = '100';
        let WhereClouse = [];
        WhereClouse.push({ "fieldName": "sfid", "fieldValue": id });
        let team_sql = qry.SelectAllQry(fields, tableName, WhereClouse, offset, limit, ' order by createddate desc');
        console.log("team sql ------> ", team_sql);
        let team_sql_res = await client.query(team_sql);
        if (team_sql_res.rows.length > 0) {
            console.log(`lob__c----->${team_sql_res.rows[0]['lob__c']}`)
            lob_data=team_sql_res.rows[0]['lob__c'];
            division_data=team_sql_res.rows[0]['division__c'];

            return [lob_data, division_data];

        } else {
            return [];
        }

    } catch (e) {
        console.log('Error In Team Day Type Wise Function ------>', e);
    }
}

async function getPicklistSfid(object_name__c,field_name__c,picklistname) {
    try {
        let picklist_sql=`SELECT sfid,name FROM salesforce.picklist__c where object_name__c='${object_name__c}' AND field_name__c='${field_name__c}' AND name='${picklistname}'`
        console.log("picklist_sql---->",picklist_sql)
        let picklist_result =await client.query(picklist_sql);
        let value=null;
        if(picklist_result.rows.length>0){
            value=picklist_result.rows[0].sfid
        }
        return value
    } catch (e) {
        console.log('Error In getPicklistSfid Function ------>', e);
    }
}

async function getMultiPicklistSfids(object_name__c,field_name__c) {
    try {
        let picklist_sql=`SELECT sfid,name FROM salesforce.picklist__c where object_name__c='${object_name__c}' AND field_name__c='${field_name__c}'`
        console.log("picklist_sql---->",picklist_sql)
        let picklist_result =await client.query(picklist_sql);
        let map={};
        if(picklist_result.rows.length>0){
            picklist_result.rows.map((obj)=>{
                map[`${obj.name}`]=obj.sfid
            })
        }
        return map
    } catch (e) {
        console.log('Error In getPicklistSfid Function ------>', e);
    }
}

async function getTerritoryFromLowerArea(team_id){
    try{
        // console.log(`Area Id Inside Get Territory From Upper Area Fn -- ${area_id}`);
        let territory_code = []
        let upper_area1_temp = [];
        let upper_area2_temp = [];
        let territory_id;
        let territory_picklist = `SELECT * FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_PICKLIST_TABLE_NAME} WHERE field_name__c = 'Area_Type__c' and object_name__c = 'Area1__c' and name = 'Territory'`
        let territory_picklist_res = await client.query(territory_picklist);
        console.log("picklist",territory_picklist_res.rows[0]['sfid']);
        if(territory_picklist_res.rows.length > 0){
            territory_id = territory_picklist_res.rows[0]['sfid']
        }else{
            territory_id = 'a01C7000000WYw3IAG'
        }

        let team_terr_sql = `Select * from salesforce.team_territory_mapping__c WHERE team_member_id__c ='${team_id}'`
        let team_terr_sql_res = await client.query(team_terr_sql);

        for (let i = 0; i < team_terr_sql_res.rows.length; i++){
            let area_terr_pick =` Select * from salesforce.area1__c WHERE sfid = '${team_terr_sql_res.rows[i]['territory_code__c']}'` //Branch
            let area_terr_pick_res = await client.query(area_terr_pick);

            if (area_terr_pick_res.rows[0]['area_type__c'] == territory_id) {
                territory_code.push(team_terr_sql_res.rows[i]['territory_code__c'])
                console.log("++++++++++++", territory_code)
            }
            else {
                let upper_area_sql = `SELECT * FROM salesforce.area1__c where parent_code__c IN ('${team_terr_sql_res.rows[i]['territory_code__c']}')`  //ASM
                let upper_area_res = await client.query(upper_area_sql);
                console.log(`Upper Area Sql -----> ${upper_area_res.rows}`);
                upper_area_res.rows.map((upper_area1) => {
                    upper_area1_temp.push(upper_area1['sfid'])
                    console.log("Hurray temp", upper_area1_temp)
                })

                if (upper_area_res.rows[0]['area_type__c'] == territory_id) {
                    for (let j = 0; j < upper_area_res.rows.length; j++) {
                        territory_code.push(upper_area_res.rows[j]['sfid'])
                    }

                } else {
                    let sql_level = ` Select * from salesforce.area1__c WHERE parent_code__c IN ('${upper_area1_temp.join("','")}')` //SE
                    let sql_level_res = await client.query(sql_level);
                    console.log("Hello", sql_level)
                    sql_level_res.rows.map((upper_area2) => {
                        upper_area2_temp.push(upper_area2['sfid'])
                        console.log("Hurray temp2", upper_area2_temp)
                    })

                    if (sql_level_res.rows[0]['area_type__c'] == territory_id) {
                        for (let z = 0; z < sql_level_res.rows.length; z++) {
                            territory_code.push(sql_level_res.rows[z]['sfid'])
                            console.log("SE")
                        }
                    } else {
                        console.log("Finally");
                        let sql_level2 = ` Select * from salesforce.area1__c WHERE parent_code__c IN ('${upper_area2_temp.join("','")}')` //Territory
                        let sql_level_res2 = await client.query(sql_level2);
                        console.log("INSIDE Jannat")
                        if (sql_level_res2.rows[0]['area_type__c'] == territory_id) {
                            for (let k = 0; k < sql_level_res2.rows.length; k++) {
                                territory_code.push(sql_level_res2.rows[k]['sfid'])
                                console.log("Territory",territory_code)
                            }
                        }


                    }

                }


            }

        
        }

        // let upper_area_arr;
        // area_id = area_id;
        // for(let i = 0 ; i < 6 ; i++){
        //     let upper_area_sql = `SELECT * FROM salesforce.area1__c where sfid = '${area_id}'`
        //     console.log(`Upper Area Sql -----> ${upper_area_sql}`);
        //     let upper_area_res = await client.query(upper_area_sql);
        //     if(upper_area_res.rows.length > 0){
        //         if(upper_area_res.rows[0]['area_level__c'] == territory_level_id){
        //             //upper_area_arr.push(upper_area_res.rows[0]['sfid'])
        //             upper_area_arr = upper_area_res.rows[0]['sfid']
        //             break;
        //         }else{
        //             area_id = upper_area_res.rows[0]['parent_code__c'];
        //         }
        //     }
        // }
        // console.log(`territory id -------> ${upper_area_arr}`);
        return territory_code;

    }catch(e){
        console.log(`Error In Get Territory From Lower Area ------> ${e}`);
    }
}
async function getTeamRegionWiseData2(data){
    try{
        let nation_sfid_arr = [];
        let nation_data = [];
        let zone_sfid_arr = [];
        let zone_data = [];
        let region_sfid_arr = [];
        let region_data = [];
        let branch_sfid_arr = [];
        let branch_data = [];
        let territory_sfid_arr = [];
        let territory_data = [];
        let asm_data = [];
        let asm_sfid_arr = [];
        let account_sfid_arr=[];

        // let grid_data = [];
        // let grid_sfid_arr = [];
        let se_data = [];
        let se_sfid_arr = [];

        let fields = [`${SF_AREA_1_TABLE_NAME}.*, a1.name__c as parent_code_name`]
        let tableName = SF_AREA_1_TABLE_NAME;
        var offset='0', limit='1000';
        let WhereClouse = [];
        //for picklist value a050w000002jazRAAQ:area_level__c
        let area_picklist_sql = `SELECT * FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_PICKLIST_TABLE_NAME} 
        WHERE ${SF_PICKLIST_TABLE_NAME}.object_name__c='Area1__c' AND ${SF_PICKLIST_TABLE_NAME}.field_name__c= 'Area_Level__c'AND ${SF_PICKLIST_TABLE_NAME}.name= '1';`;
        let area_picklist = await client.query(area_picklist_sql);
        area_picklist = area_picklist.rows[0]['sfid'];
        //console.log("???????????????????????????????????",area_picklist)
        //WhereClouse.push({ "fieldName": "town_code__c", "fieldValue": grid_sql_res.rows});  
        WhereClouse.push({ "fieldName": `${SF_TEAM_TERRITORY_MAPPING_TABLE_NAME}.team_member_id__c`, "fieldValue": data});  
        WhereClouse.push({ "fieldName": "area1__c.area_level__c", "fieldValue":area_picklist });//a050w000002jazRAAQ
//*********************************************** nation Starts Here *********************************************************/
        let joins = [
            
            {
                "type": "LEFT",
                "table_name": `${SF_TEAM_TERRITORY_MAPPING_TABLE_NAME}`,
                "p_table_field": `${SF_AREA_1_TABLE_NAME}.sfid`,
                "s_table_field": `${SF_TEAM_TERRITORY_MAPPING_TABLE_NAME}.territory_code__c `
            },
            {
                "type": "LEFT",
                "table_name": `${SF_AREA_1_TABLE_NAME} as a1`,
                "p_table_field": `${SF_AREA_1_TABLE_NAME}.parent_code__c`,
                "s_table_field": `a1.sfid `
            }
        ]

        let nation_sql = qry.fetchAllWithJoinQry(fields, tableName, joins, WhereClouse, offset, limit,` order by ${SF_AREA_1_TABLE_NAME}.createddate desc`);
        console.log("nation_sql ::::::::::::::::::::::::::: ------> ", nation_sql);
        let nation_sql_res = await client.query(nation_sql);

        if(nation_sql_res.rows.length > 0){
            nation_sql_res.rows.map((sfids)=> {
                nation_sfid_arr.push(sfids['sfid'])  
                //console.log('lllllllllllllllllllllllllll',nation_sfid_arr)
                let obj = {
                    nation_id: sfids['sfid'],
                    nation_name: sfids['name__c'],
                    nation_parent_code: sfids['parent_code__c'],
                    nation_parent_code_name: sfids['parent_code_name'],
                    nation_code :sfids['name']
                };
                nation_data.push(obj); 

            });
        }else{
            nation_sfid_arr.push(nation_sql_res['parent_code__c']);
            nation_sfid_arr = await sort.removeDuplicates(nation_sfid_arr);
            console.log('inside if condition nation idss ----->',nation_sfid_arr);
        }
//*****************************************************  nation Ends Here   *******************************************************/
    if(nation_sql_res.rows.length > 0 || (data.length > 0 && data != undefined)){
            
         let fields = [`${SF_AREA_1_TABLE_NAME}.*, a2.name__c as parent_code_name`]
         let tableName = SF_AREA_1_TABLE_NAME;
          var offset='0', limit='100';
         let WhereClouse = [];
        //for picklist value a050w000002jazWAAQ:area_level__c
        let area_picklist_sql = `SELECT * FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_PICKLIST_TABLE_NAME} 
        WHERE ${SF_PICKLIST_TABLE_NAME}.object_name__c='Area1__c' AND ${SF_PICKLIST_TABLE_NAME}.field_name__c= 'Area_Level__c'AND ${SF_PICKLIST_TABLE_NAME}.name= '2';`;
        let area_picklist = await client.query(area_picklist_sql);
        area_picklist = area_picklist.rows[0]['sfid'];
         if(nation_sfid_arr.length>0 && nation_sfid_arr[0]!=undefined)  {
             WhereClouse.push({ "fieldName": `${SF_AREA_1_TABLE_NAME}.parent_code__c`, "fieldValue": nation_sfid_arr ,"type": 'IN' });  
             WhereClouse.push({ "fieldName":   `${SF_AREA_1_TABLE_NAME}.area_level__c`, "fieldValue": area_picklist});//a050w000002jazWAAQ
            //WhereClouse.push({ "fieldName": `${SF_TEAM_TERRITORY_MAPPING_TABLE_NAME}.team_member_id__c`, "fieldValue": data});  
        }else{
                     
            //WhereClouse.push({ "fieldName": `${SF_AREA_1_TABLE_NAME}.parent_code__c`, "fieldValue": nation_sfid_arr  });  
            WhereClouse.push({ "fieldName":   `${SF_AREA_1_TABLE_NAME}.area_level__c`, "fieldValue": area_picklist});
            WhereClouse.push({ "fieldName": `${SF_TEAM_TERRITORY_MAPPING_TABLE_NAME}.team_member_id__c`, "fieldValue": data});  
                }
    let joins = [
        {
            "type": "LEFT",
            "table_name": `${SF_TEAM_TERRITORY_MAPPING_TABLE_NAME}`,
            "p_table_field": `${SF_AREA_1_TABLE_NAME}.sfid`,
            "s_table_field": `${SF_TEAM_TERRITORY_MAPPING_TABLE_NAME}.territory_code__c `
        },
        {
            "type": "LEFT",
            "table_name": `${SF_AREA_1_TABLE_NAME} as a2`,
            "p_table_field": `${SF_AREA_1_TABLE_NAME}.parent_code__c`,
            "s_table_field": `a2.sfid `
        },
        
    ]

    let zone_sql = qry.fetchAllWithJoinQry(fields, tableName, joins, WhereClouse, offset, limit,` order by ${SF_AREA_1_TABLE_NAME}.createddate desc`);
    console.log("zone_sql:::::::::::::::::::::::::::::::::::::::::::::::::::::  ------> ", zone_sql);
    let zone_sql_res = await client.query(zone_sql);

    if(zone_sql_res.rows.length > 0){
        zone_sql_res.rows.map((sfids)=> {
            zone_sfid_arr.push(sfids['sfid'])   
            let obj = {
                zone_id: sfids['sfid'],
                    zone_name: sfids['name__c'],
                    zone_parent_code: sfids['parent_code__c'],
                    zone_parent_code_name: sfids['parent_code_name'],
                    zone_code :sfids['name']
            };
            zone_data.push(obj); 

        });
    }else{
        zone_sfid_arr.push(zone_sql_res['parent_code__c']);
        zone_sfid_arr = await sort.removeDuplicates(zone_sfid_arr);
        console.log('inside if condition zone idss ----->',zone_sfid_arr);
    }
//******************************************* zone Ends Here  **************************************************/
            if(zone_sql_res.rows.length > 0 || (data.length > 0 && data != undefined)){

                let fields = [`${SF_AREA_1_TABLE_NAME}.*, a3.name__c as parent_code_name`]
                let tableName = SF_AREA_1_TABLE_NAME;
                var offset='0', limit='100';
                let WhereClouse = [];
                //for picklist value a050w000002jazbAAA:area_level__c
                let area_picklist_sql = `SELECT * FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_PICKLIST_TABLE_NAME} 
                 WHERE ${SF_PICKLIST_TABLE_NAME}.object_name__c='Area1__c' AND ${SF_PICKLIST_TABLE_NAME}.field_name__c= 'Area_Level__c'AND ${SF_PICKLIST_TABLE_NAME}.name= '3';`;
                let area_picklist = await client.query(area_picklist_sql);
                area_picklist = area_picklist.rows[0]['sfid'];
            if(zone_sfid_arr.length>0 &&   zone_sfid_arr[0]!=undefined )  {
                //WhereClouse.push({ "fieldName": "town_code__c", "fieldValue": grid_sql_res.rows});  
                WhereClouse.push({ "fieldName": `${SF_AREA_1_TABLE_NAME}.parent_code__c`, "fieldValue": zone_sfid_arr ,"type": 'IN' });  
                WhereClouse.push({ "fieldName": `${SF_AREA_1_TABLE_NAME}.area_level__c`, "fieldValue": area_picklist});//a050w000002jazbAAA
            }else{
                     
                //WhereClouse.push({ "fieldName": `${SF_AREA_1_TABLE_NAME}.parent_code__c`, "fieldValue": nation_sfid_arr  });  
                WhereClouse.push({ "fieldName":   `${SF_AREA_1_TABLE_NAME}.area_level__c`, "fieldValue": area_picklist});
                WhereClouse.push({ "fieldName": `${SF_TEAM_TERRITORY_MAPPING_TABLE_NAME}.team_member_id__c`, "fieldValue": data});  
                    }
                let joins = [
                    
                    {
                        "type": "LEFT",
                        "table_name": `${SF_TEAM_TERRITORY_MAPPING_TABLE_NAME}`,
                        "p_table_field": `${SF_AREA_1_TABLE_NAME}.sfid`,
                        "s_table_field": `${SF_TEAM_TERRITORY_MAPPING_TABLE_NAME}.territory_code__c `
                    },
                    {
                        "type": "LEFT",
                        "table_name": `${SF_AREA_1_TABLE_NAME} as a3`,
                        "p_table_field": `${SF_AREA_1_TABLE_NAME}.parent_code__c`,
                        "s_table_field": `a3.sfid `
                    }
                ]

                let region_sql = qry.fetchAllWithJoinQry(fields, tableName, joins, WhereClouse, offset, limit,` order by ${SF_AREA_1_TABLE_NAME}.createddate desc`);
                console.log(" region_sql ------> ", region_sql);
                let region_sql_res = await client.query(region_sql);

                if(region_sql_res.rows.length > 0){
                    region_sql_res.rows.map((sfids)=> {
                        region_sfid_arr.push(sfids['sfid'])  
                        let obj = {
                            region_id: sfids['sfid'],
                            region_name: sfids['name__c'],
                            region_parent_code: sfids['parent_code__c'],
                            region_parent_code_name : sfids['parent_code_name'],
                            region_code : sfids['name']
                        };
                        region_data.push(obj);   

                    });
                }else{
                    region_sfid_arr.push(region_sql_res['parent_code__c']);
                    region_sfid_arr = await sort.removeDuplicates(region_sfid_arr);
                    console.log('inside if condition region idss ----->',region_sfid_arr);
                }
//********************************** region Ends Here ****************************************************/
                if(region_sql_res.rows.length > 0 || (data.length > 0 && data != undefined)){
                    
                    let fields = [`${SF_AREA_1_TABLE_NAME}.*, ${SF_PICKLIST_TABLE_NAME}.name as area_type_name, a4.name__c as parent_code_name`]
                    let tableName = SF_AREA_1_TABLE_NAME;
                    var offset='0', limit='100';
                    let WhereClouse = [];
                    //for picklist value a050w000002jazqAAA:area_level__c
                    let area_picklist_sql = `SELECT * FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_PICKLIST_TABLE_NAME} 
                    WHERE ${SF_PICKLIST_TABLE_NAME}.object_name__c='Area1__c' AND ${SF_PICKLIST_TABLE_NAME}.field_name__c= 'Area_Level__c'AND ${SF_PICKLIST_TABLE_NAME}.name= '4';`;
                    let area_picklist = await client.query(area_picklist_sql);
                         area_picklist = area_picklist.rows[0]['sfid'];
                    if(region_sfid_arr.length>0 &&   region_sfid_arr[0]!=undefined )  {
                    WhereClouse.push({ "fieldName": "area1__c.parent_code__c", "fieldValue": region_sfid_arr ,"type": 'IN' });  
                    WhereClouse.push({ "fieldName": "area1__c.area_level__c", "fieldValue": area_picklist});//a050w000002jazqAAA
                    }else{
                    WhereClouse.push({ "fieldName": `${SF_TEAM_TERRITORY_MAPPING_TABLE_NAME}.team_member_id__c`, "fieldValue": data}); 
                    WhereClouse.push({ "fieldName": "area1__c.area_level__c", "fieldValue": area_picklist});
                    }
                    let joins = [
                        {
                            "type": "LEFT",
                            "table_name": `${SF_TEAM_TERRITORY_MAPPING_TABLE_NAME}`,
                            "p_table_field": `${SF_AREA_1_TABLE_NAME}.sfid`,
                            "s_table_field": `${SF_TEAM_TERRITORY_MAPPING_TABLE_NAME}.territory_code__c `
                        },
                        {
                            "type": "LEFT",
                            "table_name": `${SF_PICKLIST_TABLE_NAME}`,
                            "p_table_field": `${SF_AREA_1_TABLE_NAME}.area_type__c`,
                            "s_table_field": `${SF_PICKLIST_TABLE_NAME}.sfid`
                        },
                        {
                            "type": "LEFT",
                            "table_name": `${SF_AREA_1_TABLE_NAME} as a4`,
                            "p_table_field": `${SF_AREA_1_TABLE_NAME}.parent_code__c`,
                            "s_table_field": `a4.sfid `
                        }
                    ]

                    let branch_sql = qry.fetchAllWithJoinQry(fields, tableName, joins, WhereClouse, offset, limit,`order by ${SF_AREA_1_TABLE_NAME}.createddate desc`);
                    console.log("branch_sql  ------> ", branch_sql);
                    let branch_sql_res = await client.query(branch_sql);

                    if(branch_sql_res.rows.length > 0){
                        branch_sql_res.rows.map((sfids)=> {
                            branch_sfid_arr.push(sfids['sfid'])  
                            let obj = {
                                branch_id: sfids['sfid'],
                                branch_name: sfids['name__c'],
                                branch_parent_code: sfids['parent_code__c'],
                                branch_parent_code_name: sfids['parent_code_name'],
                                branch_code :sfids['name']
                            };
                            branch_data.push(obj); 
                        });
                    }else{
                        branch_sfid_arr.push(branch_sql_res['parent_code__c']);
                        branch_sfid_arr = await sort.removeDuplicates(branch_sfid_arr);
                        console.log('inside if condition branch idss ----->',branch_sfid_arr);
                    }
 //*********************************** branch ENds Here ***************************************************************/ 
                    if(branch_sql_res.rows.length > 0 || (data.length > 0 && data != undefined)){
                        let fields = [`${SF_AREA_1_TABLE_NAME}.*, ${SF_PICKLIST_TABLE_NAME}.name as area_type_name, a5.name__c as parent_code_name,${SF_AREA_1_TABLE_NAME}.parent_code__c`]
                        let tableName = SF_AREA_1_TABLE_NAME;
                        var offset='0', limit='100';
                        let WhereClouse = [];
                        //for picklist value a050w000002jb05AAA:area_level__c
                        let area_picklist_sql = `SELECT * FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_PICKLIST_TABLE_NAME} 
                        WHERE ${SF_PICKLIST_TABLE_NAME}.object_name__c='Area1__c' AND ${SF_PICKLIST_TABLE_NAME}.field_name__c= 'Area_Level__c'AND ${SF_PICKLIST_TABLE_NAME}.name= '5';`;
                        let area_picklist = await client.query(area_picklist_sql);
                            area_picklist = area_picklist.rows[0]['sfid'];
                    if(branch_sfid_arr.length>0 &&   branch_sfid_arr[0]!=undefined )  {
                        WhereClouse.push({ "fieldName": "area1__c.parent_code__c", "fieldValue": branch_sfid_arr ,"type": 'IN' });  
                        WhereClouse.push({ "fieldName": "area1__c.area_level__c", "fieldValue": area_picklist});//a050w000002jb05AAA
                    }else{
                        WhereClouse.push({ "fieldName": "area1__c.area_level__c", "fieldValue": area_picklist});
                        WhereClouse.push({ "fieldName": `${SF_TEAM_TERRITORY_MAPPING_TABLE_NAME}.team_member_id__c`, "fieldValue": data}); 


                    }
                        let joins = [
                           
                            {
                                "type": "LEFT",
                                "table_name": `${SF_TEAM_TERRITORY_MAPPING_TABLE_NAME}`,
                                "p_table_field": `${SF_AREA_1_TABLE_NAME}.sfid`,
                                "s_table_field": `${SF_TEAM_TERRITORY_MAPPING_TABLE_NAME}.territory_code__c `
                            },
                            {
                                "type": "LEFT",
                                "table_name": `${SF_PICKLIST_TABLE_NAME}`,
                                "p_table_field": `${SF_AREA_1_TABLE_NAME}.area_type__c`,
                                "s_table_field": `${SF_PICKLIST_TABLE_NAME}.sfid `
                            },
                            {
                                "type": "LEFT",
                                "table_name": `${SF_AREA_1_TABLE_NAME} as a5`,
                                "p_table_field": `${SF_AREA_1_TABLE_NAME}.parent_code__c`,
                                "s_table_field": `a5.sfid `
                            }
                        ]

                        let asm_sql = qry.fetchAllWithJoinQry(fields, tableName, joins, WhereClouse, offset, limit,`order by ${SF_AREA_1_TABLE_NAME}.createddate desc`);
                        console.log("asm_sql  ------> ", asm_sql);
                        let asm_sql_res = await client.query(asm_sql);
                        let asm_uniquedata= await getUniqueValuesFromArrayOfObject(asm_sql_res.rows)

                        if(asm_sql_res.rows.length > 0){
                            asm_uniquedata.map((sfids)=> {
                                asm_sfid_arr.push(sfids['sfid'])  
                                let obj = {
                                asm_id: sfids['sfid'],
                                asm_name: sfids['name__c'],
                                asm_parent_code: sfids['parent_code__c'],
                                asm_parent_code_name: sfids['parent_code_name'],
                                area_type : sfids['area_type__c'],
                                area_type_name : sfids['area_type_name'],
                                area_code: sfids['name']
                                };
                                asm_data.push(obj);

                            });
                    

                        }else{
                            asm_sfid_arr.push(asm_uniquedata['parent_code__c']);
                            asm_sfid_arr = await sort.removeDuplicates(asm_sfid_arr);
                            console.log('inside if condition asm idss ----->',asm_sfid_arr);
                        }

 //*********************************** ASM Ends Here ***************************************************************/                   
                    if(asm_sql_res.rows.length > 0 || (data.length > 0 && data != undefined)){
                        //let fields = [`${SF_TEAM_TERRITORY_MAPPING_TABLE_NAME}.*, ${SF_PICKLIST_TABLE_NAME}.name as area_type_name, a6.name__c as parent_code_name , p1.name as territory_type_name`]
                        let fields = [`${SF_AREA_1_TABLE_NAME}.*,${SF_TEAM_TERRITORY_MAPPING_TABLE_NAME}.territory_type__c as parent_code_name,${SF_PICKLIST_TABLE_NAME}.name as area_type_name, a6.name__c as parent_code_name , p1.name as territory_type_name`]
                        let tableName = SF_AREA_1_TABLE_NAME;
                        var offset='0', limit='100';
                        let WhereClouse = [];
                        //for picklist value a050w000002jb0AAAQ:area_level__c
                        let area_picklist_sql = `SELECT * FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_PICKLIST_TABLE_NAME} 
                        WHERE ${SF_PICKLIST_TABLE_NAME}.object_name__c='Area1__c' AND ${SF_PICKLIST_TABLE_NAME}.field_name__c= 'Area_Level__c'AND ${SF_PICKLIST_TABLE_NAME}.name= '6';`;
                        let area_picklist = await client.query(area_picklist_sql);
                           area_picklist = area_picklist.rows[0]['sfid'];
                    if(asm_sfid_arr.length>0 &&   asm_sfid_arr[0]!=undefined )  {
                        WhereClouse.push({ "fieldName": `${SF_AREA_1_TABLE_NAME}.parent_code__c`, "fieldValue": asm_sfid_arr ,"type": 'IN' });  
                        WhereClouse.push({ "fieldName": `${SF_AREA_1_TABLE_NAME}.area_level__c`, "fieldValue": area_picklist});//a050w000002jb0AAAQ
                    }else{
                        WhereClouse.push({ "fieldName": `${SF_AREA_1_TABLE_NAME}.area_level__c`, "fieldValue": area_picklist});
                        WhereClouse.push({ "fieldName": `${SF_TEAM_TERRITORY_MAPPING_TABLE_NAME}.team_member_id__c`, "fieldValue": data}); 
                        
                    }
                        let joins = [
                            {
                                "type": "LEFT",
                                "table_name": `${SF_TEAM_TERRITORY_MAPPING_TABLE_NAME}`,
                                "p_table_field": `${SF_AREA_1_TABLE_NAME}.sfid`,
                                "s_table_field": `${SF_TEAM_TERRITORY_MAPPING_TABLE_NAME}.territory_code__c `
                            },
                            {
                                "type": "LEFT",
                                "table_name": `${SF_PICKLIST_TABLE_NAME}`,
                                "p_table_field": `${SF_AREA_1_TABLE_NAME}.area_type__c`,
                                "s_table_field": `${SF_PICKLIST_TABLE_NAME}.sfid `
                            },
                            {
                                "type": "LEFT",
                                "table_name": `${SF_AREA_1_TABLE_NAME} as a6`,
                                "p_table_field": `${SF_AREA_1_TABLE_NAME}.parent_code__c`,
                                "s_table_field": `a6.sfid `
                            },
                            {
                                "type": "LEFT",
                                "table_name": `${SF_PICKLIST_TABLE_NAME} as p1`,
                                "p_table_field": `${SF_AREA_1_TABLE_NAME}.territory_type__c`,
                                "s_table_field": `p1.sfid `
                            },
                        ]

                        let se_sql = qry.fetchAllWithJoinQry(fields, tableName, joins, WhereClouse, offset, limit,`order by ${SF_TEAM_TERRITORY_MAPPING_TABLE_NAME}.createddate desc`);
                        console.log("se sql ------> ", se_sql);
                        let se_sql_res = await client.query(se_sql);

                        if(se_sql_res.rows.length > 0){
                            se_sql_res.rows.map((sfids)=> {
                                se_sfid_arr.push(sfids['sfid'])  
                                let obj = {
                                    se_id: sfids['sfid'],
                                    se_name: sfids['name__c'],
                                    se_parent_code: sfids['parent_code__c'],
                                    se_parent_code_name: sfids['parent_code_name'],
                                    se_code: sfids['name'],
                                    se_type : sfids['area_type__c'],
                                    se_type_name : sfids['territory_type_name']
                                };
                                se_data.push(obj);
                                
                            });
                        }else{
                            se_sfid_arr.push(se_sql_res['parent_code__c']);
                            se_sfid_arr = await sort.removeDuplicates(se_sfid_arr);
                            console.log('inside if condition se idss ----->',se_sfid_arr);
                        }
 //*********************************** SE Ends Here ***************************************************************/                   
                        if(se_sql_res.rows.length > 0 || (data.length > 0 && data != undefined)){
                            let fields = [`${SF_AREA_1_TABLE_NAME}.*, ${SF_AREA_1_TABLE_NAME}.name__c as parent_code_name, p1.name as territory_type_name,${SF_PICKLIST_TABLE_NAME}.name as area_type_name`]
                            //let fields = [`${SF_TEAM_TERRITORY_MAPPING_TABLE_NAME}.*, ${SF_PICKLIST_TABLE_NAME}.name as area_type_name, ${SF_AREA_1_TABLE_NAME}.name__c as parent_code_name , p1.name as territory_type_name`]
                            let tableName = SF_AREA_1_TABLE_NAME;
                            var offset='0', limit='100';
                            let WhereClouse = [];
                            //for picklist value a050w000003RLNfAAO:area_level__c
                            let area_picklist_sql = `SELECT * FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_PICKLIST_TABLE_NAME} 
                            WHERE ${SF_PICKLIST_TABLE_NAME}.object_name__c='Area1__c' AND ${SF_PICKLIST_TABLE_NAME}.field_name__c= 'Area_Level__c'AND ${SF_PICKLIST_TABLE_NAME}.name= '7';`;
                            let area_picklist = await client.query(area_picklist_sql);
                           area_picklist = area_picklist.rows[0]['sfid'];
                         if(se_sfid_arr.length>0 &&   se_sfid_arr[0]!=undefined )  {
                            WhereClouse.push({ "fieldName": "area1__c.parent_code__c", "fieldValue": se_sfid_arr ,"type": 'IN' });  
                            WhereClouse.push({ "fieldName": "area1__c.area_level__c", "fieldValue": area_picklist});
                         }else{
                            //WhereClouse.push({ "fieldName": "area1__c.area_level__c", "fieldValue": 'a050w000003RLNfAAO'});
                            WhereClouse.push({ "fieldName": `${SF_TEAM_TERRITORY_MAPPING_TABLE_NAME}.team_member_id__c`, "fieldValue": data}); 
                            WhereClouse.push({ "fieldName": "area1__c.area_level__c", "fieldValue": 'area_picklist'});//a050w000003RLNfAAO

                         }
    
                            let joins = [
                                {
                                    "type": "LEFT",
                                    "table_name": `${SF_TEAM_TERRITORY_MAPPING_TABLE_NAME}`,
                                    "p_table_field": `${SF_AREA_1_TABLE_NAME}.sfid`,
                                    "s_table_field": `${SF_TEAM_TERRITORY_MAPPING_TABLE_NAME}.territory_code__c `
                                },
                                {
                                    "type": "LEFT",
                                    "table_name": `${SF_PICKLIST_TABLE_NAME}`,
                                    "p_table_field": `${SF_AREA_1_TABLE_NAME}.area_type__c`,
                                    "s_table_field": `${SF_PICKLIST_TABLE_NAME}.sfid `
                                },
                                {
                                    "type": "LEFT",
                                    "table_name": `${SF_AREA_1_TABLE_NAME} as a5`,
                                    "p_table_field": `${SF_AREA_1_TABLE_NAME}.parent_code__c`,
                                    "s_table_field": `a5.sfid `
                                },
                                {
                                    "type": "LEFT",
                                    "table_name": `${SF_PICKLIST_TABLE_NAME} as p1`,
                                    "p_table_field": `${SF_AREA_1_TABLE_NAME}.territory_type__c`,
                                    "s_table_field": `p1.sfid `
                                },
                            ]
    
                            let territory_sql = qry.fetchAllWithJoinQry(fields, tableName, joins, WhereClouse, offset, limit,`order by ${SF_AREA_1_TABLE_NAME}.createddate desc`);
                            console.log("Territory sql ------> ", territory_sql);
                            let territory_sql_res = await client.query(territory_sql);
                            let territory_uniquedata= await getUniqueValuesFromArrayOfObject(territory_sql_res.rows)
                            
                            if(territory_uniquedata.length > 0){
                                territory_uniquedata.map((sfids)=> {
                                    territory_sfid_arr.push(sfids['sfid'])  
                                                 
                                });
                            }
                        let dealer_id = await getPicklistSfid('Account_Type_Activation__c','Account_Type__c','Dealer')
                        let account_sql=`select sfid from salesforce.account where territory__c IN ('${territory_sfid_arr.join("','")}') and account_type__c='${dealer_id}'`
                        let account_sql_res = await client.query(account_sql);
                        if(account_sql_res.rows.length > 0){
                            account_sql_res.rows.map((sfids)=> {
                                account_sfid_arr.push(sfids['sfid'])  
                                             
                            });
                        }
                        console.log("account sql::::::::::::::::::::::::",account_sql)

                        //console.log("party data::::::::::::::::::::::::",account_sfid_arr)

                    }
                    return account_sfid_arr
                    }
                }
            }
        }
        }
    }catch(e){
        console.log('Error in Get Region Wise Data Function ----->',e);
    }
}

async function getTerritoryFromAnyArea(area_id){
    try{
        let territory_level_pl = await getPicklistSfid('Area1__c','Area_Level__c','7')
        console.log(`Picklist Name --->${territory_level_pl} -- LEVEL 7`);
        let lower_area_arr = []
        let lower_area_id = []
        let temp_territory_arr = []

        lower_area_arr.push(area_id)
        lower_area_id.push(area_id)

        let area_check_sql = `SELECT * FROM salesforce.area1__c where sfid = '${area_id}'`
        let result =  await client.query(area_check_sql)

        if(result.rows.length > 0 && result.rows[0]['area_level__c'] == territory_level_pl){
            temp_territory_arr.push(area_id)
        }else{
            for(let i = 0 ; i < 10 ; i++){
                let lower_area_sql ;
    
                lower_area_sql = `SELECT * 
                                FROM salesforce.area1__c 
                                where parent_code__c IN ('${lower_area_id.join("','")}') 
                                ` 
                console.log(`Lower Area Sql In ${i}th iteration ---> ${lower_area_sql}`);
                let lower_area_res = await client.query(lower_area_sql);
                if(lower_area_res.rows.length > 0){
                    if(lower_area_res.rows[0]['area_level__c'] == territory_level_pl){
                        lower_area_res.rows.map((data) => {
                            lower_area_arr.push(data['sfid'])
                            temp_territory_arr.push(data['sfid'])
                        })
                        break;
                    }else{
                        lower_area_id.length = 0
                        lower_area_res.rows.map((data) => {
                            lower_area_arr.push(data['sfid'])
                            lower_area_id.push(data['sfid'])
                        })
                    }
                }
            }
        }

        // console.log(`Town id -------> ${temp_town_arr}`);
        temp_territory_arr = await sort.removeDuplicates(temp_territory_arr);
        console.log(`------------------------------ ${temp_territory_arr}`);
        return temp_territory_arr;

    }catch(e){
        console.log(`Error In Function Get Territory From Any Area ----> ${e}`);
    }
}

async function getTeamsDetail(team_ids){
    try{
        final_team_arr = [];
        let team_data_sql = `SELECT team__c.*,picklist__c.name as user_level 
                            FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_TEAM_TABLE_NAME} 
                            left join salesforce.picklist__c on ${SF_TEAM_TABLE_NAME}.hierarchylevel__c = picklist__c.sfid
                            where team__c.sfid IN ('${team_ids.join("','")}')`
        let result = await client.query(team_data_sql);
        if(result.rows.length > 0){
            result.rows.map((data) => {
                let level = data['user_level'].split("-")
                //console.log(`Level ---> ${level}`);
                let obj = {
                    id: data['sfid'],
                    name: data['team_member_name__c'],
                    designation : data['designation__c'],
                    level:Number(level[1])
                  };
                  final_team_arr.push(obj); 
            })
            return final_team_arr
        }else{
            return final_team_arr;
        }
    }catch(e){
        console.log(`Error In Get Teams Detail Function -----> ${e}`);
    }
}

async function getWorkingHoursAndDistance(visit_id){
    try{
        if (visit_id.length > 20) {
            let visit_sql = `select * from salesforce.visit__c where pg_id__c = '${visit_id}'`
            visit_sql_res = await client.query(visit_sql);
        } else {
            let visit_sql = `select * from salesforce.visit__c where sfid = '${visit_id}'`
            visit_sql_res = await client.query(visit_sql);
        }

        if (visit_sql_res.rows[0]['grid__c'] != null) {
            let visit_grid_sfid = []
            visit_sql_res.rows.map((visit_grid) => {
                // town_sfid_arr.push(sfids['sfid'])
                let id_obj = {
                    visit_id: visit_grid['sfid'],
                    grid_id: visit_grid['grid__c']
                }
                // console.log("OBJECT+++",id_obj)
                visit_grid_sfid.push(id_obj)
                console.log("VVVVVVVVVVVVVVVVVV", visit_grid_sfid)

            })

            let team_id = visit_sql_res.rows[0]['emp_id__c']
            let date = dtUtil.todayDate()
            // console.log(team,date_new);
            const collection = db.collection(process.env.MONGODB_COLLECTION);
            // const visit_records = await collection.find({visit_id__c:`${visit_id}`}).toArray();
            let where_mongodb = {
                team_id: `${team_id}`,
                date: `${date}`
            }
            let visit_records = await collection.find(
                where_mongodb
            ).toArray();
            console.log('Data____________+++++++++ -', visit_records);

            // console.log('Visit data from MongoDB::::::::', visit_records[0]['path'])
            if (visit_records.length > 0) {
                let count = 0;
                let actual_distance_for_first_visit = 0;
                let first_vist_temp = []
                // console.log("FIRST VISIT_iD",first_visit)
                // console.log("SECOND VISIT_iD",second_visit)

                visit_records[0]['path'].map((data) => {

                    if (visit_grid_sfid.length <= 2) {
                        console.log("1st Visit")
                        if (visit_records[0]['path'][count]['visit_id'] == visit_grid_sfid[0]['visit_id'] && visit_records[0]['path'][count]['grid_id'] == visit_grid_sfid[0]['grid_id'] && visit_records[0]['path'][count]['visit_id'] != null) {
                            let lat_long_obj = {
                                lat: visit_records[0]['path'][count]['lat'],
                                long: visit_records[0]['path'][count]['long']
                            }
                            first_vist_temp.push(lat_long_obj)
                        }
                    }
                    count++;
                })
                // console.log("First_visit array", first_vist_temp)

                let inner_count = 0;
                first_vist_temp.map((coordinates) => {
                    if (inner_count == first_vist_temp.length - 1) {
                        actual_distance_for_first_visit += 0;
                    } else {
                        let lat1 = first_vist_temp[inner_count]['lat'];
                        let lon1 = first_vist_temp[inner_count]['long'];
                        let lat2 = first_vist_temp[inner_count + 1]['lat']
                        let lon2 = first_vist_temp[inner_count + 1]['long']
                        if ((lat1 == lat2) && (lon1 == lon2) || (lat1 == null || lat1 == "") || (lat2 == null || lat2 == "") || (lon1 == null || lon1 == "") || (lon2 == null || lon2 == "")) {
                            actual_distance_for_first_visit += 0;
                        }
                        else {
                            // console.log(`function values ----->lat1 ${lat1}  long1 ${lon1}  lat2 ${lat2}  long2 ${lon2}`);
                            // console.log('infunction');
                            let radlat1 = Math.PI * lat1 / 180;
                            let radlat2 = Math.PI * lat2 / 180;
                            let theta = lon1 - lon2;
                            let radtheta = Math.PI * theta / 180;
                            let dist = Math.sin(radlat1) * Math.sin(radlat2) + Math.cos(radlat1) * Math.cos(radlat2) * Math.cos(radtheta);
                            if (dist > 1) {
                                dist = 1;
                            }
                            dist = Math.acos(dist);
                            dist = dist * 180 / Math.PI;
                            dist = dist * 60 * 1.1515;
                            dist = dist * 1.609344
                            actual_distance_for_first_visit += dist;
                        }
                    }
                    // console.log("LENGTHHH",count)
                    inner_count++;
                })
                // console.log("ACTUAL_DIST for visit1", actual_distance_for_first_visit);


                let initial_time = visit_records[0]['path'][0]['time']
                let final_time = visit_records[0]['path'][visit_records[0]['path'].length - 1]['time']

                let Hours = dtUtil.getTotalHourMinute(date, initial_time, date, final_time);
                // console.log("+++++++++++++++++++", Hours[1]);
                return [actual_distance_for_first_visit,Hours[1]]

            }
        } else {
            let visit_grid_sfid = []
            visit_sql_res.rows.map((visit_grid) => {
                // town_sfid_arr.push(sfids['sfid'])
                let id_obj = {
                    visit_id: visit_grid['sfid'],
                }
                // console.log("OBJECT+++",id_obj)
                visit_grid_sfid.push(id_obj)
                console.log("VVVVVVVVVVVVVVVVVV", visit_grid_sfid)

            })

            let team_id = visit_sql_res.rows[0]['emp_id__c']
            let date = dtUtil.todayDate()
            // console.log(team,date_new);
            const collection = db.collection(process.env.MONGODB_COLLECTION);
            // const visit_records = await collection.find({visit_id__c:`${visit_id}`}).toArray();
            let where_mongodb = {
                team_id: `${team_id}`,
                date: `${date}`
            }
            let visit_records = await collection.find(
                where_mongodb
            ).toArray();
            console.log('Data____________+++++++++ -', visit_records);

            // console.log('Visit data from MongoDB::::::::', visit_records[0]['path'])
          
            if (visit_records.length > 0) {
                let count = 0;
                let actual_distance_for_first_visit = 0;
                let first_vist_temp = []
                // console.log("FIRST VISIT_iD",first_visit)
                // console.log("SECOND VISIT_iD",second_visit)

                visit_records[0]['path'].map((data) => {

                    if (visit_grid_sfid.length <= 2) {
                        // console.log("1st Visit")
                        if (visit_records[0]['path'][count]['visit_id'] == visit_grid_sfid[0]['visit_id']) {
                            let lat_long_obj = {
                                lat: visit_records[0]['path'][count]['lat'],
                                long: visit_records[0]['path'][count]['long']
                            }
                            first_vist_temp.push(lat_long_obj)
                        }
                    }
                    count++;
                })
                console.log("First_visit array", first_vist_temp)

                let inner_count = 0;
                first_vist_temp.map((coordinates) => {
                    if (inner_count == first_vist_temp.length - 1) {
                        actual_distance_for_first_visit += 0;
                    } else {
                        let lat1 = first_vist_temp[inner_count]['lat'];
                        let lon1 = first_vist_temp[inner_count]['long'];
                        let lat2 = first_vist_temp[inner_count + 1]['lat']
                        let lon2 = first_vist_temp[inner_count + 1]['long']
                        if ((lat1 == lat2) && (lon1 == lon2) || (lat1 == null || lat1 == "") || (lat2 == null || lat2 == "") || (lon1 == null || lon1 == "") || (lon2 == null || lon2 == "")) {
                            actual_distance_for_first_visit += 0;
                        }
                        else {
                            // console.log(`function values ----->lat1 ${lat1}  long1 ${lon1}  lat2 ${lat2}  long2 ${lon2}`);
                            // console.log('infunction');
                            let radlat1 = Math.PI * lat1 / 180;
                            let radlat2 = Math.PI * lat2 / 180;
                            let theta = lon1 - lon2;
                            let radtheta = Math.PI * theta / 180;
                            let dist = Math.sin(radlat1) * Math.sin(radlat2) + Math.cos(radlat1) * Math.cos(radlat2) * Math.cos(radtheta);
                            if (dist > 1) {
                                dist = 1;
                            }
                            dist = Math.acos(dist);
                            dist = dist * 180 / Math.PI;
                            dist = dist * 60 * 1.1515;
                            dist = dist * 1.609344
                            actual_distance_for_first_visit += dist;
                        }
                    }
                    // console.log("LENGTHHH",count)
                    inner_count++;
                })
                // console.log("ACTUAL_DIST for visit1", actual_distance_for_first_visit);


                let initial_time = visit_records[0]['path'][0]['time']
                let final_time = visit_records[0]['path'][visit_records[0]['path'].length - 1]['time']

                let Hours = dtUtil.getTotalHourMinute(date, initial_time, date, final_time);
                // console.log("+++++++++++++++++++", Hours[1]);
                return [actual_distance_for_first_visit,Hours[1]]

            }
        }

    }catch(e){
        console.log(`Error In get Working Hours And Distance -----> ${e}`);
    }
}

async function getTownFromTerritory(territory_ids){
    try{
        let town_ids = [];
        //let town_type_id = await getPicklistSfid('Area2__c','Town_Type__c','HQ Town')
        let town_sql = `SELECT * 
        FROM salesforce.area1__c 
        where parent_code__c IN ('${territory_ids.join("','")}')
        `
        let result = await client.query(town_sql);
        if(result.rows.length > 0){
            result.rows.map((ids) => {
                town_ids.push(ids['sfid'])
            })
            return town_ids
        }else{
            return town_ids
        }
    }catch(e){
        console.log(`Error In Get Town From Territory Function -----> ${e}`);
    }
}

async function townMatchFunction(team_id,geo_hqtown_id){
    try{
        let town_area_type_id = await getPicklistSfid('Area2__c','Area_Type__c','Town')
        //let town_type_id = await getPicklistSfid('Area2__c','Town_Type__c','HQ Town')

        let territory_from_team = `SELECT territory_code__c FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_TEAM_TERRITORY_MAPPING_TABLE_NAME}
                                    where team_member_id__c = '${team_id}'`
        let territory_from_team_result = await client.query(territory_from_team);
        if(territory_from_team_result.rows.length > 0){
            let territory_ids = [];
            for(let i = 0 ; i <  territory_from_team_result.rows.length ; i++){
                let territory_id = await getTerritoryFromAnyArea(territory_from_team_result.rows[i]['territory_code__c'])
                territory_ids = [...territory_ids,...territory_id]
            }
            console.log(`Territory ids ----> ${territory_ids}`);
            //console.log(`SELECT * FROM salesforce.area1__c where sfid IN ('${territory_ids.join("','")}')`);
            let town_from_territory = await getTownFromTerritory(territory_ids);
            //console.log(`Town IDs on Territory----> ${town_from_territory}`);
            //console.log(`SELECT * FROM salesforce.area1__c where sfid IN ('${town_from_territory.join("','")}')`);
            let town_from_geo_town = [];
            if(geo_hqtown_id){
                let town_from_geotown = `SELECT sfid from ${process.env.TABLE_SCHEMA_NAME}.${SF_AREA_1_TABLE_NAME}
                                        where geographical_town__c = '${geo_hqtown_id}' 
                                        and area_type__c = '${town_area_type_id}'
                                        and sfid IN ('${town_from_territory.join("','")}')`
                console.log(`Town From Geo Town SQL ----> ${town_from_geotown}`);
                let town_from_geotown_res = await client.query(town_from_geotown)
                if(town_from_geotown_res.rows.length > 0){
                    town_from_geotown_res.rows.map((ids) => {
                        town_from_geo_town.push(ids['sfid'])
                    })
                    return town_from_geo_town
                }else{
                    return 'no_data'
                }
            }else{
                return 'no_data'
            }

            // if(town_from_territory.length > 0 && town_from_geo_town.length > 0){
            //     console.log(`Town Ids From Territory ----> ${town_from_territory}`);
            //     console.log(`Town Ids From Geo Town ----> ${town_from_geo_town}`);
            //     let matched_element = await getMatchedElementFromTwoArray(town_from_territory,town_from_geo_town)
            //     if(matched_element.length > 0){
            //         return matched_element
            //     }else{
            //         return 'no_data'
            //     }
            // }else{
            //     return 'no_data'
            // }
        }else{
            //territory from team else
            return 'no_data'
        }
    }catch(e){
        console.log(`Error In Town Match Function -----> ${e}`);
    }
}

async function getMatchedElementFromTwoArray(array1,array2){
    try{
        let map = {}, result = [], i;
        for (i = 0; i < array1.length; ++i) {
            map[array1[i]] = 1;
        }

        for (i = 0; i < array2.length; ++i) {
            if (map[array2[i]] === 1) {
                result.push(array2[i]);

                // avoid returning a value twice if it appears twice in array 2
                map[array2[i]] = 0;
            }
        }

        return result;

    }catch(e){
        console.log(`Error In Get Matched Element From Two Array -----> ${e}`);
    }
}

async function getNewGridName(town_id,half_grid_name__c,half_name){
    try{
        let grid_name_count;
        let words;
        let last_number;
        let new_grid_name__c;
        let new_name__c;
        let grid_count_sql = `SELECT * FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_AREA_1_TABLE_NAME} WHERE parent_code__c = '${town_id}' and sfid is not null order by createddate desc offset 0 limit 1`
        console.log(`Grid Count SQl -----> ${grid_count_sql}`);
        let grid_count_res = await client.query(grid_count_sql)
        if(grid_count_res.rows.length > 0){
            grid_name_count = grid_count_res.rows[0]['name__c']
            words = grid_name_count.split('-');
            //last_number = words[words.length - 1] 
            // var x = Number("1000")
            last_number = Number(words[words.length - 1])
            console.log(`Last Number ---> ${last_number}`);
            //console.log(`Last Number I----> ${last_number} & Grid Name Count I----> ${grid_name_count}`);   
        }else{
            //grid_name_count = 0
            last_number = 0
        } 
        let final_number = last_number + 1
        console.log(`Last Number ----> ${last_number} & Final Number ----> ${final_number}`);
        if(final_number.toString().length == 1){
            new_grid_name__c = `${half_grid_name__c}-0${final_number}`
            new_name__c = `${half_name}0${final_number}`
        }else{
            new_grid_name__c = `${half_grid_name__c}-${final_number}`
            new_name__c = `${half_name}${final_number}`
        }
        console.log(`New Grid Name  ----> ${new_grid_name__c} & New Name ----> ${new_name__c}`);
        return{new_grid_name__c,new_name__c}

    }catch(e){
        console.log(`Error In Get New Grid Name Function -----> ${e}`);
    }
} 

async function setEncodedLatLongPath(lat_long_array){
    try{
        //let a=[ [[lat,long],[lat,long],[lat,long]] ,[[lat,long]] ]
        let accumlated_array = []
        //console.log(`Array Sended To Function ---> ${lat_long_array}`);
        let chunk_array = _.chunk(lat_long_array, 20);
        //console.log(`Chunk Array---> ${chunk_array}`);
        for(let i = 0 ; i < chunk_array.length ; i++){
            console.log(`The Chunked Array Length ---> ${chunk_array[i].length}`);
            //let encoded_string ;
            let origin = chunk_array[i][0]
            let destination = chunk_array[i][chunk_array[i].length - 1]
            let withoutFirstAndLast = chunk_array[i].slice(1, -1);
            console.log(`=============> ${withoutFirstAndLast}`);
            let encoded_string = polyline.encode(withoutFirstAndLast);
            // if(withoutFirstAndLast.length > 0){
            //     encoded_string = polyline.encode(chunk_array[i])
            // }else{
            //     encoded_string = polyline.encode(withoutFirstAndLast);
            // }
            let response = await getDirection(origin,destination,encoded_string)
            accumlated_array.push(response)
        }
        return accumlated_array

        // let origin = lat_long_array[0]
        // let destination = lat_long_array[lat_long_array.length - 1]
        // let withoutFirstAndLast = lat_long_array.slice(1, -1);
        // let encoded_string = polyline.encode(withoutFirstAndLast);
        // let response = await getDirection(origin,destination,encoded_string)
        // return response

    }catch(e){
        console.log(`Error In Set Encoded Lat Long Path --------> ${e}`);
    }
}

async function getPathLatLongInArrayFormat(teamid,date){
    try{
        const collection = db.collection(process.env.MONGODB_COLLECTION);
        const visit_records = await collection.find(
            {
                team_id : `${teamid}`, 
                date : `${date}`
            }).toArray();
        //console.log('Data -',visit_records[0]['path']);
        if(visit_records.length > 0){
            let newArray = visit_records[0]['path'].filter(function (data) {
                if (!data.lat || !data.long) {
                    return false
                } else {
                    return true
                }
                // return  (data.lat !="" || data.long !="")
            });
            //console.log(newArray);
            const clean_array = newArray.map(item => {
                delete item.grid_id
                delete item.visit_id
                delete item.path_count
                delete item.time
                delete item.remarks__c
                return item
            })
            let lat_long_array = clean_array.map(obj => Object.values(obj));
            //console.log(`Test array ----> ${test}`);
            return lat_long_array
        }else{
            return [];
        }
        
    }catch(e){
        console.log(`Error In Get Path Lat Long In Array Format -----> ${e}`);
    }
}

//origin=41.43206,-81.38992
//Encoded polylines must be prefixed with enc: and followed by a colon (:). For example: waypoints=enc:gfo}EtohhU:.
//&waypoints=via:enc:${waypoints}:
async function getDirection(origin,destination,waypoints) {
    console.log(`Origin ----> ${origin} , Destination -----> ${destination} , WayPoints ---->${waypoints}`);
    if(waypoints){
        if (origin && destination) {
            return rp(`https://maps.googleapis.com/maps/api/directions/json?origin=${origin[0]},${origin[1]}&destination=${destination[0]},${destination[1]}&waypoints=via:enc:${waypoints}:&key=${process.env.GOOGLE_API_KEY}`)
                .then(async function (data) {
                    data = JSON.parse(data);
                    
                    let polyline_data = data.routes[0].overview_polyline.points
                    
                    return polyline_data
                })
                .catch(function (err) {
                    console.log(err);
                    // Crawling failed...
                });
        } else {
            return 'N/A';
        }
    }else{
        if (origin && destination) {
            return rp(`https://maps.googleapis.com/maps/api/directions/json?origin=${origin[0]},${origin[1]}&destination=${destination[0]},${destination[1]}&key=${process.env.GOOGLE_API_KEY}`)
                .then(async function (data) {
                    data = JSON.parse(data);
                    // console.log('Data in Get distance API ::::', data.rows[0].elements[0])
                    let polyline_data = data.routes[0].overview_polyline.points
                    return polyline_data
                })
                .catch(function (err) {
                    console.log(err);
                    // Crawling failed...
                });
        } else {
            return 'N/A';
        }
    }
    
}

async function getTerritoryFromAnyAreaOrTeam(id,type){
    try{
        //@Notice Removed the Secondary area logic from this api 30-03-2022
        let temp_area_arr = [];
        if (type == 'team') {
            let area_id_arr = [];
            let team_id_arr = [];
            let temp_arr = [];
            console.log(`ID----> ${id}`)
            let lob_sql = `SELECT * FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_TEAM_TABLE_NAME} where sfid = '${id}'`
            let lob_sql_res = await client.query(lob_sql);
            let lob_value = lob_sql_res.rows[0]['lob__c']
            let territory_from_team_sql = `SELECT * 
                                    FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_TEAM_TERRITORY_MAPPING_TABLE_NAME} 
                                    where team_member_id__c = '${id}'`
            //console.log(territory_from_team_sql)
            let result = await client.query(territory_from_team_sql);
            result.rows.map((ids) => {
                area_id_arr.push(ids['territory_code__c'])
            })
            for(let i = 0 ; i < area_id_arr.length ; i++){
                let temp_data = await getTerritoryFromAnyArea(area_id_arr[i])
                temp_area_arr = [...temp_area_arr,...temp_data]
            }
            return temp_area_arr     
        }
        if(type == 'area'){
            let temp_data = await getTerritoryFromAnyArea(id)
            return temp_data
        }
        
    }catch(e){
        console.log(`Error In Function Get Subordinate On Area -----> ${e}`);
    }
}

async function getPathLatLongForSpecificVisitGrid(id,type,date){
    try{
        let visit_records;
        const collection = db.collection(process.env.MONGODB_COLLECTION);
        let ids = [];
        ids.push(id)
        if(type == 'visit'){
            visit_records = await collection.aggregate([
                // Get just the docs that contain a Visit Ids Which IS In An Array
                    {$match: {"path.visit_id": {$in : ids}}},
                    {$project: {
                        team_id: '$team_id',
                        team_name: '$team_name',
                        date: '$date',
                        active__c: '$active__c',
                        path: {$filter: {
                            input: "$path",
                            as: "path",
                            cond: {$in: ["$$path.visit_id", ids]}
                        }}
                    }}
                ]).toArray();
        }
        if(type == 'grid'){
            visit_records = await collection.aggregate([
                // Get just the docs that contain a Visit Ids Which IS In An Array
                    {$match: {"path.grid_id": {$in : ids}}},
                    {$project: {
                        team_id: '$team_id',
                        team_name: '$team_name',
                        date: '$date',
                        active__c: '$active__c',
                        path: {$filter: {
                            input: "$path",
                            as: "path",
                            cond: {$in: ["$$path.grid_id", ids]}
                        }}
                    }}
                ]).toArray();
        }
        if(type == 'day' && date != 'no_date'){
            visit_records = await collection.find(
                {
                    team_id : `${id}`, 
                    date : `${date}`
                }).toArray();
            }
        
        //console.log('Data -',visit_records[0]['path']);
        if(visit_records.length > 0){
            let newArray = visit_records[0]['path'].filter(function (data) {
                if (!data.lat || !data.long) {
                    return false
                } else {
                    return true
                }
                // return  (data.lat !="" || data.long !="")
            });
            //console.log(newArray);
            const clean_array = newArray.map(item => {
                delete item.grid_id
                delete item.visit_id
                delete item.path_count
                delete item.time
                delete item.remarks__c
                return item
            })
            let lat_long_array = clean_array.map(obj => Object.values(obj));
            //console.log(`Test array ----> ${test}`);
            return lat_long_array
        }else{
            return [];
        }
        
    }catch(e){
        console.log(`Error In Get Path Lat Long In Array Format -----> ${e}`);
    }
}

async function getDistanceOnWaypoints(lat_long_array){
    try{
        //let a=[ [[lat,long],[lat,long],[lat,long]] ,[[lat,long]] ]
        // let accumlated_array = []
        // //console.log(`Array Sended To Function ---> ${lat_long_array}`);
        // let chunk_array = _.chunk(lat_long_array, 20);
        // //console.log(`Chunk Array---> ${chunk_array}`);
        // for(let i = 0 ; i < chunk_array.length ; i++){
        //     console.log(`The Chunked Array Length ---> ${chunk_array[i].length}`);
        //     //let encoded_string ;
        //     let origin = chunk_array[i][0]
        //     let destination = chunk_array[i][chunk_array[i].length - 1]
        //     let withoutFirstAndLast = chunk_array[i].slice(1, -1);
        //     console.log(`=============> ${withoutFirstAndLast}`);
        //     let encoded_string = polyline.encode(withoutFirstAndLast);
        //     // if(withoutFirstAndLast.length > 0){
        //     //     encoded_string = polyline.encode(chunk_array[i])
        //     // }else{
        //     //     encoded_string = polyline.encode(withoutFirstAndLast);
        //     // }
        //     let response = await getDirectionForDistance(origin,destination,encoded_string)
        //     accumlated_array.push(response)
        // }
        // return accumlated_array

        let accumlated_array = []
        let total_distance= 0
        // //console.log(`Array Sended To Function ---> ${lat_long_array}`);
        let chunk_array = _.chunk(lat_long_array, 20);
        
        let j = 0;
        for(let i = 0; i < chunk_array.length; i++){
            let origin;
            if(i == 0){
                // i = 0
                origin = chunk_array[0][0]
            }else{
                // i = i-1
                origin = chunk_array[i-1][chunk_array[i-1].length - 1]
            }
            //console.log(`Value Of i ----> ${i}   Value Of J ----> ${j}`);
            let destination = chunk_array[j][chunk_array[j].length - 1]
            let withoutFirstAndLast = chunk_array[j].slice(1, -1);
            let encoded_string = polyline.encode(withoutFirstAndLast);
            let response = await getDirectionForDistance(origin,destination,encoded_string)
            //console.log(`Without first and last ----> ${withoutFirstAndLast}`);
            //console.log(`Response ---> ${response}`);
            //accumlated_array.push(response)
            if(response){
                total_distance += response
                accumlated_array.push(response/1000)
            }
            j++
        }
        console.log(`Accumlated Array --> ${accumlated_array}`);
        return total_distance
        // let origin = lat_long_array[0]
        // let destination = lat_long_array[lat_long_array.length - 1]
        // let withoutFirstAndLast = lat_long_array.slice(1, -1);
        // let encoded_string = polyline.encode(withoutFirstAndLast);
        // let response = await getDirection(origin,destination,encoded_string)
        // return response

    }catch(e){
        console.log(`Error In Set Encoded Lat Long Path --------> ${e}`);
    }
}

async function getDirectionForDistance(origin,destination,waypoints) {
    //console.log(`Origin ----> ${origin} , Destination -----> ${destination} , WayPoints ---->${waypoints}`);
    if(waypoints){
        if (origin && destination) {
            //console.log(`https://maps.googleapis.com/maps/api/directions/json?origin=${origin[0]},${origin[1]}&destination=${destination[0]},${destination[1]}&waypoints=enc:${waypoints}:&key=${process.env.GOOGLE_API_KEY}`);
            return rp(`https://maps.googleapis.com/maps/api/directions/json?origin=${origin[0]},${origin[1]}&destination=${destination[0]},${destination[1]}&waypoints=via:enc:${waypoints}:&key=${process.env.GOOGLE_API_KEY}`)
                .then(async function (data) {
                    data = JSON.parse(data);
                    
                    let waypoints_distance = data.routes[0].legs[0].distance.value 
                    return waypoints_distance
                })
                .catch(function (err) {
                    console.log(err);
                    // Crawling failed...
                });
        } else {
            return 'N/A';
        }
    }else{
        if (origin && destination) {
            return rp(`https://maps.googleapis.com/maps/api/directions/json?origin=${origin[0]},${origin[1]}&destination=${destination[0]},${destination[1]}&key=${process.env.GOOGLE_API_KEY}`)
                .then(async function (data) {
                    data = JSON.parse(data);
                    // console.log('Data in Get distance API ::::', data.rows[0].elements[0])
                    let waypoints_distance = data.routes[0].legs[0].distance.value
                    return waypoints_distance
                })
                .catch(function (err) {
                    console.log(err);
                    // Crawling failed...
                });
        } else {
            return 'N/A';
        }
    }
    
}

async function getVisitData(visit_id){
    try{
        let visit_detail;
        if(visit_id.length > 20){
            visit_detail = `SELECT * FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_VISIT_TABLE_NAME} where pg_id__c = '${visit_id}'`
        }else{
            visit_detail = `SELECT * FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_VISIT_TABLE_NAME} where sfid = '${visit_id}'`
        }
        let visit_detail_res = await client.query(visit_detail)
        if(visit_detail_res.rows.length > 0){
            //For Visit Time Calculations
            let pjp_id = visit_detail_res.rows[0]['pjp_header__c']
            let emp_id__c = visit_detail_res.rows[0]['emp_id__c']
            let visit_date__c = visit_detail_res.rows[0]['visit_date__c']
            let total_visit_time = 0
            let total_visit_distance = 0
            let leads_genrated = 0
            let visit_start = await dtUtil.convertDatePickerTimeToMySQLTime(visit_detail_res.rows[0]['check_in_time__c'])
            let visit_end = await dtUtil.convertDatePickerTimeToMySQLTime(visit_detail_res.rows[0]['check_out_time__c'])
            let visit_time = await dtUtil.getTotalHourMinuteV2(visit_start, visit_end)
            let pause_time = 0;
            console.log(`Visit STart Time ---> ${visit_start} Visit End Time ---> ${visit_end}  Visit Time ---> ${visit_time}`);
            if(visit_detail_res.rows[0]['pause_in_time__c'] && visit_detail_res.rows[0]['pause_out_time__c']){
                let pause_start = await dtUtil.convertDatePickerTimeToMySQLTime(visit_detail_res.rows[0]['pause_in_time__c'])
                let pause_end = await dtUtil.convertDatePickerTimeToMySQLTime(visit_detail_res.rows[0]['pause_out_time__c'])
                pause_time = await dtUtil.getTotalHourMinuteV2(pause_start, pause_end)
            }
            if(pause_time > 0){
                total_visit_time = Math.round(visit_time[0] -  pause_time[0])
            }else{
                total_visit_time = Math.round(visit_time[0])
            }
            //For Visit Distance Calculation
            let total_visit_distance_fn = await getPathLatLongForSpecificVisitGrid(visit_detail_res.rows[0]['sfid'],'visit','no_date')
            //let total_visit_distance_value = await GetDistanceLengthTurf(total_visit_distance_fn)
            let total_visit_distance_value = await getDistanceHarvsine(total_visit_distance_fn)
            console.log(`Distance --------------------------------------${total_visit_distance_value}`);
            //let total_visit_distance_value = await getDistanceOnWaypoints(total_visit_distance_fn)
            if(total_visit_distance_value > 0){
                total_visit_distance = Number(total_visit_distance_value.toFixed(2))
            }
            //For Leads Created During Visits
            let leads_on_visit = `SELECT * FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_LEAD_TABLE_NAME} where visit__c = '${visit_id}'`
            let lead_res = await client.query(leads_on_visit)
            if(lead_res.rows.length > 0){
                leads_genrated = lead_res.rows.length
            }
            return {total_visit_time,total_visit_distance,leads_genrated,pjp_id,emp_id__c,visit_date__c}
        }
    }catch(e){
        console.log(`Error In Get Visit Data ------> ${e}`);
    }
}

async function getPjpData(pjp_id){
    try{
        let pjp_sql = `SELECT * FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_PICKLIST_TABLE_NAME} where sfid = '${pjp_id}'`
        let pjp_res = await client.query(pjp_sql)
        if(pjp_res.rows.length > 0){
            let total_day_hr = pjp_res.rows[0]['total_hours__c']
            let total_work_hr = pjp_res.rows[0]['visit_working_hour__c']
            let total_day_dist = pjp_res.rows[0]['total_distance__c']
            let total_work_dist = pjp_res.rows[0]['total_working_distance__c']

            return {total_day_hr,total_work_hr,total_day_dist,total_work_dist}
        }
    }catch(e){
        console.log(`Error In Get Pjp Data Function ---->  ${e}`);
    }
}

async function getTotalAttendenceData(team_id,date){
    try{
        let attendence = `SELECT * FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_ATTENDANCE_TABLE_NAME} where emp_id__c = '${team_id}' and attendance_date__c = '${date}'`
        console.log(`Attendence SQL ---> ${attendence}`);
        let attendence_res = await client.query(attendence)
        if(attendence_res.rows.length > 0 ){
            let total_day_time = 0;
            let pause_time = 0
            let total_dist = 0
            let start_time = await dtUtil.convertDatePickerTimeToMySQLTime(attendence_res.rows[0]['start_time__c']) 
            let end_time = await dtUtil.convertDatePickerTimeToMySQLTime(attendence_res.rows[0]['end_time__c'])  
            // if(end_time){
            //     end_time = dtUtil.todayDatetime();
            // }
            let total_time = await dtUtil.getTotalHourMinuteV2(start_time, end_time)
            total_day_time = total_time[0]

            if(attendence_res.rows[0]['pause_start__c'] && attendence_res.rows[0]['pause_end__c']){
                let pause_start = await dtUtil.convertDatePickerTimeToMySQLTime(attendence_res.rows[0]['pause_start__c']) 
                let pause_end = await dtUtil.convertDatePickerTimeToMySQLTime(attendence_res.rows[0]['pause_end__c']) 
                let pause = await dtUtil.getTotalHourMinuteV2(pause_start, pause_end)
                pause_time = pause[0]
            }
            total_day_time = total_day_time - pause_time

            let total_day_travelled = await getPathLatLongForSpecificVisitGrid(team_id,'day',date)
            //let total_day_distance_value = await getDistanceOnWaypoints(total_day_travelled)
            //let total_day_distance_value = await GetDistanceLengthTurf(total_day_travelled)
            let total_day_distance_value = await getDistanceHarvsine(total_day_travelled)

            if(total_day_distance_value > 0){
                total_dist = Number(total_day_distance_value.toFixed(2))
            }

            return {total_day_time,total_dist}
        }
    }catch(e){
        console.log(`Error In Get Attendence Data ---> ${e}`);
    }
}

async function getTotalWorkingData(team_id,date){
    try{
        //For Visit Time Part
        let visit_working_time = 0
        let working_start_time;
        let working_end_time;
        let completed_visit_status = await getPicklistSfid('Visit__c','Visit_Status__c','Completed')
        let all_visit_sql = `SELECT * FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_VISIT_TABLE_NAME} WHERE emp_id__c = '${team_id}' and visit_date__c = '${dtUtil.ISOtoLocal(date)}' and visit_status__c = '${completed_visit_status}' order by check_in_time__c asc`
        console.log(`Sql ---> ${all_visit_sql}`);
        let all_visit_res = await client.query(all_visit_sql)
        if(all_visit_res.rows.length > 0){
            let pause_time = 0;
            working_start_time = await dtUtil.convertDatePickerTimeToMySQLTime(all_visit_res.rows[0]['check_in_time__c'])
            working_end_time = await dtUtil.convertDatePickerTimeToMySQLTime(all_visit_res.rows[all_visit_res.rows.length - 1]['check_out_time__c'])
            visit_working_time = await dtUtil.getTotalHourMinuteV2(working_start_time, working_end_time)
            
            for(let i = 0 ; i<all_visit_res.rows.length ; i++){
                if(all_visit_res.rows[i]['pause_in_time__c'] && all_visit_res.rows[i]['pause_out_time__c']){
                    let pause_start_time = await dtUtil.convertDatePickerTimeToMySQLTime(all_visit_res.rows[i]['pause_in_time__c'])
                    let pause_end_time = await dtUtil.convertDatePickerTimeToMySQLTime(all_visit_res.rows[i]['pause_out_time__c'])
                    let pause = await dtUtil.getTotalHourMinuteV2(pause_start_time, pause_end_time)
                    pause_time = pause[0]
                }
            }
            if(pause_time > 0){
                visit_working_time = visit_working_time[0] - pause_time
            }else{
                visit_working_time = visit_working_time[0]
            }
        }
        let visit_working_dist = 0
        all_visit_res.rows.map((data) => {
            if(data['distance_travelled_during_scouting__c']){
                visit_working_dist += data['distance_travelled_during_scouting__c']
            }
        })

        return {visit_working_time,visit_working_dist,working_start_time,working_end_time}

    }catch(e){
        console.log(`Error In Get Total Working Data ----> ${e}`);
    }
}

async function updateVisitDataInPjp(pjp_id,emp_id__c,visit_date__c){
    try{
        let pjp_fieldValue = [];
        let pjp_whereClouse = [];
        let pjp_tablename = SF_PJP_TABLE_NAME
        let visit_data = await getTotalWorkingData(emp_id__c,visit_date__c)
        console.log(`Visit Working Distance ------ ${visit_data.visit_working_dist}`);
        if (visit_data.visit_working_time) {
            pjp_fieldValue.push({ "field": "visit_working_hour__c", "value": visit_data.visit_working_time })
        }
        if (visit_data.visit_working_dist) {
            pjp_fieldValue.push({ "field": "total_working_distance__c", "value": visit_data.visit_working_dist})
        }
        pjp_whereClouse.push({ "field": "sfid", "value": pjp_id });
        if (pjp_fieldValue.length > 0) {
            let pjp_update = await qry.updateRecord(pjp_tablename, pjp_fieldValue, pjp_whereClouse)
            if (pjp_update.success) {
                return 'success'
            } else {
                return 'failed'
            }
        }
    }catch(e){
        console.log(`Error In Update Visit Data In PJP ---->${e}`);
    }
}

async function updateAttendanceDataInPjp(emp_id__c,date){
    try{
        let pjp_fieldValue = [];
        let pjp_whereClouse = [];
        let pjp_tablename = SF_PJP_TABLE_NAME
        let day_data = await getTotalAttendenceData(emp_id__c,date)

        if (day_data.total_day_time) {
            pjp_fieldValue.push({ "field": "total_hours__c", "value": day_data.total_day_time })
        }
        if (day_data.total_dist) {
            pjp_fieldValue.push({ "field": "total_distance__c", "value": day_data.total_dist})
        }
        pjp_whereClouse.push({ "field": "emp_id__c", "value": emp_id__c });
        pjp_whereClouse.push({ "field": "pjp_date__c", "value": date });
        if (pjp_fieldValue.length > 0) {
            let pjp_update = await qry.updateRecord(pjp_tablename, pjp_fieldValue, pjp_whereClouse)
            console.log(`Pjp Update ---> ${pjp_update}`);
            if (pjp_update.success) {
                return 'success'
            } else {
                return 'failed'
            }
        }
    }catch(e){
        console.log(`Error In Update Visit Data In PJP ---->${e}`);
    }
}

async function GetDistanceLengthTurf(data) {
    distance_of_travel = 0;
    for(let i = 0 ; i< data.length ; i++){
        if(data[i+1]){
            //console.log(`ith ---> ${data[i]} && i+1 ---> ${data[i+1]}`);
            let originPoint = turf.point(data[i])
            let destinationPoint = turf.point(data[i+1])
            distance_of_travel += turf.distance(originPoint, destinationPoint);
        }
    }
    return distance_of_travel.toFixed(2)
}

async function getDistanceHarvsine(data){
    try{
        let value = 0;
        let coords = []
        if (data && data.length) {
            for (let i = 1; i < data.length; i++) {
                value += haversine(data[i - 1], data[i])
            }
            value = value / 1000
        }
        return value;
    }catch(e){
        console.log(`Error In Get Distance Harvsine ---> ${e}`);
    }
}

function haversine(coords1, coords2) {
    const R = 6371e3; // metres
    const φ1 = coords1[0] * Math.PI/180; // φ, λ in radians
    const φ2 = coords2[0] * Math.PI/180;
    const Δφ = (coords2[0]-coords1[0]) * Math.PI/180;
    const Δλ = (coords2[1]-coords1[1]) * Math.PI/180;
  
    const a = Math.sin(Δφ/2) * Math.sin(Δφ/2) +
            Math.cos(φ1) * Math.cos(φ2) *
            Math.sin(Δλ/2) * Math.sin(Δλ/2);
    const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
  
    return R * c; // in metres
  }
  
// function GetDistanceLength(params) {
//     let value = 0;
//     let coords = []

//     if (params && params.length) {
//         for (let i = 1; i < params.length; i++) {
//             value += haversine(params[i - 1], params[i])
//         }

//         value = value / 1000
//     }

//     return value;
// }

async function getAnyAreaFromArea(team_id,area_level){
    try{
        //console.log(`Parameter --> ${team_id} ${area_level}`);
        let team_sql = `SELECT * FROM salesforce.team__c where sfid = '${team_id}'`
        let team_sql_res = await client.query(team_sql);
        let lob_value = team_sql_res.rows[0]['lob__c']
        //console.log(`Lob Value ----> ${lob_value}`);
        let territory_from_team_sql = `SELECT * FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_TEAM_TERRITORY_MAPPING_TABLE_NAME} where team_member_id__c = '${team_id}' and lob__c = '${lob_value}'`
        let result = await client.query(territory_from_team_sql);
        let territory_code = []
        if(result.rows.length > 0){
            result.rows.map((data) => {
                territory_code.push(data['territory_code__c'])
            })
            let area_level_sql = `SELECT picklist__c.name as area_level FROM salesforce.area1__c
                             left join salesforce.picklist__c on area1__c.area_level__c = picklist__c.sfid
                             where area1__c.sfid = '${territory_code[0]}'`
            //console.log(`Area Level Sql ---> ${area_level_sql}`);
            let area_level_value = await client.query(area_level_sql)
            console.log(`Values ----> ${area_level_value.rows[0]['area_level']}`);
            if(area_level > area_level_value.rows[0]['area_level']){
                //For Finding Lower Area
                console.log(`For Lower Area Case`);
                let lower_area_arr = []
                let lower_area_id = []
                //lower_area_id.push(territory_code)
                lower_area_id = [...territory_code]
                let area_level_id = await getPicklistSfid('Area1__c', 'Area_Level__c', `${area_level}`)
                for(let i = 0 ; i < 6 ; i++){
                    let lower_area_sql = `SELECT * FROM salesforce.area1__c where parent_code__c IN ('${lower_area_id.join("','")}')`
                    console.log(`Lower Area Sql In ${i}th iteration ---> ${lower_area_sql}`);
                    let lower_area_res = await client.query(lower_area_sql);
                    if(lower_area_res.rows.length > 0){
                        if(lower_area_res.rows[0]['area_level__c'] == area_level_id){
                            lower_area_id.length = 0
                            lower_area_res.rows.map((data) => {
                                lower_area_arr.push(data['sfid'])
                                lower_area_id.push(data['sfid'])
                            })
                            break;
                        }else{
                            lower_area_id.length = 0
                            lower_area_res.rows.map((data) => {
                                lower_area_arr.push(data['sfid'])
                                lower_area_id.push(data['sfid'])
                            })
                        }
                    }
                }
                return lower_area_id
            }
            if(area_level < area_level_value.rows[0]['area_level']){
                //For Finding Upper Area
                console.log(`For Upper Area Case`);
                let upper_area_arr = []
                let area_id = [...territory_code];
                //console.log(`Area Id ---> ${area_id}`);
                let area_level_id = await getPicklistSfid('Area1__c', 'Area_Level__c', `${area_level}`)
                for (let i = 0; i < 6; i++) {
                    let upper_area_sql = `SELECT * FROM salesforce.area1__c where sfid IN ('${area_id.join("','")}')`
                    console.log(`FOr Iteration --> ${i} Sql --> ${upper_area_sql}`);
                    let upper_area_res = await client.query(upper_area_sql);
                    if (upper_area_res.rows.length > 0) {
                        if (upper_area_res.rows[0]['area_level__c'] == area_level_id) {
                            upper_area_arr.push(upper_area_res.rows[0]['sfid'])
                            break;
                        } else {
                            area_id.length = 0
                            area_id.push(upper_area_res.rows[0]['parent_code__c']);
                            // upper_area_arr.push(upper_area_res.rows[0]['sfid'])
                        }
                    }
                }
                return upper_area_arr
            }
            if(area_level == area_level_value){
                console.log(`For Same Area`)
                return territory_code
            }
        }
    }catch(e){
        console.log(`Error In Get Any Area From Area ---> ${e}`)
    }
}

async function deletePjpRelatedData(pjp_id){
    try{
        let deleted_town_records = 0
        let deleted_visit_records = 0
        let return_flag = false
        // let pjp_sql = `SELECT * FROM salesforce.pjp__c where emp_id__c = '${team_id}' and pjp_date__c = '${date}'`
        // let pjp_res = await client.query(pjp_sql)
        if(pjp_id){
            //Delete Pjp Record
            let pjp_delete_sql = `delete from salesforce.pjp__c where sfid = '${pjp_id}'`
            let pjp_delete_res = await client.query(pjp_delete_sql)
            if(pjp_delete_res.rowCount){
                console.log(`Pjp Delete For Id ----> ${pjp_id}`);
                //Delete Town Related Records
                let pjp_town_delete_sql = `delete from salesforce.pjp_town_id__c where pjp_id__c = '${pjp_id}'`
                let pjp_town_delete_res = await client.query(pjp_town_delete_sql)
                if(pjp_town_delete_res.rowCount){
                    deleted_town_records ++
                    //Delete Visit Related Records
                    let pjp_visit_delete_sql = `delete from salesforce.visit__c where pjp_header__c = '${pjp_id}'`
                    let pjp_visit_delete_res = await client.query(pjp_visit_delete_sql)
                    if(pjp_visit_delete_res.rowCount){
                        deleted_visit_records ++
                        return_flag = true
                    }
                }
            }
            console.log(`${deleted_town_records} Town Delete related To Pjp`);
            console.log(`${deleted_visit_records} Visit Delete related To Pjp`);
            return return_flag;
        }
    }catch(e){
        console.log(`Error In Pjp Delete Function -----> ${e}`);
    }
}

async function createPjpData(team_id,today_date){
    try{
        let current_month = dtUtil.getMonth(today_date);  // 03
        let next_month = current_month;
        //let next_month = current_month;  //for running cron for current month
        let required_year = dtUtil.getYear(today_date);  // 2022
        if(next_month == 13){
          required_year = required_year + 1;
          next_month = 1;
        }
        //let start_date = moment(`${required_year}-${next_month}-${01}`).format('YYYY-MM-DD')
        let start_date = today_date
        console.log(next_month);
        console.log(required_year);
        console.log(start_date);
  
        // get picklist value (sfid) for Rejected from picklist
        let rejected_sfid_sql = `SELECT sfid FROM salesforce.picklist__c where object_name__c='PJP__c' and field_name__c='Approval_Status__c' and name='Pending'`;
        let rejected_sfid = await client.query(rejected_sfid_sql);
        rejected_sfid = rejected_sfid.rows[0]['sfid'];
        let no_of_weeks_in_current_month = await dtUtil.getNumWeeksForMonth(required_year, next_month);
  
        let current_day_number = dtUtil.getDay(start_date);    // this will be the start of the week where we need to start creating PJP. i.e 1/2/3/4/5/6/7
        let current_day = dtUtil.no_to_day(current_day_number);   // this is the first day of the month
  
        // Get end of the month 
        let last_day_month = dtUtil.getEndOfMonth(start_date);  // 2022-03-31
        console.log('last_day_month :::', last_day_month)
        // Get team data
        // check it's gtm
        // get it's teritory type
        // get it's day type
        // get it's day type plan
        // fill teritory if only one teritory is mapped with team table else leave it blank, team teritory mapping table
  
  
        // Getting all team members
        let team_sql = `select * from ${process.env.TABLE_SCHEMA_NAME}.team__c where sfid = '${team_id}'`;
        let team = await client.query(team_sql);
        console.log('Team  SQL :::', team_sql);
        if(team.rows.length > 0){
            for(let i=0; i< team.rows.length; i++){
                  //if(team.rows[i]['sfid'] == 'a0mC700000007zoIAA'){  // this is for testing only
                    console.log('Running cron for User :::', team.rows[i])
  
                    // Getting GTM
                    let fields = ['gtm__c.*, day_picklist.name as day_name, week_picklist.name as week_name'];
                    const tableName = 'gtm__c';
                    const WhereClouse = [];
                    let offset = '0',
                        limit = '1000';
                    let joins = [
                        {
                            type: 'LEFT',
                            table_name: 'picklist__c as day_picklist',
                            p_table_field: `gtm__c.day__c`,
                            s_table_field: `day_picklist.sfid`,
                        },
                        {
                            type: 'LEFT',
                            table_name: 'picklist__c as week_picklist',
                            p_table_field: `gtm__c.week__c`,
                            s_table_field: `week_picklist.sfid`,
                        },
                    ];
                    WhereClouse.push({ fieldName: 'gtm__c.territory_type__c', fieldValue: team.rows[i]['teamterritory_type__c']} )
            
                    let gtm_sql = qry.fetchAllWithJoinQry(fields, tableName, joins, WhereClouse,offset,limit,`order by gtm__c.createddate desc`);
                    console.log('Gtm SQL ======>',gtm_sql);
            
                    let data = await client.query(gtm_sql);
                    console.log('GTM data :::', data.rows.length)
                    // Filter data according to week
                    let week_1_map = {};
                    let week_2_map = {};
                    let week_3_map = {};
                    let week_4_map = {};
                    let week_5_map = {};
                    let week_6_map = {};
                    data.rows.forEach((da)=> {
                        if(da['week_name'] == '1st Week'){
                          week_1_map[da['day_name']] = da
                        }
                        if(da['week_name'] == '2st Week'){
                          week_2_map[da['day_name']] = da
                        }
                        if(da['week_name'] == '3st Week'){
                          week_3_map[da['day_name']] = da
                        }
                        if(da['week_name'] == '4st Week'){
                          week_4_map[da['day_name']] = da
                        }
                        if(da['week_name'] == '5st Week'){
                          week_5_map[da['day_name']] = da
                        }
                        if(da['week_name'] == '6st Week'){
                          week_6_map[da['day_name']] = da
                        }
                    });
                    let data_insert = [];
                    // Process data for 1st Week
                    //console.log('week_1_map ::::', week_1_map)
                    let start_date_1_week = start_date;
                    let end_date_1_week = dtUtil.addDays(start_date_1_week, 7 - current_day_number);
                    //let end_date_1_week = dtUtil.addDays(start_date_1_week, 8 - current_day_number);
                    /* Here, 8 represents the total number of days(7) in a week + 1. 
                    It means if somebody wants to use this logic for only 3 days in a week then he has to use (3 days+1) = 4  */
                    let data_week_1 = await process_data_week(week_1_map, 1, current_day, current_day_number, start_date_1_week, team.rows[i], team.rows[i]['team_member_name__c'], rejected_sfid, end_date_1_week, last_day_month)
                    if(data_week_1 && data_week_1.length > 0){
                        data_insert = [...data_insert, ...data_week_1];
                        console.log('Data Insert for Week 1 :======>',data_insert);
                    }
  
                    // Process data for 2nd Week
                    //console.log('week_2_map ::::', week_2_map)
                    let start_date_2_week = dtUtil.addDays(end_date_1_week, 1);
                    let end_date_2_week =  dtUtil.addDays(start_date_2_week, 6);//7 means for all the days in a week(MONDAY,TUESDAY,WEDNESDAY,THURSDAY,FRIDAY,SATURDAY,SUNDAY)
                    // let end_date_2_week =  dtUtil.addDays(start_date_2_week, 7);//7 means for all the days in a week(MONDAY,TUESDAY,WEDNESDAY,THURSDAY,FRIDAY,SATURDAY,SUNDAY)
                    let data_week_2 = await process_data_week(week_2_map, 2, 'Monday', 1, start_date_2_week, team.rows[i], team.rows[i]['team_member_name__c'], rejected_sfid, end_date_2_week, last_day_month)
                    if(data_week_2 && data_week_2.length > 0){
                        data_insert = [...data_insert, ...data_week_2];
                        console.log('Data Insert for Week 2 :======>',data_insert);
                    }
  
                    // Process data for 3rd Week
                    let start_date_3_week = dtUtil.addDays(end_date_2_week, 1);
                    let end_date_3_week =  dtUtil.addDays(start_date_3_week, 6);//7 means for all the days in a week(MONDAY,TUESDAY,WEDNESDAY,THURSDAY,FRIDAY,SATURDAY,SUNDAY)
                    // let end_date_3_week =  dtUtil.addDays(start_date_3_week, 7);//7 means for all the days in a week(MONDAY,TUESDAY,WEDNESDAY,THURSDAY,FRIDAY,SATURDAY,SUNDAY)
                    let data_week_3 = await process_data_week(week_3_map, 3, 'Monday', 1, start_date_3_week, team.rows[i], team.rows[i]['team_member_name__c'], rejected_sfid, end_date_3_week, last_day_month)
                    if(data_week_3 && data_week_3.length > 0){
                        data_insert = [...data_insert, ...data_week_3];
                        console.log('Data Insert for Week 3 :======>',data_insert);
                    }
  
                    // Process data for 4th Week
                    if(no_of_weeks_in_current_month > 3){
                        let start_date_4_week = dtUtil.addDays(end_date_3_week, 1);
                        // let end_date_4_week = dtUtil.addDays(start_date_4_week, 7);//7 means for all the days in a week(MONDAY,TUESDAY,WEDNESDAY,THURSDAY,FRIDAY,SATURDAY,SUNDAY)
                        let end_date_4_week = dtUtil.addDays(start_date_4_week, 6);//7 means for all the days in a week(MONDAY,TUESDAY,WEDNESDAY,THURSDAY,FRIDAY,SATURDAY,SUNDAY)
                        let data_week_4 = await process_data_week(week_4_map, 4, 'Monday', 1, start_date_4_week, team.rows[i], team.rows[i]['team_member_name__c'], rejected_sfid, end_date_4_week, last_day_month)
                        if(data_week_4 && data_week_4.length > 0){
                            data_insert = [...data_insert, ...data_week_4];
                            console.log('Data Insert for Week 4 :======>',data_insert);
                        }
  
                        // Process data for 5th Week
                        if(no_of_weeks_in_current_month > 4){
                            let start_date_5_week = dtUtil.addDays(end_date_4_week, 1);
                            // let end_date_5_week = dtUtil.addDays(start_date_5_week, 7);//7 means for all the days in a week(MONDAY,TUESDAY,WEDNESDAY,THURSDAY,FRIDAY,SATURDAY,SUNDAY)
                            let end_date_5_week = dtUtil.addDays(start_date_5_week, 6);//7 means for all the days in a week(MONDAY,TUESDAY,WEDNESDAY,THURSDAY,FRIDAY,SATURDAY,SUNDAY)
                            let data_week_5 = await process_data_week(week_5_map, 5, 'Monday', 1, start_date_5_week, team.rows[i], team.rows[i]['team_member_name__c'], rejected_sfid, end_date_5_week, last_day_month)
                            if(data_week_5 && data_week_5.length > 0){
                                data_insert = [...data_insert, ...data_week_5];
                                console.log('Data Insert for Week 5 :======>',data_insert);
                            }
  
                            // Process data for 6th Week
                            if(no_of_weeks_in_current_month > 5){
                                let start_date_6_week = dtUtil.addDays(end_date_5_week, 1);
                                // let end_date_6_week = dtUtil.addDays(start_date_6_week, 7);//7 means for all the days in a week(MONDAY,TUESDAY,WEDNESDAY,THURSDAY,FRIDAY,SATURDAY,SUNDAY)
                                let end_date_6_week = dtUtil.addDays(start_date_6_week, 6);//7 means for all the days in a week(MONDAY,TUESDAY,WEDNESDAY,THURSDAY,FRIDAY,SATURDAY,SUNDAY)
                                let data_week_6 = await process_data_week(week_6_map, 6, 'Monday', 1, start_date_6_week, team.rows[i], team.rows[i]['team_member_name__c'], rejected_sfid, end_date_6_week, last_day_month)
                                if(data_week_6 && data_week_6.length > 0){
                                    data_insert = [...data_insert, ...data_week_6];
                                    console.log('Data Insert for Week 6 :======>',data_insert);
                                }
                            }
                        }
                    }
                    // console.log('data_insert ::::', data_insert)
  
                    if(data_insert.length > 0){
                      // Insert this data into DB using insertMany function
                      let pjp_field = [`day_type_gtm__c, territory__c, pjp_date__c, isdeleted, emp_id__c, approval_status__c, day_type_plan__c, created_by_batch__c, days__c, gtm__c, emp_name__c, territory_name__c, createddate, pg_id__c`];
  
                      for(let i=0; i<data_insert.length; i++){
                        let data = data_insert[i];
                        // Check if records already exists in the DB
                        let checking_sql = `select * from ${process.env.TABLE_SCHEMA_NAME}.pjp__c where emp_id__c='${data['emp_id__c']}' and pjp_date__c = '${data['pjp_date__c']}' ;`;
                        let checking_sql_result = await client.query(checking_sql);
                        if(checking_sql_result.rows.length <= 0){
                          let pjp_sql = await qry.insertManyRecord(pjp_field, [data], 'pjp__c');
                        }
                      }
                      // let pjp_field_sql = await qry.insertManyRecord(pjp_field, data_insert, 'pjp__c', ', pg_id__c');
                      // let pjp_field_sql = await qry.insertManyRecordCustom(pjp_field, data_insert, 'pjp__c', ', pg_id__c');
                      // console.log('pjp_field_sql ::::', pjp_field_sql)
                    }
                 //}  // this is only for testing
            }
        }
        console.log('Cron job ended for Creating PJP')
      } catch (err) {
        console.log('Error in pjpJob:', err);
      }
}

async function process_data_week (week, week_number, current_day, current_day_number, start_date, team_data, team_name, pjp_status, exit_date, last_day_month){
    /*
    current_day = Monday
    week_number = 1
    current_day_number = 1
    week = {
        'Monday': {
            territory_type__c: 'a080w000009uQVpAAM',
            day_type__c: 'a0C0w000004ihp3EAA',
            day__c: 'a050w000002jdQaAAI',
            name: 'GTM-0024',
            isdeleted: false,
            week__c: 'a050w000002jdQpAAI',
            systemmodstamp: 2021-11-19T03:01:29.000Z,
            createddate: 2021-11-19T03:01:29.000Z,
            employee_type__c: 'a050w000002jeaOAAQ',
            sfid: 'a0O0w00000128tzEAA', // this is gtm_sfid
            id: 24,
            _hc_lastop: 'SYNCED',
            _hc_err: null,
            day_name: 'Friday',
            week_name: '4st Week'
        }
    }
    start_date = '2021-12-05'
    team_data = {
  
    }
    team_name = Kurnool
    last_day_month = '2021-12-31'
    */
    
    /*  Insert Payload
    let insert_payload = {
        day_type_gtm__c: week['day_type__c'],
        territory__c: '',
        pjp_date__c: start_date,
        isdeleted: false,
        emp_id__c: team_id,
        approval_status__c: pjp_status,
        day_type_plan__c: '',
        created_by_batch__c: true,
        week_days__c: week['day__c'],
        gtm__c: week['sfid'],
        emp_name__c: team_name,
        territory_name__c: ''
    }
    */
    let week_arr = Object.values(week);
    let week_length = week_arr.length;
    // For monday length can be mx 6 and for friday it can be max 2 (friday and saturday)
    // below for loop will run (7 - current_day_number) times
    let return_data = [];
    // console.log('Week data for testing :::', week)
    if(exit_date > last_day_month){
        exit_date = last_day_month;
    }
    let createddate = new Date();
    createddate = moment(createddate).format('YYYY-MM-DD hh:mm:ss');
    for(let i=0; start_date <= exit_date; i++){
        console.log(`week[current_day]::exit_date -${exit_date}::current_day_number -${current_day_number}::current_day -${current_day}::start_date -${start_date}::last_day_month -${last_day_month}`)
        if(week[current_day] && start_date <= last_day_month){
            //console.log('*************************************************************************************',week,week[current_day],week[current_day]['day_type__c']);
            let data = {
              day_type_gtm__c: week[current_day]['day_type__c'],
              territory__c: '',
              pjp_date__c: start_date,
              isdeleted: false,
              emp_id__c: team_data['sfid'],
              approval_status__c: pjp_status,
              day_type_plan__c: '',
              created_by_batch__c: true,
              week_days__c: week[current_day]['day__c'],
              gtm__c: week[current_day]['sfid'],
              emp_name__c: team_data['team_member_name__c'],
              territory_name__c: '',
              createddate: createddate,
              pg_id__c: uuidv4()
          }
  
          //@CHANGE ---> Added Left Join In Territory_sql and added a check for only territory type in line 342 If All Works Correctly Remove This Comment
            // Get Territory data for user
            // let territory_sql = `select team_territory_mapping__c.territory_name__c, area1__c.sfid as territory_id from ${process.env.TABLE_SCHEMA_NAME}.team_territory_mapping__c left join ${process.env.TABLE_SCHEMA_NAME}.area1__c on area1__c.territory_type__c = '' left join ${process.env.TABLE_SCHEMA_NAME}.territory_type__c on territory_type__c.sfid = ${week[current_day]['territory_type__c']} where team_member_id__c='${team_data['sfid']}' and team_territory_mapping__c.lob__c = '${team_data['lob__c']}' ;`;
            let territory_sql = `select team_territory_mapping__c.*, area1__c.*
                                from ${process.env.TABLE_SCHEMA_NAME}.team_territory_mapping__c 
                                LEFT JOIN salesforce.area1__c ON team_territory_mapping__c.territory_code__c=area1__c.sfid
                                where team_member_id__c='${team_data['sfid']}' 
                                and team_territory_mapping__c.lob__c = '${team_data['lob__c']}' ;`;
            let territory = await client.query(territory_sql);
            // console.log('territory_sql::::', territory_sql);
            // console.log('Territory Data::::', territory.rows);
  
            let territory_picklist = await func.getPicklistSfid('Area1__c','Area_Type__c','Territory')
  
            // Update territory_data for every object only if only one record is found else do nothing
            if(territory.rows && territory.rows.length == 1 && territory.rows[0]['area_type__c'] == territory_picklist){
                if(territory.rows[0]['territory_code__c'] && territory.rows[0]['territory_name__c']){
                  data['territory__c'] = territory.rows[0]['territory_code__c'],
                  data['territory_name__c'] = territory.rows[0]['territory_name__c']
                }
            }
  
            return_data.push(data)
        }
        
        // add one day into current no day
        current_day_number = current_day_number + 1
        current_day = dtUtil.no_to_day(current_day_number);
        // increase one more day to start_date
        start_date = dtUtil.addDays(start_date, 1);
    }
    // console.log('return_data in the end of the week:::', return_data)
  
    return return_data;
  
}


async function getLowerAreaFromBranch(branch_id){
    try{
        let lower_area_id = []
        let lower_area_arr = []
        lower_area_id.push(branch_id)
        let area_level_7_id = await getPicklistSfid('Area1__c', 'Area_Level__c', '7')
        for (let i = 0; i < 6; i++) {
            let lower_area_sql = `SELECT * FROM salesforce.area1__c where parent_code__c IN ('${lower_area_id.join("','")}')`
            console.log(`Lower Area Sql In ${i}th iteration ---> ${lower_area_sql}`);
            let lower_area_res = await client.query(lower_area_sql);
            if (lower_area_res.rows.length > 0) {
                if (lower_area_res.rows[0]['area_level__c'] == area_level_7_id) {
                    lower_area_res.rows.map((data) => {
                        lower_area_arr.push(data['sfid'])
                    })
                    break;
                } else {
                    lower_area_id.length = 0
                    lower_area_res.rows.map((data) => {
                        lower_area_arr.push(data['sfid'])
                        lower_area_id.push(data['sfid'])
                    })
                }
            }
        }
        return lower_area_arr
    }catch(e){
        console.log(`Error In Get Lower Area From Branch -----> ${e}`);
    }
}