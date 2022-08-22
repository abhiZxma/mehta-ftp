const cron = require('node-cron');
const qry = require(`${PROJECT_DIR}/utility/selectQueries`);
const dtUtil = require(`${PROJECT_DIR}/utility/dateUtility`);
const moment = require('moment');
const uuidv4 = require('uuid/v4');
const { update } = require('lodash');
const sort = require(`${PROJECT_DIR}/utility/sort`);
const func = require(`${PROJECT_DIR}/utility/functionalUtility`);
// const { sendnotificationToDevice } = require(`${PROJECT_DIR}/utility/notifications`);

const minute = 0;
const hour = 9;

const dynamic_data = {
  'a0C0w000004jB87EAE': 'Holiday',
  'a0C0w000004jB7sEAE': 'Leave',
  'a0C0w000004ihotEAA': 'HQ Town   Scouting',
  'a0C0w000004ihp3EAA': 'Non-HQ Town -B Scouting',
  'a0C0w000004ihoyEAA': 'Non-HQ Town -A Scouting',
  'a0C0w000004ie7FEAQ': 'Non-HQ Town-B ODJ Follow-up',
  'a0C0w000004ie7EEAQ': 'IGP + DMI',
  'a0C0w000004ie7DEAQ': 'Shadow Working',
  'a0C0w000004ie7CEAQ': 'ID / Key Account Visit',
  'a0C0w000004ie7BEAQ': 'Non-HQ Town-B Pristine Scouting',
  'a0C0w000004ie7AEAQ': 'Pristine Scouting',
  'a0C0w000004ie79EAA': 'Non-HQ Town -B Scouting',
  'a0C0w000004ie78EAA': 'Non-HQ Town-A ODJ Follow-up',
  'a0C0w000004ie77EAA': 'ID / DMI Meeting',
  'a0C0w000004ie76EAA': 'Non  HQ Town',
  'a0C0w000004ie75EAA': 'HQ Town Project Closer',
  'a0C0w000004ie74EAA': 'Flexible Day',
  'a0C0w000004ie73EAA': 'Non HQ Town Shadow Working',
  'a0C0w000004ie72EAA': 'Non-HQ Town-A Pristine Scouting',
  'a0C0w000004ie71EAA': 'HQ Town Retailer visit',
  'a0C0w000004ie70EAA': 'HQ Town Pristine Scouting',
  'a0C0w000004ie6zEAA': 'ODJ Follow up',
  'a0C0w000004ie6yEAA': 'Non-HQ Town -A Scouting',
  'a0C0w000004ie6xEAA': 'HQ Town Weekly review',
  'a0C0w000004ie6wEAA': 'Weekly Review',
  'a0C0w000004ie6vEAA': 'HQ Town ODJ Follow-up',
  'a0C0w000004ie6uEAA': 'Non HQ Town ODJ Follow-up',
  'a0C0w000004ie6tEAA': 'Project Followup &Closer',
  'a0C0w000004ie6sEAA': 'Sub-Dealer',
  'a0C0w000004ie6rEAA': 'Primary Dealer',
  'a0C0w000004ie6qEAA': 'IGP + DMI + Tower Entry + Follow Up',
  'a0C0w000004ie6pEAA': 'Scouting + DMI + G+4 Premium',
  'a0C0w000004ie7GEAQ': 'HQ / Non HQ Town Shadow Working',
  'a0C0w000004ie7HEAQ': 'HQ Town Shadow Working',
  'a0C0w000004ie6oEAA': 'HQ Town   Scouting',
}

const pjpJob = cron.schedule(
  `10 1 25 * *`,
// const pjpJob = cron.schedule(
   //`37 00 * * *`,
  async () => {
    try{
      console.log('Cron job started for Creating PJP');
      let today_date = new Date();
      today_date = moment(today_date).format('YYYY-MM-DD');
      let current_month = dtUtil.getMonth(today_date);  // 03
      let next_month = current_month + 1;
      //let next_month = current_month;  //for running cron for current month
      let required_year = dtUtil.getYear(today_date);  // 2022
      if(next_month == 13){
        required_year = required_year + 1;
        next_month = 1;
      }
      let start_date = moment(`${required_year}-${next_month}-${01}`).format('YYYY-MM-DD')
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
      let team_sql = `select * from ${process.env.TABLE_SCHEMA_NAME}.team__c;`;
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
  },
  { scheduled: true }
);

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

const visitComplianceJob = cron.schedule(
  `00 23 * * *`,
// const visitComplianceJob = cron.schedule(  // this is for testing only
//   `17 19 * * *`,
  async () => {
    try{
      console.log('Cron job started for PJP Compliance');
      let today_date = new Date();
      today_date = moment(today_date).format('YYYY-MM-DD');
      // Check Users
      let team_sql = `select * from ${process.env.TABLE_SCHEMA_NAME}.team__c;`;
      let team = await client.query(team_sql);
      if(team.rows.length > 0){
          for(let i=0; i< team.rows.length; i++){
               //if(team.rows[i]['sfid'] == 'a030w0000080llRAAQ'){  // this is for testing only
                  console.log('Running cron for User :::', team.rows[i]['name'])
                  // Check Visits for Every User
                  let fields = ['visit__c.*, pjp__c.name as pjp_header_name, pjp__c.day_type_plan__c as day_type_plan, pjp__c.day_type_gtm__c as day_type_expected, day_type__c.name as day_type_name'];
                  const tableName = 'visit__c';
                  const WhereClouse = [];
                  let offset = '0',
                      limit = '1000';
                  let joins = [
                      {
                          type: 'LEFT',
                          table_name: 'pjp__c',
                          p_table_field: `visit__c.pjp_header__c`,
                          s_table_field: `pjp__c.sfid`,
                      },
                      {
                          type: 'LEFT',
                          table_name: 'day_type__c',
                          p_table_field: `pjp__c.day_type_plan__c`,
                          s_table_field: `day_type__c.sfid`,
                      },
                  ];
                  WhereClouse.push({ fieldName: 'visit__c.emp_id__c', fieldValue: team.rows[i]['sfid']} )
                  WhereClouse.push({ fieldName: 'visit__c.visit_date__c', fieldValue: today_date} )
          
                  let visit_sql = qry.fetchAllWithJoinQry(fields, tableName, joins, WhereClouse,offset,limit,`order by visit__c.createddate desc`);
                  
                  let visit_data = await client.query(visit_sql);
                  console.log('visit_data :::', visit_data.rows)

                  // Check every visit and update it's pjp_header compliance
                  if(visit_data.rows && visit_data.rows.length > 0){
                    for(let i=0; i< visit_data.rows.length; i++){
                      let visit = visit_data.rows[i];
                      switch(visit.day_type_plan){
                        case 'a0C0w000004ihotEAA':
                            if(visit['grid__c']){
                              let update_sql = `update ${process.env.TABLE_SCHEMA_NAME}.pjp__c set pjp_compliance__c=true where sfid = '${visit['pjp_header__c']}'`
                              let result = await client.query(update_sql);
                              console.log('SQL :::', update_sql);
                              console.log(`SQL visit_plan ::${visit.day_type_plan} and grid ::${visit['grid__c']}`);
                              console.log('SQL result :::', result);
                            }else {
                              let update_sql = `update ${process.env.TABLE_SCHEMA_NAME}.pjp__c set pjp_compliance__c=false where sfid = '${visit['pjp_header__c']}'`
                              let result = await client.query(update_sql);
                              console.log('SQL :::', update_sql);
                              console.log(`SQL visit_plan ::${visit.day_type_plan} and grid ::${visit['grid__c']}`);
                              console.log('SQL result :::', result);
                            }
                        break;
                        case 'a0C0w000004ie6pEAA':
                          if(visit['grid__c']){
                            let update_sql = `update ${process.env.TABLE_SCHEMA_NAME}.pjp__c set pjp_compliance__c=true where sfid = '${visit['pjp_header__c']}'`
                            let result = await client.query(update_sql);
                            console.log('SQL :::', update_sql);
                            console.log(`SQL visit_plan ::${visit.day_type_plan} and grid ::${visit['grid__c']}`);
                            console.log('SQL result :::', result);
                          }else {
                            let update_sql = `update ${process.env.TABLE_SCHEMA_NAME}.pjp__c set pjp_compliance__c=false where sfid = '${visit['pjp_header__c']}'`
                            let result = await client.query(update_sql);
                            console.log('SQL :::', update_sql);
                            console.log(`SQL visit_plan ::${visit.day_type_plan} and grid ::${visit['grid__c']}`);
                            console.log('SQL result :::', result);
                          }
                        break;
                        case 'a0C0w000004ihoyEAA':
                          if(visit['grid__c']){
                            let update_sql = `update ${process.env.TABLE_SCHEMA_NAME}.pjp__c set pjp_compliance__c=true where sfid = '${visit['pjp_header__c']}'`
                            let result = await client.query(update_sql);
                            console.log('SQL :::', update_sql);
                            console.log(`SQL visit_plan ::${visit.day_type_plan} and grid ::${visit['grid__c']}`);
                            console.log('SQL result :::', result);
                          }else {
                            let update_sql = `update ${process.env.TABLE_SCHEMA_NAME}.pjp__c set pjp_compliance__c=false where sfid = '${visit['pjp_header__c']}'`
                            let result = await client.query(update_sql);
                            console.log('SQL :::', update_sql);
                            console.log(`SQL visit_plan ::${visit.day_type_plan} and grid ::${visit['grid__c']}`);
                            console.log('SQL result :::', result);
                          }
                        break;  
                        case 'a0C0w000004ie7AEAQ':
                          if(visit['grid__c']){
                            let update_sql = `update ${process.env.TABLE_SCHEMA_NAME}.pjp__c set pjp_compliance__c=true where sfid = '${visit['pjp_header__c']}'`
                            let result = await client.query(update_sql);
                            console.log('SQL :::', update_sql);
                            console.log(`SQL visit_plan ::${visit.day_type_plan} and grid ::${visit['grid__c']}`);
                            console.log('SQL result :::', result);
                          }else {
                            let update_sql = `update ${process.env.TABLE_SCHEMA_NAME}.pjp__c set pjp_compliance__c=false where sfid = '${visit['pjp_header__c']}'`
                            let result = await client.query(update_sql);
                            console.log('SQL :::', update_sql);
                            console.log(`SQL visit_plan ::${visit.day_type_plan} and grid ::${visit['grid__c']}`);
                            console.log('SQL result :::', result);
                          }
                        break;  
                        case 'a0C0w000004ie72EAA':
                          if(visit['grid__c']){
                            let update_sql = `update ${process.env.TABLE_SCHEMA_NAME}.pjp__c set pjp_compliance__c=true where sfid = '${visit['pjp_header__c']}'`
                            let result = await client.query(update_sql);
                            console.log('SQL :::', update_sql);
                            console.log(`SQL visit_plan ::${visit.day_type_plan} and grid ::${visit['grid__c']}`);
                            console.log('SQL result :::', result);
                          }else {
                            let update_sql = `update ${process.env.TABLE_SCHEMA_NAME}.pjp__c set pjp_compliance__c=false where sfid = '${visit['pjp_header__c']}'`
                            let result = await client.query(update_sql);
                            console.log('SQL :::', update_sql);
                            console.log(`SQL visit_plan ::${visit.day_type_plan} and grid ::${visit['grid__c']}`);
                            console.log('SQL result :::', result);
                          }
                        break;  
                        case 'a0C0w000004ie70EAA':
                          if(visit['grid__c']){
                            let update_sql = `update ${process.env.TABLE_SCHEMA_NAME}.pjp__c set pjp_compliance__c=true where sfid = '${visit['pjp_header__c']}'`
                            let result = await client.query(update_sql);
                            console.log('SQL :::', update_sql);
                            console.log(`SQL visit_plan ::${visit.day_type_plan} and grid ::${visit['grid__c']}`);
                            console.log('SQL result :::', result);
                          }else {
                            let update_sql = `update ${process.env.TABLE_SCHEMA_NAME}.pjp__c set pjp_compliance__c=false where sfid = '${visit['pjp_header__c']}'`
                            let result = await client.query(update_sql);
                            console.log('SQL :::', update_sql);
                            console.log(`SQL visit_plan ::${visit.day_type_plan} and grid ::${visit['grid__c']}`);
                            console.log('SQL result :::', result);
                          }
                        break; 
                        case 'a0C0w000004ie7BEAQ':
                          if(visit['grid__c']){
                            let update_sql = `update ${process.env.TABLE_SCHEMA_NAME}.pjp__c set pjp_compliance__c=true where sfid = '${visit['pjp_header__c']}'`
                            let result = await client.query(update_sql);
                            console.log('SQL :::', update_sql);
                            console.log(`SQL visit_plan ::${visit.day_type_plan} and grid ::${visit['grid__c']}`);
                            console.log('SQL result :::', result);
                          }else {
                            let update_sql = `update ${process.env.TABLE_SCHEMA_NAME}.pjp__c set pjp_compliance__c=false where sfid = '${visit['pjp_header__c']}'`
                            let result = await client.query(update_sql);
                            console.log('SQL :::', update_sql);
                            console.log(`SQL visit_plan ::${visit.day_type_plan} and grid ::${visit['grid__c']}`);
                            console.log('SQL result :::', result);
                          }
                        break;
                        case 'a0C0w000004ie79EAA':
                          if(visit['grid__c']){
                            let update_sql = `update ${process.env.TABLE_SCHEMA_NAME}.pjp__c set pjp_compliance__c=true where sfid = '${visit['pjp_header__c']}'`
                            let result = await client.query(update_sql);
                            console.log('SQL :::', update_sql);
                            console.log(`SQL visit_plan ::${visit.day_type_plan} and grid ::${visit['grid__c']}`);
                            console.log('SQL result :::', result);
                          }else {
                            let update_sql = `update ${process.env.TABLE_SCHEMA_NAME}.pjp__c set pjp_compliance__c=false where sfid = '${visit['pjp_header__c']}'`
                            let result = await client.query(update_sql);
                            console.log('SQL :::', update_sql);
                            console.log(`SQL visit_plan ::${visit.day_type_plan} and grid ::${visit['grid__c']}`);
                            console.log('SQL result :::', result);
                          }
                        break;  
                        case 'a0C0w000004ie7DEAQ':
                          if(visit['grid__c']){
                            let update_sql = `update ${process.env.TABLE_SCHEMA_NAME}.pjp__c set pjp_compliance__c=true where sfid = '${visit['pjp_header__c']}'`
                            let result = await client.query(update_sql);
                            console.log('SQL :::', update_sql);
                            console.log(`SQL visit_plan ::${visit.day_type_plan} and grid ::${visit['grid__c']}`);
                            console.log('SQL result :::', result);
                          }else {
                            let update_sql = `update ${process.env.TABLE_SCHEMA_NAME}.pjp__c set pjp_compliance__c=false where sfid = '${visit['pjp_header__c']}'`
                            let result = await client.query(update_sql);
                            console.log('SQL :::', update_sql);
                            console.log(`SQL visit_plan ::${visit.day_type_plan} and grid ::${visit['grid__c']}`);
                            console.log('SQL result :::', result);
                          }
                        break;  
                        case 'a0C0w000004ie7HEAQ':
                          if(visit['grid__c']){
                            let update_sql = `update ${process.env.TABLE_SCHEMA_NAME}.pjp__c set pjp_compliance__c=true where sfid = '${visit['pjp_header__c']}'`
                            let result = await client.query(update_sql);
                            console.log('SQL :::', update_sql);
                            console.log(`SQL visit_plan ::${visit.day_type_plan} and grid ::${visit['grid__c']}`);
                            console.log('SQL result :::', result);
                          }else {
                            let update_sql = `update ${process.env.TABLE_SCHEMA_NAME}.pjp__c set pjp_compliance__c=false where sfid = '${visit['pjp_header__c']}'`
                            let result = await client.query(update_sql);
                            console.log('SQL :::', update_sql);
                            console.log(`SQL visit_plan ::${visit.day_type_plan} and grid ::${visit['grid__c']}`);
                            console.log('SQL result :::', result);
                          }
                        break;  
                        case 'a0C0w000004ie7GEAQ':
                          if(visit['grid__c']){
                            let update_sql = `update ${process.env.TABLE_SCHEMA_NAME}.pjp__c set pjp_compliance__c=true where sfid = '${visit['pjp_header__c']}'`
                            let result = await client.query(update_sql);
                            console.log('SQL :::', update_sql);
                            console.log(`SQL visit_plan ::${visit.day_type_plan} and grid ::${visit['grid__c']}`);
                            console.log('SQL result :::', result);
                          }else {
                            let update_sql = `update ${process.env.TABLE_SCHEMA_NAME}.pjp__c set pjp_compliance__c=false where sfid = '${visit['pjp_header__c']}'`
                            let result = await client.query(update_sql);
                            console.log('SQL :::', update_sql);
                            console.log(`SQL visit_plan ::${visit.day_type_plan} and grid ::${visit['grid__c']}`);
                            console.log('SQL result :::', result);
                          }
                        break;  
                        case 'a0C0w000004ie7FEAQ':
                          if(visit['grid__c']){
                            let update_sql = `update ${process.env.TABLE_SCHEMA_NAME}.pjp__c set pjp_compliance__c=true where sfid = '${visit['pjp_header__c']}'`
                            let result = await client.query(update_sql);
                            console.log('SQL :::', update_sql);
                            console.log(`SQL visit_plan ::${visit.day_type_plan} and grid ::${visit['grid__c']}`);
                            console.log('SQL result :::', result);
                          }else {
                            let update_sql = `update ${process.env.TABLE_SCHEMA_NAME}.pjp__c set pjp_compliance__c=false where sfid = '${visit['pjp_header__c']}'`
                            let result = await client.query(update_sql);
                            console.log('SQL :::', update_sql);
                            console.log(`SQL visit_plan ::${visit.day_type_plan} and grid ::${visit['grid__c']}`);
                            console.log('SQL result :::', result);
                          }
                        break;  
                        case 'a0C0w000004ie7FEAQ':
                          if(visit['grid__c']){
                            let update_sql = `update ${process.env.TABLE_SCHEMA_NAME}.pjp__c set pjp_compliance__c=true where sfid = '${visit['pjp_header__c']}'`
                            let result = await client.query(update_sql);
                            console.log('SQL :::', update_sql);
                            console.log(`SQL visit_plan ::${visit.day_type_plan} and grid ::${visit['grid__c']}`);
                            console.log('SQL result :::', result);
                          }else {
                            let update_sql = `update ${process.env.TABLE_SCHEMA_NAME}.pjp__c set pjp_compliance__c=false where sfid = '${visit['pjp_header__c']}'`
                            let result = await client.query(update_sql);
                            console.log('SQL :::', update_sql);
                            console.log(`SQL visit_plan ::${visit.day_type_plan} and grid ::${visit['grid__c']}`);
                            console.log('SQL result :::', result);
                          }
                        break; 
                        case 'a0C0w000004ie6qEAA':
                          if(visit['influencer__c']){
                            let update_sql = `update ${process.env.TABLE_SCHEMA_NAME}.pjp__c set pjp_compliance__c=true where sfid = '${visit['pjp_header__c']}'`
                            let result = await client.query(update_sql);
                            console.log('SQL :::', update_sql);
                            console.log(`SQL visit_plan ::${visit.day_type_plan} and influencer::${visit['influencer__c']}`);
                            console.log('SQL result :::', result);
                          }else {
                            let update_sql = `update ${process.env.TABLE_SCHEMA_NAME}.pjp__c set pjp_compliance__c=false where sfid = '${visit['pjp_header__c']}'`
                            let result = await client.query(update_sql);
                            console.log('SQL :::', update_sql);
                            console.log(`SQL visit_plan ::${visit.day_type_plan} and influencer ::${visit['influencer__c']}`);
                            console.log('SQL result :::', result);
                          }
                        break;
                        case 'a0C0w000004ie6rEAA':
                          if(visit['dealer_retailer__c']){
                            let Dealer=`select * from ${process.env.TABLE_SCHEMA_NAME}.account_type_activation__c where account_name__c = '${visit['dealer_retailer__c']}';`
                            let result = await client.query(Dealer);
                            if(result.rows[0]['account_type__c']=='a050w000002jcMcAAI'){
                              let update_sql = `update ${process.env.TABLE_SCHEMA_NAME}.pjp__c set pjp_compliance__c=true where sfid = '${visit['pjp_header__c']}'`
                              let result = await client.query(update_sql);
                              console.log('SQL :::', update_sql);
                              console.log(`SQL visit_plan ::${visit.day_type_plan} and dealer_retailer__c ::${visit['dealer_retailer__c']}`);
                              console.log('SQL result :::', result);
                            }
                          }else {
                            let update_sql = `update ${process.env.TABLE_SCHEMA_NAME}.pjp__c set pjp_compliance__c=false where sfid = '${visit['pjp_header__c']}'`
                            let result = await client.query(update_sql);
                            console.log('SQL :::', update_sql);
                            console.log(`SQL visit_plan ::${visit.day_type_plan} and dealer_retailer__c ::${visit['dealer_retailer__c']}`);
                            console.log('SQL result :::', result);
                          }
                        break;
                        case 'a0C0w000004ie6sEAA':
                          if(visit['dealer_retailer__c']){
                            let Retailer=`select * from ${process.env.TABLE_SCHEMA_NAME}.account_type_activation__c where account_name__c = '${visit['dealer_retailer__c']}';`
                            let result = await client.query(Retailer);
                            if(result.rows[0]['account_type__c']=='a050w000002jcMXAAY'){
                              let update_sql = `update ${process.env.TABLE_SCHEMA_NAME}.pjp__c set pjp_compliance__c=true where sfid = '${visit['pjp_header__c']}'`
                              let result = await client.query(update_sql);
                              console.log('SQL :::', update_sql);
                              console.log(`SQL visit_plan ::${visit.day_type_plan} and dealer_retailer__c ::${visit['dealer_retailer__c']}`);
                              console.log('SQL result :::', result);
                            }
                          }else {
                            let update_sql = `update ${process.env.TABLE_SCHEMA_NAME}.pjp__c set pjp_compliance__c=false where sfid = '${visit['pjp_header__c']}'`
                            let result = await client.query(update_sql);
                            console.log('SQL :::', update_sql);
                            console.log(`SQL visit_plan ::${visit.day_type_plan} and dealer_retailer__c ::${visit['dealer_retailer__c']}`);
                            console.log('SQL result :::', result);
                          }
                        break;
                        case 'a0C0w000004ie6tEAA':
                          if(visit['task__c']){
                            let update_sql = `update ${process.env.TABLE_SCHEMA_NAME}.pjp__c set pjp_compliance__c=true where sfid = '${visit['pjp_header__c']}'`
                            let result = await client.query(update_sql);
                            console.log('SQL :::', update_sql);
                            console.log(`SQL visit_plan ::${visit.day_type_plan} and task ::${visit['task__c']}`);
                            console.log('SQL result :::', result);
                          }else {
                            let update_sql = `update ${process.env.TABLE_SCHEMA_NAME}.pjp__c set pjp_compliance__c=false where sfid = '${visit['pjp_header__c']}'`
                            let result = await client.query(update_sql);
                            console.log('SQL :::', update_sql);
                            console.log(`SQL visit_plan ::${visit.day_type_plan} and task__c ::${visit['task__c']}`);
                            console.log('SQL result :::', result);
                          }
                         break;
                        case 'a0C0w000004ie6uEAA':
                          if(visit['task__c']){
                            let update_sql = `update ${process.env.TABLE_SCHEMA_NAME}.pjp__c set pjp_compliance__c=true where sfid = '${visit['pjp_header__c']}'`
                            let result = await client.query(update_sql);
                            console.log('SQL :::', update_sql);
                            console.log(`SQL visit_plan ::${visit.day_type_plan} and task ::${visit['task__c']}`);
                            console.log('SQL result :::', result);
                          }else {
                            let update_sql = `update ${process.env.TABLE_SCHEMA_NAME}.pjp__c set pjp_compliance__c=false where sfid = '${visit['pjp_header__c']}'`
                            let result = await client.query(update_sql);
                            console.log('SQL :::', update_sql);
                            console.log(`SQL visit_plan ::${visit.day_type_plan} and grid ::${visit['grid__c']}`);
                            console.log('SQL result :::', result);
                          }
                        break;
                        case 'a0C0w000004ie6vEAA':
                          if(visit['task__c']){
                            let update_sql = `update ${process.env.TABLE_SCHEMA_NAME}.pjp__c set pjp_compliance__c=true where sfid = '${visit['pjp_header__c']}'`
                            let result = await client.query(update_sql);
                            console.log('SQL :::', update_sql);
                            console.log(`SQL visit_plan ::${visit.day_type_plan} and task ::${visit['task__c']}`);
                            console.log('SQL result :::', result);
                          }else {
                            let update_sql = `update ${process.env.TABLE_SCHEMA_NAME}.pjp__c set pjp_compliance__c=false where sfid = '${visit['pjp_header__c']}'`
                            let result = await client.query(update_sql);
                            console.log('SQL :::', update_sql);
                            console.log(`SQL visit_plan ::${visit.day_type_plan} and task ::${visit['task__c']}`);
                            console.log('SQL result :::', result);
                          }
                        break;
                        case 'a0C0w000004ie6wEAA':
                          if(visit['task__c']){
                            let update_sql = `update ${process.env.TABLE_SCHEMA_NAME}.pjp__c set pjp_compliance__c=true where sfid = '${visit['pjp_header__c']}'`
                            let result = await client.query(update_sql);
                            console.log('SQL :::', update_sql);
                            console.log(`SQL visit_plan ::${visit.day_type_plan} and task ::${visit['task__c']}`);
                            console.log('SQL result :::', result);
                          }else {
                            let update_sql = `update ${process.env.TABLE_SCHEMA_NAME}.pjp__c set pjp_compliance__c=false where sfid = '${visit['pjp_header__c']}'`
                            let result = await client.query(update_sql);
                            console.log('SQL :::', update_sql);
                            console.log(`SQL visit_plan ::${visit.day_type_plan} and task ::${visit['task__c']}`);
                            console.log('SQL result :::', result);
                          }
                        break;
                        case 'a0C0w000004ie6xEAA':
                          if(visit['task__c']){
                            let update_sql = `update ${process.env.TABLE_SCHEMA_NAME}.pjp__c set pjp_compliance__c=true where sfid = '${visit['pjp_header__c']}'`
                            let result = await client.query(update_sql);
                            console.log('SQL :::', update_sql);
                            console.log(`SQL visit_plan ::${visit.day_type_plan} and task ::${visit['task__c']}`);
                            console.log('SQL result :::', result);
                          }else {
                            let update_sql = `update ${process.env.TABLE_SCHEMA_NAME}.pjp__c set pjp_compliance__c=false where sfid = '${visit['pjp_header__c']}'`
                            let result = await client.query(update_sql);
                            console.log('SQL :::', update_sql);
                            console.log(`SQL visit_plan ::${visit.day_type_plan} and task ::${visit['task__c']}`);
                            console.log('SQL result :::', result);
                          }
                        break;
                        case 'a0C0w000004ie6zEAA':
                          if(visit['task__c']){
                            let update_sql = `update ${process.env.TABLE_SCHEMA_NAME}.pjp__c set pjp_compliance__c=true where sfid = '${visit['pjp_header__c']}'`
                            let result = await client.query(update_sql);
                            console.log('SQL :::', update_sql);
                            console.log(`SQL visit_plan ::${visit.day_type_plan} and task ::${visit['task__c']}`);
                            console.log('SQL result :::', result);
                          }else {
                            let update_sql = `update ${process.env.TABLE_SCHEMA_NAME}.pjp__c set pjp_compliance__c=false where sfid = '${visit['pjp_header__c']}'`
                            let result = await client.query(update_sql);
                            console.log('SQL :::', update_sql);
                            console.log(`SQL visit_plan ::${visit.day_type_plan} and task ::${visit['task__c']}`);
                            console.log('SQL result :::', result);
                          }
                        break;
                        case 'a0C0w000004ie71EAA':
                          if(visit['dealer_retailer__c']){
                            let Retailer=`select * from ${process.env.TABLE_SCHEMA_NAME}.account_type_activation__c where account_name__c = '${visit['dealer_retailer__c']}';`
                            let result = await client.query(Retailer);
                            if(result.rows[0]['account_type__c']=='a050w000002jcMXAAY'){
                              let update_sql = `update ${process.env.TABLE_SCHEMA_NAME}.pjp__c set pjp_compliance__c=true where sfid = '${visit['pjp_header__c']}'`
                              let result = await client.query(update_sql);
                              console.log('SQL :::', update_sql);
                              console.log(`SQL visit_plan ::${visit.day_type_plan} and dealer_retailer__c ::${visit['dealer_retailer__c']}`);
                              console.log('SQL result :::', result);
                            }
                          }else {
                            let update_sql = `update ${process.env.TABLE_SCHEMA_NAME}.pjp__c set pjp_compliance__c=false where sfid = '${visit['pjp_header__c']}'`
                            let result = await client.query(update_sql);
                            console.log('SQL :::', update_sql);
                            console.log(`SQL visit_plan ::${visit.day_type_plan} and dealer_retailer__c ::${visit['dealer_retailer__c']}`);
                            console.log('SQL result :::', result);
                          }
                        break;
                        case 'a0C0w000004ie73EAA':
                          if(visit['grid__c']){
                            let update_sql = `update ${process.env.TABLE_SCHEMA_NAME}.pjp__c set pjp_compliance__c=true where sfid = '${visit['pjp_header__c']}'`
                            let result = await client.query(update_sql);
                            console.log('SQL :::', update_sql);
                            console.log(`SQL visit_plan ::${visit.day_type_plan} and grid ::${visit['grid__c']}`);
                            console.log('SQL result :::', result);
                          }else {
                            let update_sql = `update ${process.env.TABLE_SCHEMA_NAME}.pjp__c set pjp_compliance__c=false where sfid = '${visit['pjp_header__c']}'`
                            let result = await client.query(update_sql);
                            console.log('SQL :::', update_sql);
                            console.log(`SQL visit_plan ::${visit.day_type_plan} and grid ::${visit['grid__c']}`);
                            console.log('SQL result :::', result);
                          }
                        break;
                        case 'a0C0w000004ie74EAA':
                          if(visit['task__c']){
                            let update_sql = `update ${process.env.TABLE_SCHEMA_NAME}.pjp__c set pjp_compliance__c=true where sfid = '${visit['pjp_header__c']}'`
                            let result = await client.query(update_sql);
                            console.log('SQL :::', update_sql);
                            console.log(`SQL visit_plan ::${visit.day_type_plan} and task ::${visit['task__c']}`);
                            console.log('SQL result :::', result);
                          }else {
                            let update_sql = `update ${process.env.TABLE_SCHEMA_NAME}.pjp__c set pjp_compliance__c=false where sfid = '${visit['pjp_header__c']}'`
                            let result = await client.query(update_sql);
                            console.log('SQL :::', update_sql);
                            console.log(`SQL visit_plan ::${visit.day_type_plan} and task__c ::${visit['task__c']}`);
                            console.log('SQL result :::', result);
                          }
                        break;
                        case 'a0C0w000004ie75EAA':
                          if(visit['task__c']){
                            let update_sql = `update ${process.env.TABLE_SCHEMA_NAME}.pjp__c set pjp_compliance__c=true where sfid = '${visit['pjp_header__c']}'`
                            let result = await client.query(update_sql);
                            console.log('SQL :::', update_sql);
                            console.log(`SQL visit_plan ::${visit.day_type_plan} and task ::${visit['task__c']}`);
                            console.log('SQL result :::', result);
                          }else {
                            let update_sql = `update ${process.env.TABLE_SCHEMA_NAME}.pjp__c set pjp_compliance__c=false where sfid = '${visit['pjp_header__c']}'`
                            let result = await client.query(update_sql);
                            console.log('SQL :::', update_sql);
                            console.log(`SQL visit_plan ::${visit.day_type_plan} and task ::${visit['task__c']}`);
                            console.log('SQL result :::', result);
                          }
                        break;
                        case 'a0C0w000004ie76EAA':
                          if(visit['task__c']){
                            let update_sql = `update ${process.env.TABLE_SCHEMA_NAME}.pjp__c set pjp_compliance__c=true where sfid = '${visit['pjp_header__c']}'`
                            let result = await client.query(update_sql);
                            console.log('SQL :::', update_sql);
                            console.log(`SQL visit_plan ::${visit.day_type_plan} and task ::${visit['task__c']}`);
                            console.log('SQL result :::', result);
                          }else {
                            let update_sql = `update ${process.env.TABLE_SCHEMA_NAME}.pjp__c set pjp_compliance__c=false where sfid = '${visit['pjp_header__c']}'`
                            let result = await client.query(update_sql);
                            console.log('SQL :::', update_sql);
                            console.log(`SQL visit_plan ::${visit.day_type_plan} and task ::${visit['task__c']}`);
                            console.log('SQL result :::', result);
                          }
                        break;
                        case 'a0C0w000004ie77EAA':
                          if(visit['influencer__c']){
                            let update_sql = `update ${process.env.TABLE_SCHEMA_NAME}.pjp__c set pjp_compliance__c=true where sfid = '${visit['pjp_header__c']}'`
                            let result = await client.query(update_sql);
                            console.log('SQL :::', update_sql);
                            console.log(`SQL visit_plan ::${visit.day_type_plan} and influencer ::${visit['influencer__c']}`);
                            console.log('SQL result :::', result);
                          }else {
                            let update_sql = `update ${process.env.TABLE_SCHEMA_NAME}.pjp__c set pjp_compliance__c=false where sfid = '${visit['pjp_header__c']}'`
                            let result = await client.query(update_sql);
                            console.log('SQL :::', update_sql);
                            console.log(`SQL visit_plan ::${visit.day_type_plan} and influencer__c ::${visit['influencer__c']}`);
                            console.log('SQL result :::', result);
                          }
                        break;
                        case 'a0C0w000004ie78EAA':
                          if(visit['task__c']){
                            let update_sql = `update ${process.env.TABLE_SCHEMA_NAME}.pjp__c set pjp_compliance__c=true where sfid = '${visit['pjp_header__c']}'`
                            let result = await client.query(update_sql);
                            console.log('SQL :::', update_sql);
                            console.log(`SQL visit_plan ::${visit.day_type_plan} and task ::${visit['task__c']}`);
                            console.log('SQL result :::', result);
                          }else {
                            let update_sql = `update ${process.env.TABLE_SCHEMA_NAME}.pjp__c set pjp_compliance__c=false where sfid = '${visit['pjp_header__c']}'`
                            let result = await client.query(update_sql);
                            console.log('SQL :::', update_sql);
                            console.log(`SQL visit_plan ::${visit.day_type_plan} and task ::${visit['task__c']}`);
                            console.log('SQL result :::', result);
                          }
                        break;
                        case 'a0C0w000004ie7CEAQ':
                          if(visit['influencer__c']){
                            let update_sql = `update ${process.env.TABLE_SCHEMA_NAME}.pjp__c set pjp_compliance__c=true where sfid = '${visit['pjp_header__c']}'`
                            let result = await client.query(update_sql);
                            console.log('SQL :::', update_sql);
                            console.log(`SQL visit_plan ::${visit.day_type_plan} and influencer ::${visit['influencer__c']}`);
                            console.log('SQL result :::', result);
                          }else {
                            let update_sql = `update ${process.env.TABLE_SCHEMA_NAME}.pjp__c set pjp_compliance__c=false where sfid = '${visit['pjp_header__c']}'`
                            let result = await client.query(update_sql);
                            console.log('SQL :::', update_sql);
                            console.log(`SQL visit_plan ::${visit.day_type_plan} and influencer ::${visit['influencer__c']}`);
                            console.log('SQL result :::', result);
                          }
                        break;
                        case 'a0C0w000004ie7EEAQ':
                          if(visit['influencer__c']){
                            let update_sql = `update ${process.env.TABLE_SCHEMA_NAME}.pjp__c set pjp_compliance__c=true where sfid = '${visit['pjp_header__c']}'`
                            let result = await client.query(update_sql);
                            console.log('SQL :::', update_sql);
                            console.log(`SQL visit_plan ::${visit.day_type_plan} and influencer ::${visit['influencer__c']}`);
                            console.log('SQL result :::', result);
                          }else {
                            let update_sql = `update ${process.env.TABLE_SCHEMA_NAME}.pjp__c set pjp_compliance__c=false where sfid = '${visit['pjp_header__c']}'`
                            let result = await client.query(update_sql);
                            console.log('SQL :::', update_sql);
                            console.log(`SQL visit_plan ::${visit.day_type_plan} and influencer__c ::${visit['influencer__c']}`);
                            console.log('SQL result :::', result);
                          }
                        break;
                        // case 'a0C0w000004jB7sEAE':
                        //   if(visit['']){
                        //     let update_sql = `update ${process.env.TABLE_SCHEMA_NAME}.pjp__c set pjp_compliance__c=true where sfid = '${visit['pjp_header__c']}'`
                        //     let result = await client.query(update_sql);
                        //     console.log('SQL :::', update_sql);
                        //     console.log(`SQL visit_plan ::${visit.day_type_plan} and grid ::${visit['']}`);
                        //     console.log('SQL result :::', result);
                        //   }else {
                        //     let update_sql = `update ${process.env.TABLE_SCHEMA_NAME}.pjp__c set pjp_compliance__c=false where sfid = '${visit['pjp_header__c']}'`
                        //     let result = await client.query(update_sql);
                        //     console.log('SQL :::', update_sql);
                        //     console.log(`SQL visit_plan ::${visit.day_type_plan} and grid ::${visit['']}`);
                        //     console.log('SQL result :::', result);
                        //   }
                        // break;
                        // case 'a0C0w000004jB87EAE':
                        //   if(visit['']){
                        //     let update_sql = `update ${process.env.TABLE_SCHEMA_NAME}.pjp__c set pjp_compliance__c=true where sfid = '${visit['pjp_header__c']}'`
                        //     let result = await client.query(update_sql);
                        //     console.log('SQL :::', update_sql);
                        //     console.log(`SQL visit_plan ::${visit.day_type_plan} and grid ::${visit['grid__c']}`);
                        //     console.log('SQL result :::', result);
                        //   }else {
                        //     let update_sql = `update ${process.env.TABLE_SCHEMA_NAME}.pjp__c set pjp_compliance__c=false where sfid = '${visit['pjp_header__c']}'`
                        //     let result = await client.query(update_sql);
                        //     console.log('SQL :::', update_sql);
                        //     console.log(`SQL visit_plan ::${visit.day_type_plan} and grid ::${visit['grid__c']}`);
                        //     console.log('SQL result :::', result);
                        //   }
                        // break;
                    }
                    }
                  }
              //}
          }
      }

      console.log('Cron job ended for PJP Compliance')
    } catch (err) {
      console.log('Error in PJP Compliance Job:', err);
    }
  },
  { scheduled: true }
);

// const minute2 = 0;
// const hour2 = 6;

// const pjpJob2 = cron.schedule(
//   `${minute2} ${hour2} * * *`,
//   async () => {
//     try {
//       console.log('Cron job 2 started for Credit Limit low and sauda limit Low');
//       let fields = ['account.sfid, account.balance_credit_limit__c, account.total_credit_limit__c, team__c.sfid as team_sfid'];
//       const tableName = SF_ACCOUNT_TABLE_NAME;
//       const WhereClouse = [];
//       let offset = '0',
//         limit = '1000';
//       let joins = [
//         {
//           type: 'LEFT',
//           table_name: 'team__c',
//           p_table_field: `account.sfid`,
//           s_table_field: `team__c.account__c`,
//         },
//       ];
//       WhereClouse.push({ fieldName: 'account.sfid', type: 'NOTNULL' });
//       WhereClouse.push({ fieldName: 'team__c.sfid', type: 'NOTNULL' });
//       WhereClouse.push({ fieldName: 'account.balance_credit_limit__c', type: 'NOTNULL' });
//       WhereClouse.push({ fieldName: 'account.total_credit_limit__c', type: 'NOTNULL' });
//       WhereClouse.push({ fieldName: 'account.account_type__c', fieldValue: ['Dealer', 'Distributor'], type: 'IN' });
//       let acc_sql = qry.fetchAllWithJoinQry(fields, tableName, joins, WhereClouse, offset, limit);
//       let accounts = await client.query(acc_sql);
//       if (accounts.rowCount) {
//         for (let i = 0; i < accounts.rows.length; i++) {
//           const { balance_credit_limit__c, total_credit_limit__c, team_sfid } = accounts.rows[i];
//           /**
//            * Logic - Balance_Credit_Limit__c < 20% of Total_Credit_Limit__c
//            */
//           if (balance_credit_limit__c < total_credit_limit__c * 0.2) {
//             let fields = ['*'],
//               WhereClouse = [],
//               offset = '0',
//               limit = '1';
//             WhereClouse.push({ fieldName: 'user__c', fieldValue: team_sfid });
//             let push_notification_sql = qry.SelectAllQry(fields, SF_PUSH_NOTIFICATIONS_TABLE_NAME, WhereClouse, offset, limit, ' order by createddate desc');
//             let pushNotifications = await client.query(push_notification_sql);
//             if (pushNotifications.rowCount && pushNotifications.rows[0].firebase_token__c) {
//               const title = 'Credit limit is low!';
//               const body = 'Your current credit limit is low. kindly make payment.';
//               sendnotificationToDevice(pushNotifications.rows[0], { title, body }, { title });
//             }
//           }
//         }
//       }
//       /**
//        * Distributor Low Sauda Push Notification
//        * Notification should be appeared only when the sum of Balance_Quantity__c < 5
//        */
//       let dis_sql = `select t.sfid as team_sfid, SUM(s.balance_quantity__c) from salesforce.sauda__c as s left join salesforce.account as a on a.sfid = s.distributor_name__c left join salesforce.team__c as t on t.account__c = a.sfid where a.account_type__c = 'Distributor' and t.sfid is not null group by team_sfid having SUM(s.balance_quantity__c) < 5000`;
//       let dis_result = await client.query(dis_sql);
//       if (dis_result.rowCount) {
//         for (let i = 0; i < dis_result.rows.length; i++) {
//           const { team_sfid } = dis_result.rows[i];
//           let fields = ['*'],
//             WhereClouse = [],
//             offset = '0',
//             limit = '1';
//           WhereClouse.push({ fieldName: 'user__c', fieldValue: team_sfid });
//           let push_notification_sql = qry.SelectAllQry(fields, SF_PUSH_NOTIFICATIONS_TABLE_NAME, WhereClouse, offset, limit, ' order by createddate desc');
//           let pushNotifications = await client.query(push_notification_sql);
//           if (pushNotifications.rowCount && pushNotifications.rows[0].firebase_token__c) {
//             const title = 'Sauda limit is low!';
//             const body = 'Book new Sauda until it gets Over!!';
//             sendnotificationToDevice(pushNotifications.rows[0], { title, body }, { title });
//           }
//         }
//       }
//     } catch (err) {
//       console.log('Error in pjpJob:', err);
//     }
//   },
//   { scheduled: true }
// );

const AllPjpCompilance = cron.schedule(
  `30 23 * * *`, //for Everyday At 11 Pm In Night
  async () => {
    try {
      let visit_completed_status_id = await func.getPicklistSfid('Visit__c','Visit_Status__c','Completed')
      let today_date_pjp = dtUtil.todayDate();
      let all_pjp_sql = `SELECT * FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_PJP_TABLE_NAME} where pjp_date__c <= '${today_date_pjp}' and sfid is not null`  //For Testing
      let all_pjp_sql_res = await client.query(all_pjp_sql);
      //let all_pjp_sql = `SELECT * FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_LEAD_TABLE_NAME}`
      console.log("_all_pjp_sql", all_pjp_sql_res.rows.length)
      if (all_pjp_sql_res.rows.length > 0) {
        for (let i = 0; i < all_pjp_sql_res.rows.length; i++) {
          //For Grid Compliance Part 
          /*1) We Find Out All The Visit Regariding to Each Pjp and Check Which 
          Visit Are Complete and Which Are Not WE Put True In All The Visits Which Are Completed Resr False */
          let complete_visit_id_arr = [];
          // let uncomplete_visit_id_arr = [];
          //let visit_town_arr = [];
          let date_arr = [];
          let pjp_town_arr = [];
          let all_visit_sql = `SELECT * FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_VISIT_TABLE_NAME} where pjp_header__c = '${all_pjp_sql_res.rows[i]['sfid']}'`
          let all_visit_sql_res = await client.query(all_visit_sql);
          // console.log(`VISIT__LENGTH ----- ${all_visit_sql_res.rows.length}`);
          if (all_visit_sql_res.rows.length > 0) {
            // console.log(`--> ${all_visit_sql_res.rows}`);
            all_visit_sql_res.rows.map((id) => {
              let date_variable = dtUtil.ISOtoLocal(id['createddate'])
              date_arr.push(date_variable)
              date_arr = sort.removeDuplicates(date_arr);
              if (id['visit_status__c'] == visit_completed_status_id && id['grid__c'] != null) { //completed
                complete_visit_id_arr.push(id['sfid'])
                //Store All Town On WHich Visit Created On 
                // visit_town_arr.push(id['town__c'])
                // visit_town_arr = sort.removeDuplicates(visit_town_arr);
                // console.log("complete_visit_id_arr", complete_visit_id_arr)
              }
              // else{
              //     uncomplete_visit_id_arr.push(id['sfid'])
              // }
            })
            // console.log(`DATE ARRAY ------> ${date_arr}`);
            for (let j = 0; j < complete_visit_id_arr.length; j++) {
              let grid_compliance_update = `update ${process.env.TABLE_SCHEMA_NAME}.${SF_VISIT_TABLE_NAME} set grid_compliance__c='true' where sfid = '${complete_visit_id_arr[j]}'`
              let grid_compliance_update_res = await client.query(grid_compliance_update);

              //let grid_compliance_update_res = await client.query(grid_compliance_update);
              //console.log(`Grid COmpliance Update For SFID ${complete_visit_id_arr[j]} ----> ${grid_compliance_update_res}`);
              // if(uncomplete_visit_id_arr.length > 0){
              // }
            }
          }


          //FOR TOWN COMPLIANCE
          // console.log(`For Town Compliance`);
          let pjp_town_sql = `SELECT * FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_PJP_TOWN_ID_TABLE_NAME} where pjp_id__c = '${all_pjp_sql_res.rows[i]['sfid']}'`
          let pjp_town_sql_res = await client.query(pjp_town_sql);
          // console.log("Pjp_town_sql", pjp_town_sql_res.rows.length)
          if (pjp_town_sql_res.rows.length > 0) {
            pjp_town_sql_res.rows.map((ids) => {
              pjp_town_arr.push(ids['town_name__c'])
              pjp_town_arr = sort.removeDuplicates(pjp_town_arr);
            })
            for (let z = 0; z < pjp_town_arr.length; z++) {
              let compare_pjp_visit_town = `SELECT * 
                                            FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_VISIT_TABLE_NAME} 
                                            where pjp_header__c = '${all_pjp_sql_res.rows[i]['sfid']}'
                                            and town__c = '${pjp_town_arr[z]}'
                                            and visit_status__c = '${visit_completed_status_id}'`
              let compare_pjp_visit_town_res = await client.query(compare_pjp_visit_town);
              if (compare_pjp_visit_town_res.rows.length > 0) {
                let update_town_compliance = `update ${process.env.TABLE_SCHEMA_NAME}.${SF_PJP_TOWN_ID_TABLE_NAME} set town_compliance__c='true' where pjp_id__c = '${all_pjp_sql_res.rows[i]['sfid']}' and town_name__c = '${pjp_town_arr[z]}'`
                let update_town_compliance_res = await client.query(update_town_compliance);

                // console.log(`PJP TOWN ID UPDATE -----> ${update_town_compliance}, ${update_town_compliance_res}`);
              }
            }
          }

          //PJP Compliance
          // console.log(`For PJP Compliance`);
          let today_date = dtUtil.ISOtoLocal(dtUtil.todayDate());
          let index = date_arr.indexOf(today_date);
          // console.log('Index------>',today_date);
          if (index !== -1) {
            date_arr.splice(index, 1);
          }
          // console.log("date_arr", date_arr)
          // let visit_check_sql = `SELECT *,CAST(createddate as date) as new_date FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_VISIT_TABLE_NAME} where visit_status__c = 'a050w000002jNnPAAU' and new_date IN ('${date_arr.join("','")}')`
          if (date_arr.length > 0) {
            let visit_check_sql = `SELECT *,CAST(createddate as date) FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_VISIT_TABLE_NAME} where visit_status__c = '${visit_completed_status_id}' and createddate::date IN ('${date_arr.join("','")}')`
            // console.log(`Visit Check SQl pjp compilance ----- ${visit_check_sql}`);
            let visit_check_res = await client.query(visit_check_sql);
            if (visit_check_res.rows.length > 0) {
              let update_pjp_compliance = `update ${process.env.TABLE_SCHEMA_NAME}.${SF_PJP_TABLE_NAME} set pjp_compliance__c='true' where sfid = '${all_pjp_sql_res.rows[i]['sfid']}'`
              let result = await client.query(update_pjp_compliance)
              // console.log(`Update PJP compliance ---> ${result}`);
            }
          }

          //day type exception
          if (all_pjp_sql_res.rows[i]['day_type_gtm__c'] == all_pjp_sql_res.rows[i]['day_type_plan__c']) {
            let day_type_compliance = `update ${process.env.TABLE_SCHEMA_NAME}.${SF_PJP_TABLE_NAME} set day_type_exception__c='true' where  pjp__c.day_type_gtm__c=pjp__c.day_type_plan__c`
            let day_type_compliance_res = await client.query(day_type_compliance)
            console.log(`Update PJP compliance ---> ${day_type_compliance}`);
            console.log("update_sql2::::::", day_type_compliance_res)
          } else {
            let day_type_compliance2 = `update ${process.env.TABLE_SCHEMA_NAME}.${SF_PJP_TABLE_NAME} set day_type_exception__c='false' where  pjp__c.day_type_gtm__c!=pjp__c.day_type_plan__c`
            let day_type_compliance_res2 = await client.query(day_type_compliance2)
            console.log(`Update PJP compliance ---> ${day_type_compliance2}`);
            console.log("update_sql::::::", day_type_compliance_res2)
          }

        }
      }
    } catch (e) {
      console.log("Error In All Pjp compilance Test ---->", e);
    }
  },
  { scheduled: true }
);



// const pjpAllCompilance = cron.schedule(
//   `00 04 * * *`, //for Everyday At 04 Am In morning
//   //`07 16 * * *`, //just for testing Everyday At 04 Pm In Night
//   async () => {
//     try {

//       //@DOUBTS ---> Logic Dono Ka Cnfirm Krna h Ek baar,
//       //check in check out ki jagah complete pr check lga skte h 
//       //agar aapke diffrent town ho pjp m or unme se ek hi town pr koi visit complete hui ho toh dono ka compliance true krna h ya ek ka or isme complete ka check bhi lgana h kya

//       let all_pjp_sql = `SELECT * FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_PJP_TABLE_NAME} where sfid is not null`  //For Testing
//       let all_pjp_sql_res = await client.query(all_pjp_sql);
//       //let all_pjp_sql = `SELECT * FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_LEAD_TABLE_NAME}`
//       //console.log('date_variable:::::::::::::::::::::',all_pjp_sql_res.rows[0]['createddate'])
//       //console.log('all_pjp_sql:::::::::::::::::::::',all_pjp_sql)

//       if (all_pjp_sql_res.rows.length > 0) {
//         for (let i = 0; i < all_pjp_sql_res.rows.length; i++) {
//           //For Grid Compliance Part 
//           /*1) We Find Out All The Visit Regariding to Each Pjp and Check Which 
//           Visit Are Complete and Which Are Not WE Put True In All The Visits Which Are Completed Resr False */
//           console.log('check::::::::::::::', all_pjp_sql_res.rows[i]['sfid'])

//           let complete_visit_id_arr = [];
//           // let uncomplete_visit_id_arr = [];
//           //let visit_town_arr = [];
//           let date_arr = [];
//           let pjp_town_arr = [];
//           let all_visit_sql = `SELECT * FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_VISIT_TABLE_NAME} where pjp_header__c = '${all_pjp_sql_res.rows[i]['sfid']}'`
//           let all_visit_sql_res = await client.query(all_visit_sql);
//           console.log(`LENGTH ----- ${all_visit_sql_res.rows.length}`);
//           console.log(`visit ----- `, all_visit_sql);
//           if (all_visit_sql_res.rows.length > 0) {
//             console.log(`--> ${all_visit_sql_res.rows}`);
//             all_visit_sql_res.rows.map((id) => {
//               let date_variable = dtUtil.ISOtoLocal(id['createddate'])
//               console.log('date_variable:::::::::::::::::::::', date_variable)
//               if (id['createddate'] != null) {  //check
//                 date_arr.push(date_variable)
//               }
//               date_arr = sort.removeDuplicates(date_arr);
//               if (id['visit_status__c'] == 'a050w000002jNnPAAU' && id['grid__c'] != null) {
//                 complete_visit_id_arr.push(id['sfid'])
//                 //Store All Town On WHich Visit Created On 
//                 // visit_town_arr.push(id['town__c'])
//                 // visit_town_arr = sort.removeDuplicates(visit_town_arr);
//               }
//               // else{
//               //     uncomplete_visit_id_arr.push(id['sfid'])
//               // }
//             })
//             console.log('complete_visit_id_arr:::::::::::::', complete_visit_id_arr)
//             console.log(`DATE ARRAY ------> ${date_arr}`);
//             for (let j = 0; j < complete_visit_id_arr.length; j++) {
//               let grid_compliance_update = `update ${process.env.TABLE_SCHEMA_NAME}.${SF_VISIT_TABLE_NAME} set grid_compliance__c='true' where sfid = '${complete_visit_id_arr[j]}'`
//               let grid_compliance_update_res = await client.query(grid_compliance_update);
//               console.log(`Grid COmpliance Update For SFID::::::::: ${complete_visit_id_arr[j]} ----> ${grid_compliance_update_res}`);
//               console.log('grid compliance update sql::::::', grid_compliance_update)
//               // if(uncomplete_visit_id_arr.length > 0){
//               // }
//             }
//           }


//           //FOR TOWN COMPLIANCE
//           console.log(`For Town Compliance`);
//           let pjp_town_sql = `SELECT * FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_PJP_TOWN_ID_TABLE_NAME} where pjp_id__c = '${all_pjp_sql_res.rows[i]['sfid']}'`
//           let pjp_town_sql_res = await client.query(pjp_town_sql);
//           console.log('pjp town:::', pjp_town_sql)
//           if (pjp_town_sql_res.rows.length > 0) {
//             pjp_town_sql_res.rows.map((ids) => {
//               pjp_town_arr.push(ids['town_name__c'])
//               pjp_town_arr = sort.removeDuplicates(pjp_town_arr);
//             })
//             for (let z = 0; z < pjp_town_arr.length; z++) {
//               let compare_pjp_visit_town = `SELECT * 
//                                                 FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_VISIT_TABLE_NAME} 
//                                                 where pjp_header__c = '${all_pjp_sql_res.rows[i]['sfid']}'
//                                                 and town__c = '${pjp_town_arr[z]}'
//                                                 and visit_status__c = 'a050w000002jNnPAAU'`
//               let compare_pjp_visit_town_res = await client.query(compare_pjp_visit_town);
//               console.log("compare_pjp_visit_town:::::", compare_pjp_visit_town)
//               if (compare_pjp_visit_town_res.rows.length > 0) {
//                 let update_town_compliance = `update ${process.env.TABLE_SCHEMA_NAME}.${SF_PJP_TOWN_ID_TABLE_NAME} set town_compliance__c='true' where pjp_id__c = '${all_pjp_sql_res.rows[i]['sfid']}' and town_name__c = '${pjp_town_arr[z]}'`
//                 let update_town_compliance_res = await client.query(update_town_compliance)
//                 console.log(`PJP TOWN ID UPDATE -----> ${update_town_compliance}`);
//                 console.log(`PJP TOWN ID UPDATE result -----> ${update_town_compliance_res}`);
//               }
//             }
//           }

//           //PJP Compliance
//           console.log(`For PJP Compliance`);
//           let today_date = dtUtil.todayDate();
//           let index = date_arr.indexOf(today_date);
//           console.log("date_arr", date_arr)

//           //console.log('Index------>',index);
//           if (index !== -1) {
//             date_arr.splice(index, 1);
//           }
//           if (date_arr.length > 0) {
//             let visit_check_sql = `SELECT *,CAST(createddate as date)  FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_VISIT_TABLE_NAME} where visit_status__c = 'a050w000002jNnPAAU' and createddate::date IN ('${date_arr.join("','")}')`

//             //let visit_check_sql = `SELECT *,CAST(createddate as date) as new_date FROM ${process.env.TABLE_SCHEMA_NAME}.${SF_VISIT_TABLE_NAME} where visit_status__c = 'a050w000002jNnPAAU' and new_date IN ('${date_arr.join("','")}')`
//             console.log(`Visit Check SQl ----- ${visit_check_sql}`);
//             let visit_check_res = await client.query(visit_check_sql);
//             console.log("visit_check_res.rows.length::::", visit_check_res.rows.length)
//             if (visit_check_res.rows.length > 0) {
//               let update_pjp_compliance = `update ${process.env.TABLE_SCHEMA_NAME}.${SF_PJP_TABLE_NAME} set pjp_compliance__c='true' where sfid = '${all_pjp_sql_res.rows[i]['sfid']}'`
//               let result = await client.query(update_pjp_compliance)
//               console.log(`Update PJP compliance ---> ${result}`);
//               console.log("update_sql::::::", update_pjp_compliance)
//             }
//           }
//           //day type exception
//           if (all_pjp_sql_res.rows[i]['day_type_gtm__c'] == all_pjp_sql_res.rows[i]['day_type_plan__c']) {
//             let day_type_compliance = `update ${process.env.TABLE_SCHEMA_NAME}.${SF_PJP_TABLE_NAME} set day_type_exception__c='true' where  pjp__c.day_type_gtm__c=pjp__c.day_type_plan__c`
//             let day_type_compliance_res = await client.query(day_type_compliance)
//             console.log(`Update PJP compliance ---> ${day_type_compliance}`);
//             console.log("update_sql2::::::", day_type_compliance_res)
//           } else {
//             let day_type_compliance2 = `update ${process.env.TABLE_SCHEMA_NAME}.${SF_PJP_TABLE_NAME} set day_type_exception__c='false' where  pjp__c.day_type_gtm__c!=pjp__c.day_type_plan__c`
//             let day_type_compliance_res2 = await client.query(day_type_compliance2)
//             console.log(`Update PJP compliance ---> ${day_type_compliance2}`);
//             console.log("update_sql::::::", day_type_compliance_res2)
//           }
//         }
//       }
//       console.log('Cron job ended for Creating PJP')
//     } catch (e) {
//       console.log(`Error In Test -----> ${e}`);
//     }
//   }, { scheduled: true }
// );

module.exports = {
  pjpJob,
  visitComplianceJob,
  AllPjpCompilance
};
