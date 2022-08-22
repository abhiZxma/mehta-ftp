var moment = require('moment');



module.exports = {
    timestampToDate,
    todayDate,
    getDates,
    todayDatetime,
    removeMiliSec,
    currentMonth,
    utcTodayDate,
    ISOtoLocal,
    timeToLocal,
    getTotalHourMinute,
    getNumWeeksForMonth,
    getMonth,
    getYear,
    getDay,
    day_to_no,
    no_to_day,
    addDays,
    getEndOfMonth,
    daysInMonth,
    convertDatePickerTimeToMySQLTime,
    addMinutes,
    V2addMinutes,
    V2convertDatePickerTimeToMySQLTime,
    getTotalHourMinuteV2,
    dateZeroFix,
    minutesToTimeFormat
};

function getEndOfMonth(date){
    return moment(date).clone().endOf('month').format('YYYY-MM-DD');
}

function addDays(date, days){
    var result = new Date(date);
    result.setDate(result.getDate() + days);
    return moment(result).format('YYYY-MM-DD');
}

function day_to_no(day){
    let day_number_map = {
        'Monday': 1,
        'Tuesday': 2,
        'Wednesday': 3,
        'Thursday': 4,
        'Friday': 5,
        'Saturday': 6,
        'Sunday': 7
    }
    return day_number_map[day];
}

function no_to_day(no){
    let number_day_map = {
        1: 'Monday',
        2: 'Tuesday',
        3: 'Wednesday',
        4: 'Thursday',
        5: 'Friday',
        6: 'Saturday',
        7: 'Sunday'
    }
    return number_day_map[no];
}

function getDay(date){
    return moment(date).day();
}

function getMonth(date){
    let month = moment(date).month();
    return month + 1;
}

function getYear(date){
    return moment(date).year();
}

function getNumWeeksForMonth(year,month){
    date = new Date(year,month-1,1);
    day = date.getDay();
    numDaysInMonth = new Date(year, month, 0).getDate();
    return Math.ceil((numDaysInMonth + day) / 7);
}

function timestampToDate(timestamp,format){
    timestamp = removeMiliSec(timestamp)
    
    return moment.unix(timestamp).format(format);
}
function todayDate(){
   
    return  moment().format('YYYY-MM-DD');
}

function utcTodayDate(){
    return moment.utc().format('YYYY-MM-DD');
}

function currentMonth(){
   
    return  moment().format('MM');
   
}

function todayDatetime(){
   
    return  moment().format('YYYY-MM-DD HH:mm:ss');
   
} 

function getDates(day, till_date_timestamp, from_date_timestamp) {
    allDates = [];
    try{
        from_date_timestamp = removeMiliSec(from_date_timestamp)
        var monday = moment.unix(from_date_timestamp)
        .startOf('month')
        .day(day);
        if (monday.date() > 7) monday.add(7,'d');
        //till_date_timestamp = moment(till_date).format('X');
    
        while (till_date_timestamp > moment(monday).format('X')) {
           // if (moment(monday).format('X') > from_date_timestamp) {

                allDates.push(moment(monday).format('YYYY-MM-DD'));
                monday.add(7, 'd');
           // }
        }
    }catch(e){
        console.log(e);
    }
    console.log(allDates);
    return allDates;
}

function removeMiliSec(timestamp) {
    
    if (typeof (timestamp) == 'string' && timestamp.length == 13) {
        timestamp = timestamp.substring(0, timestamp.length - 3);
    } else if (typeof (timestamp) == 'number'  && timestamp.toString().length == 13) {
        timestamp = timestamp.toString().substring(0, timestamp.toString().length - 3);
    }
    return timestamp;
}

function ISOtoLocal(date){
    let d = new Date(date);
    return d.getFullYear()+"-"+parseInt(d.getMonth()+1)+'-'+d.getDate()
}

function timeToLocal(value){
    let d = new Date(value);
    return d.getHours()+":"+d.getMinutes()+':'+d.getSeconds()
}

function getTotalHourMinute(start_date,start_time,end_date,end_time){
    //send date in format YYYY-MM-DD and time in format HH:mm:ss in these params
    let start_date_variable = moment(start_date+" "+start_time, 'YYYY-MM-DD HH:mm:ss');
    let end_date_variable = moment( end_date+" "+end_time, 'YYYY-MM-DD HH:mm:ss');
    //console.log(`Start_date ===> ${start_date_variable}  End_date ===>${end_date_variable}`);
    let duration = moment.duration(end_date_variable.diff(start_date_variable));
    let temp = duration.asMinutes();
    let total_hour_done = (temp/60).toFixed(2);
    let hour = Math.floor(temp/60);
    let minutes = temp % 60;
    let actual_minutes = (minutes/60).toFixed(2)
    //console.log(`${hour} hour & ${minutes.toFixed(0)} minutes ${temp1}`);
    return [temp,total_hour_done,hour,minutes,actual_minutes]

}

function daysInMonth (month, year) {
    return new Date(year, month, 0).getDate();
}

//This Function Convert ( Wed Apr 06 2022 19:00:38 GMT+0530 (India Standard Time) ) this to this (2022-04-06 19:00:38)
function convertDatePickerTimeToMySQLTime(str) {
    let month, day, year, hours, minutes, seconds;
    date = new Date(str),
    month = ("0" + (date.getMonth() + 1)).slice(-2),
    day = ("0" + date.getDate()).slice(-2);
    hours = ("0" + date.getHours()).slice(-2);
    minutes = ("0" + date.getMinutes()).slice(-2);
    seconds = ("0" + date.getSeconds()).slice(-2);
    let mySQLDate = [date.getFullYear(), month, day].join("-");
    let mySQLTime = [hours, minutes, seconds].join(":");
    return [mySQLDate, mySQLTime].join(" ");
}

async function addMinutes(dt, minutes,operation) {
    if(operation == 'subtract'){
        console.log(`Subtract`);
        let temp = new Date(dt.getTime() - minutes*60000);
        console.log(`Temp ----> ${temp}`);
        let modified_date = await convertDatePickerTimeToMySQLTime(temp)
        return modified_date
    }
    if(operation == 'add'){
        console.log(`add`);
        let temp = new Date(dt.getTime() + minutes*60000);
        let modified_date = await convertDatePickerTimeToMySQLTime(temp)
        return modified_date
    }
}

function V2convertDatePickerTimeToMySQLTime(str) {
    let month, day, year, hours, minutes, seconds;
    date = new Date(str),
    month = ("0" + (date.getMonth() + 1)).slice(-2),
    day = ("0" + date.getDate()).slice(-2);
    hours = ("0" + date.getHours()).slice(-2);
    minutes = ("0" + date.getMinutes()).slice(-2);
    seconds = ("0" + date.getSeconds()).slice(-2);
    let mySQLDate = [date.getFullYear(), month, day].join("-");
    let mySQLTime = [hours, minutes, seconds].join(":");
    return [mySQLTime].join(" ");
}


async function V2addMinutes(dt, minutes,operation) {
    if(operation == 'subtract'){
        console.log(`Subtract`);
        let temp = new Date(dt.getTime() - minutes*60000);
        let modified_date = await V2convertDatePickerTimeToMySQLTime(temp)
        return modified_date
    }
    if(operation == 'add'){
        console.log(`add`);
        let temp = new Date(dt.getTime() + minutes*60000);
        let modified_date = await V2convertDatePickerTimeToMySQLTime(temp)
        return modified_date
    }
}

function getTotalHourMinuteV2(start_time,end_time){

    let start = moment(start_time)
    let end = moment(end_time)
    console.log(`Start_date ===> ${start}  End_date ===>${end}`);
    let duration = moment.duration(end.diff(start));
    console.log(`Without Round -----> ${duration.asMinutes()}`);
    let temp = Math.round(duration.asMinutes());
    console.log(`Temp -----> ${temp}`);
    let total_hour_done = (temp/60).toFixed(2);
    let hour = Math.floor(temp/60);
    let minutes = temp % 60;
    let actual_minutes = (minutes/60).toFixed(2)
    //console.log(`${hour} hour & ${minutes.toFixed(0)} minutes ${temp1}`);
    return [temp,total_hour_done,hour,minutes,actual_minutes]

}

function dateZeroFix(date){
    date = date.toString()
    date = date.split("-")
    console.log(`date -----> ${date[1]}`);
    let year_part = date[0].toString()
    let month_part = date[1].toString()
    let day_part = date[2].toString()

    if(month_part.length < 2 ){
        month_part = `0${month_part}`
    }
    if(day_part.length < 2 ){
        day_part = `0${day_part}`
    }
    date = `${year_part}-${month_part}-${day_part}`
    console.log(`Month ---> ${month_part} Day ----> ${day_part}`);
    return date
}

async function minutesToTimeFormat(minutes){
    try{
        let m = minutes % 60;
        let h = (minutes - m) / 60;
        let format = h.toString() + ":" + (m < 10 ? "0" : "") + m.toString() + ":00" 
        //console.log(`Formatted ---> ${format}`);
        return format
    }catch(e){
        console.log(`Error In Minutes Fn -----> ${e}`);
    }
}


