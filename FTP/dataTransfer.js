        const cron = require('node-cron');

        const qry = require(`${PROJECT_DIR}/utility/selectQueries`);
        const dtUtil = require(`${PROJECT_DIR}/utility/dateUtility`);
        const moment = require('moment');
        const validation = require(`${PROJECT_DIR}/utility/validation`);
        //const uuidv4 = require('uuid/v4');
        // const whatsapp = require(`${PROJECT_DIR}/utility/whatsApp`);
        // const notification = require(`${PROJECT_DIR}/utility/notifications`);
        const func = require(`${PROJECT_DIR}/utility/functionalUtility`);
        // const openvpnmanager = require('node-openvpn');
        const mail = require(`${PROJECT_DIR}/utility/sendEmail`);
        const { performance } = require('perf_hooks');
        const nodemailer = require('nodemailer');
        const uuidv4=require('uuid')
        const path = require('path');
        const fs = require('fs');
        const csv = require('csvtojson')
        const db = require(`${PROJECT_DIR}/utility/selectQueries`);
        const logs = require('./logFIle')
        const createCsvWriter = require('csv-writer');
        const Client_ftp = require('ftp');
        const { insertManyRecord,updateManyCustom } = require('../utility/selectQueries');
        console.log("__dirname",__dirname)
        // __dirname='FTP'
        console.log("new__dirname",__dirname)


        async function file_upload() {
            try {
                var c = new Client_ftp();
                let config = {
                    host: '10.12.100.2',
                    port: 1993,
                    user: 'zoxima',
                    password: 'Cpil#1986'
                }
                c.connect(config);
                c.on('ready', function () {
                    // upload area logs
                    fs.readdir(path.join(__dirname, OUT_area_PATH), (err, files) => {
                        if (err) throw err
                        for (let i = 0; i < files.length; i++) {
                            const fileName = files[i];
                            console.log("fileName", path.join(__dirname, OUT_area_PATH, fileName))
                            c.append(path.join(__dirname, OUT_area_PATH, fileName), `/zoxima/upload_logs/${fileName}`, false, function (err) {
                                if (err) throw err;
                                c.end();
                            });
                            fs.unlink(path.join(__dirname, OUT_area_PATH, fileName), err => {
                                if (err) throw err;
                            });
                        }
                    })
                    // upload dealer logs
                    fs.readdir(path.join(__dirname, OUT_dealer_DATA_PATH), (err, files) => {
                        if (err) throw err
                        for (let i = 0; i < files.length; i++) {
                            const fileName = files[i];
                            console.log("fileName", path.join(__dirname, OUT_dealer_DATA_PATH, fileName))
                            c.append(path.join(__dirname, OUT_dealer_DATA_PATH, fileName), `/zoxima/upload_logs/${fileName}`, false, function (err) {
                                if (err) throw err;
                                c.end();
                            });
                            fs.unlink(path.join(__dirname, OUT_dealer_DATA_PATH, fileName), err => {
                                if (err) throw err;
                            });
                        }
                    })
                    // upload outstanding logs
                    fs.readdir(path.join(__dirname, OUT_outstanding_DATA_PATH), (err, files) => {
                        if (err) throw err
                        for (let i = 0; i < files.length; i++) {
                            const fileName = files[i];
                            console.log("fileName", path.join(__dirname, OUT_outstanding_DATA_PATH, fileName))
                            c.append(path.join(__dirname, OUT_outstanding_DATA_PATH, fileName), `/zoxima/upload_logs/${fileName}`, false, function (err) {
                                if (err) throw err;
                                c.end();
                            });
                            fs.unlink(path.join(__dirname, OUT_outstanding_DATA_PATH, fileName), err => {
                                if (err) throw err;
                            });
                        }
                    })
                    // upload pending order
                    fs.readdir(path.join(__dirname, OUT_pending_order_DATA_PATH), (err, files) => {
                        if (err) throw err
                        for (let i = 0; i < files.length; i++) {
                            const fileName = files[i];
                            console.log("fileName", path.join(__dirname, OUT_pending_order_DATA_PATH, fileName))
                            c.append(path.join(__dirname, OUT_pending_order_DATA_PATH, fileName), `/zoxima/upload_logs/${fileName}`, false, function (err) {
                                if (err) throw err;
                                c.end();
                            });
                            fs.unlink(path.join(__dirname, OUT_pending_order_DATA_PATH, fileName), err => {
                                if (err) throw err;
                            });
                        }
                    })
                    // upload pending order
                    fs.readdir(path.join(__dirname, OUT_credit_limit_PATH), (err, files) => {
                        if (err) throw err
                        for (let i = 0; i < files.length; i++) {
                            const fileName = files[i];
                            console.log("fileName", path.join(__dirname, OUT_credit_limit_PATH, fileName))
                            c.append(path.join(__dirname, OUT_credit_limit_PATH, fileName), `/zoxima/upload_logs/${fileName}`, false, function (err) {
                                if (err) throw err;
                                c.end();
                            });
                            fs.unlink(path.join(__dirname, OUT_credit_limit_PATH, fileName), err => {
                                if (err) throw err;
                            });
                        }
                    })
                    // upload pending order
                    fs.readdir(path.join(__dirname, OUT_product_item_DATA_PATH), (err, files) => {
                        if (err) throw err
                        for (let i = 0; i < files.length; i++) {
                            const fileName = files[i];
                            console.log("fileName", path.join(__dirname, OUT_product_item_DATA_PATH, fileName))
                            c.append(path.join(__dirname, OUT_product_item_DATA_PATH, fileName), `/zoxima/upload_logs/${fileName}`, false, function (err) {
                                if (err) throw err;
                                c.end();
                            });
                            fs.unlink(path.join(__dirname, OUT_product_item_DATA_PATH, fileName), err => {
                                if (err) throw err;
                            });
                        }
                    })
                });
            } catch (e) {
                console.log("error while uploading file", e)
            }
        }
        function trimzero(str){
            if(str){
                str = (str * 1).toString();
            }
            return str
        }
        async function attendance_file_upload() {
            try {
                var c = new Client_ftp();
                let config = {
                    host: 'mail.centuryply.co.in',
                    port: 21,
                    user: 'ftpsalesforce-centuryplycoin',
                    password: 'Abcd@7654324'
                }
                c.connect(config);
                c.on('ready', function () {
                    c.list('./zoxima_check/cpil_data', false, function (err, list) {
                        if (err) throw err;
                        console.dir(list);
                        for (let i = 0; i < list.length; i++) {
                            const element = list[i];
                            c.get(`/zoxima_check/cpil_data/${element.name}`, function (err, stream) {
                                if (err) throw err;
                                stream.once('close', function () { c.end(); });
                                if (element.name.startsWith("outstanding")) {
                                    stream.pipe(fs.createWriteStream(path.join(__dirname, IN_outstanding_data_PATH, element.name)));
                                }
                                if (element.name.startsWith("PrimarySales")) {
                                    stream.pipe(fs.createWriteStream(path.join(__dirname, IN_Primary_sales_DATA_PATH, element.name)));
                                }
                                if (element.name.startsWith("Dealer baseline")) {
                                    stream.pipe(fs.createWriteStream(path.join(__dirname, IN_dealer_data_PATH, element.name)));
                                }
                                if (element.name.startsWith("pendingorder")) {
                                    stream.pipe(fs.createWriteStream(path.join(__dirname, IN_pending_order_DATA_PATH, element.name)));
                                }
                            });
                        }
                    });
                    fs.readdir(path.join(__dirname, OUT_attendance_file_to_ftp_DATA_PATH), (err, files) => {
                        if (err) throw err
                        for (let i = 0; i < files.length; i++) {
                            const fileName = files[i];
                            console.log("fileName", path.join(__dirname, 'FTP/Files/OUT/attendance_file_to_ftp', fileName))
                            c.append(path.join(__dirname, OUT_attendance_file_to_ftp_DATA_PATH, fileName), `/salesforce_attendance/${fileName}`, false, function (err) {
                                if (err) throw err;
                                c.end();
                            });
                            fs.unlink(path.join(__dirname, 'FTP/Files/OUT/attendance_file_to_ftp', fileName), err => {
                                if (err) throw err;
                            });
                        }
                    })
                });
            } catch (e) {
                console.log("error while uploading file", e)
            }
        }
        function dateZeroFix(date) {
            let strdate = date.toString()
            let mydate = strdate.split(".")
            //console.log(`date -----> ${mydate}`);
            let month_part = mydate[1].toString()
            let day_part = mydate[0].toString()
            let year_part = mydate[2].toString()
            let new_date = `${year_part}-${month_part}-${day_part}`
            return new_date
        }
        function dateZeroFix2(date) {
            let strdate = date.toString()
            let month_part = strdate.slice(4, 6)
            let day_part = strdate.slice(6, 8)
            let year_part = strdate.slice(0, 4)
            let new_date = `${year_part}-${month_part}-${day_part}`
            return new_date
        }
        function sendMailFTP(subject, text, folder_name, file_name) {
            const FileDirectoryPath = path.join(__dirname, `${OUT_DIR_PATH}/${folder_name}/${file_name}`);


            var transporter = nodemailer.createTransport({
                service: 'gmail',
                auth: {
                    user: 'api.backend@zoxima.com',
                    pass: 'sqrzcqidvbhfowbj',
                }
            });

            var mailOptions = {
                //team member
                to: MAIL_OPTIONS_TO,
                cc: MAIL_OPTIONS_CC,
                subject: `${subject}`,
                text: `${text}`,
                attachments: [
                    {   // file on disk as an attachment
                        filename: `${file_name}`,
                        path: FileDirectoryPath
                    }
                ]
            };

            transporter.sendMail(mailOptions, function (error, info) {
                if (error) {
                    console.log(error);
                } else {
                    console.log('Email sent: ' + info.response);
                }
            });
        }

        function checkPan(pan) {
            if(pan.length!=10){
                return false
            }else{
                return true
            }
        }
        function apostrophe(str1){
            // let str1="South-B'lore-Fur-RDC"
            if(str1){
                let str2=str1.split("'").join(" ");
                console.log("after replace",str2);
                return str2
            }
            else{
                return str1
            }
        }

        //40 15 * * *
        const all_file_download = cron.schedule(`40 15 * * *`,
        async ()=>{
            try {
                
                var c = new Client_ftp();
                let config = {
                    host: '10.12.100.2',
                    port: 1993,
                    user: 'zoxima',
                    password: 'Cpil#1986'
                }
                c.connect(config);
                c.on('ready', function () {
                    let today_date = new Date(dtUtil.todayDate())
                    let tod_year = today_date.getFullYear()
                    let tod_month = today_date.getMonth()
                    // let tod_month = `07`

                    if (tod_month < 10) {
                        tod_month = `0${tod_month + 1}`
                    }
                    let tod_date = today_date.getDate()
                    // let tod_date = `31`
                    if (tod_date < 10) {
                        tod_date = `0${tod_date}`
                    }
                    let pending_order_lam = 'pendingorder_lam.csv'
                    let pending_order_panel = 'pendingorder_panel.csv'
                    let outstanding = `outstanding${tod_year}${tod_month}${tod_date}`
                    let primary_sales = `PrimarySales${tod_year}${tod_month}${tod_date}`
                    let dealer = `Dealer${tod_year}${tod_month}${tod_date}`
                    let branch = `Branchmaster${tod_year}${tod_month}${tod_date}`
                    let territory = `Teritorrymaster${tod_year}${tod_month}${tod_date}`
                    let credit_limit = `Panelcreditlimit${tod_year}${tod_month}${tod_date}`
                    console.log(`Branch file name ---> ${branch}`);
                    c.list('./', false, function (err, list) {
                        if (err) throw err;
                        for (let i = 0; i < list.length; i++) {
                            const element = list[i];
                            if (element.name == pending_order_panel || element.name == pending_order_lam ||element.name.startsWith(branch) || element.name.startsWith(territory) || element.name.startsWith(primary_sales) || element.name.startsWith(credit_limit) || element.name.startsWith(outstanding) || element.name.startsWith(territory) || element.name.startsWith(branch) || element.name.startsWith(dealer))
                                c.get(`/${element.name}`, async function (err, stream) {
                                    if (err) throw err;
                                    stream.once('close', function () { c.end(); });
                                    console.log("pathtodownload", path.join(__dirname, '/Files/IN', element.name))
                                    if (element.name.startsWith(branch) || element.name.startsWith(territory)) {
                                        stream.pipe(fs.createWriteStream(path.join(__dirname, IN_area_data_PATH, element.name)));
                                    }
                                    if (element.name.startsWith(outstanding)) {
                                        stream.pipe(fs.createWriteStream(path.join(__dirname, IN_outstanding_data_PATH, element.name)));
                                    }
                                    if (element.name.startsWith(primary_sales)) {
                                        stream.pipe(fs.createWriteStream(path.join(__dirname, IN_Primary_sales_DATA_PATH, element.name)));
                                    }
                                    if (element.name.startsWith(dealer)) {
                                        stream.pipe(fs.createWriteStream(path.join(__dirname, IN_dealer_data_PATH, element.name)));
                                    }
                                    if (element.name == pending_order_lam || element.name == pending_order_panel) {
                                        stream.pipe(fs.createWriteStream(path.join(__dirname, IN_pending_order_DATA_PATH, element.name)));
                                    }
                                    // if (element.name.startsWith(credit_limit)) {
                                    //     stream.pipe(fs.createWriteStream(path.join(__dirname, '/Files/IN/credit_limit', element.name)));
                                    // }
                                });
                        }
                    });
                });
        console.log("end here")
            } catch (e) {
                console.log("error while uploading file", e)
            }
        },
        { scheduled: true })
        // Fully optimized product
        const product_Upload_And_Mapping=cron.schedule(`22 11 * * *`,
        async ()=>{
            try {
                let cron_name = 'product_Upload_And_Mapping'
                const productDirectoryPath = path.join('FTP', IN_product_item_DATA_PATH);
                fs.readdir(productDirectoryPath, async (err, files) => {
                    if (err) {
                        return console.log('Unable to load directory: ', err);
                    }
                    console.log("started mapping product")
                    for (let index = 0; index < files.length; index++) {
                        const file = files[index];
                        if(file!='.gitkeep'){
                        let failed = 0;
                        const dataPush = []
                        let file_path = path.join(productDirectoryPath, file)
                        const table_name = SF_PRODUCT_ITEM_TABLE_NAME
                        const jsonArray = await csv().fromFile(file_path);
                        let temp_j=0;
                        let i_loop_length=jsonArray.length
                        let lob_type_code={}
                        let lob_type = await func.getPicklistSfid('Category__c','Category_Type__c','LOB')
                        let get_lob_sql = `SELECT LOWER(name) as name,sfid FROM salesforce.category__c where category_type__c='${lob_type}'`
                        console.log("get_lob_sql",get_lob_sql)
                        let get_lob_result = await client.query(get_lob_sql)         
                        if (get_lob_result.rows.length > 0) {
                            get_lob_result.rows.map((obj)=>{
                                lob_type_code[obj.name]=obj.sfid
                            })
                        }
                        console.log("lob_type_code",lob_type_code)
                        if(i_loop_length<=1000){
                                let map_product_code={}
                                let map_product_sfid={}
                                const fieldsTObeInserted = ['Pg_id__c,Name,Product_Name__c,LOB__c,PG1__c,PG2__c,PG3__c,PG4__c,mode__c,cron_name__c'];
                                let valuesToBeinserted=[]
                        for (temp_j; temp_j < i_loop_length; temp_j++) {
                                console.log("temp_j",temp_j)
                                console.log("i_loop_length",i_loop_length)
                                const t0 = performance.now();
                                const recordObj = jsonArray[temp_j];
                                let validationError = []
                                validation.issetNotEmpty(recordObj['Product Code']) ? true : validationError.push({ "field": "Product Code", "message": " Product Code is empty." });
                                validation.issetNotEmpty(recordObj['Product Name']) ? true : validationError.push({ "field": "Product Name", "message": "Product Name is empty" });
                                validation.issetNotEmpty(recordObj['LOB']) ? true : validationError.push({ "field": "LOB", "message": " LOB is empty" });
                                validation.issetNotEmpty(recordObj['PG1']) ? true : validationError.push({ "field": "PG1", "message": "PG1 Town is empty." });
                                validation.issetNotEmpty(recordObj['PG2']) ? true : validationError.push({ "field": "PG2", "message": " PG2 is empty" });
                                validation.issetNotEmpty(recordObj['PG3']) ? true : validationError.push({ "field": "PG3", "message": " PG3 is empty" });
                                validation.issetNotEmpty(recordObj['PG4']) ? true : validationError.push({ "field": "PG4", "message": "PG4 is empty" });    
                                if (validationError.length == 0 && recordObj['LOB'].toLowerCase() in lob_type_code) {
                                    map_product_code[recordObj['Product Code']]=recordObj
                                    recordObj['LOB']=lob_type_code[recordObj['LOB'].toLowerCase()]
                                } else {
                                    dataPush.push({ 'status': 'failed', 'docNo': recordObj['Product Code'], 'data': JSON.stringify(recordObj), 'message': JSON.stringify(validationError) })
                                    ++failed
                                }
                            }
                            console.log("map_product_code",map_product_code)
                                if(Object.keys(map_product_code).length>0){
                                    let get_all_record_exist_sql=`select name from salesforce.${SF_PRODUCT_ITEM_TABLE_NAME} where sfid is not null and name in ('${Object.keys(map_product_code).join("','")}') `                                    
                                    let get_all_record_exist_res=await client.query(get_all_record_exist_sql)
                                    console.log("get_all_record_exist_res length",get_all_record_exist_res.rows.length)
                                    if(get_all_record_exist_res.rows.length>0){
                                        get_all_record_exist_res.rows.map((obj)=>{
                                            map_product_sfid[obj.name]=''  
                                        })}
                                console.log("map_product_sfid",map_product_sfid)                                    
                                        Object.values(map_product_code).map((recordObj)=>{
                                            if (!(recordObj['Product Code'] in map_product_sfid)) {
                                                let item_obj={}
                                                item_obj['Pg_id__c']=uuidv4()
                                                item_obj['Name']=recordObj['Product Code']
                                                item_obj['Product_Name__c']=recordObj['Product Name']
                                                item_obj['LOB']=recordObj['LOB']
                                                item_obj['PG1']=recordObj['PG1']
                                                item_obj['PG2']=recordObj['PG2']
                                                item_obj['PG3']=recordObj['PG3']
                                                item_obj['PG4']=recordObj['PG4']
                                                item_obj['mode__c']=MODE_OF_UPDATE
                                                item_obj['cron_name__c']=cron_name
                                                valuesToBeinserted.push(item_obj)
                                            } else {
                                                dataPush.push({ 'status': 'failed', 'docNo': recordObj['Product Code'], 'data': JSON.stringify(recordObj), 'message': `Record is already there` })
                                                ++failed
                                            }
                                        })
                                }                                
                                console.log("valuesToBeinserted length--------------->",valuesToBeinserted.length)
                                console.log("valuesToBeinserted--------------->",valuesToBeinserted)
                                if(valuesToBeinserted.length>0){
                                    await insertManyRecord(fieldsTObeInserted, valuesToBeinserted, table_name)
                                }
                                console.log("area_roud--->", 1)
                        }else{
                            let j_loop_length=0
                            for (let i = 1; i <= (Math.ceil(i_loop_length/1000)); i++) {
                                const t0 = performance.now();
                                if((i_loop_length-j_loop_length)>1000){
                                    j_loop_length=(i)*1000
                                }else{
                                    j_loop_length=i_loop_length
                                }
                                let map_product_code={}
                                let map_product_sfid={}
                                const fieldsTObeInserted = ['Pg_id__c,Name,Product_Name__c,LOB__c,PG1__c,PG2__c,PG3__c,PG4__c,mode__c,cron_name__c'];
                                let valuesToBeinserted=[]
                                for (temp_j; temp_j < j_loop_length; temp_j++) {
                                        console.log("temp_j",temp_j)
                                        console.log("j_loop_length",j_loop_length)
                                        const t0 = performance.now();
                                        const recordObj = jsonArray[temp_j];
                                        let validationError = []
                                        validation.issetNotEmpty(recordObj['Product Code']) ? true : validationError.push({ "field": "Product Code", "message": " Product Code is empty." });
                                        validation.issetNotEmpty(recordObj['Product Name']) ? true : validationError.push({ "field": "Product Name", "message": "Product Name is empty" });
                                        validation.issetNotEmpty(recordObj['LOB']) ? true : validationError.push({ "field": "LOB", "message": " LOB is empty" });
                                        validation.issetNotEmpty(recordObj['PG1']) ? true : validationError.push({ "field": "PG1", "message": "PG1 Town is empty." });
                                        validation.issetNotEmpty(recordObj['PG2']) ? true : validationError.push({ "field": "PG2", "message": " PG2 is empty" });
                                        validation.issetNotEmpty(recordObj['PG3']) ? true : validationError.push({ "field": "PG3", "message": " PG3 is empty" });
                                        validation.issetNotEmpty(recordObj['PG4']) ? true : validationError.push({ "field": "PG4", "message": "PG4 is empty" });    
                                        if (validationError.length == 0 && recordObj['LOB'].toLowerCase() in lob_type_code) {
                                            map_product_code[recordObj['Product Code']]=recordObj
                                            recordObj['LOB']=lob_type_code[recordObj['LOB'].toLowerCase()]
                                        } else {
                                            dataPush.push({ 'status': 'failed', 'docNo': recordObj['Product Code'], 'data': JSON.stringify(recordObj), 'message': JSON.stringify(validationError) })
                                            ++failed
                                        }
                                    }
                                if(Object.keys(map_product_code).length>0){
                                    let get_all_record_exist_sql=`select name from salesforce.${SF_PRODUCT_ITEM_TABLE_NAME} where sfid is not null and name in ('${Object.keys(map_product_code).join("','")}') `                                    
                                    console.log("let get_all_record_exist_sql",get_all_record_exist_sql)
                                    let get_all_record_exist_res=await client.query(get_all_record_exist_sql)
                                    console.log("get_all_record_exist_res length",get_all_record_exist_res.rows.length)
                                    if(get_all_record_exist_res.rows.length>0){
                                        get_all_record_exist_res.rows.map((obj)=>{
                                            map_product_sfid[obj.name]=''  
                                        })
                                    }
                                    Object.values(map_product_code).map((recordObj)=>{
                                        if (!(recordObj['Product Code'] in map_product_sfid)) {
                                            let item_obj={}
                                            item_obj['Pg_id__c']=uuidv4()
                                            item_obj['Name']=recordObj['Product Code']
                                            item_obj['Product_Name__c']=recordObj['Product Name']
                                            item_obj['LOB']=recordObj['LOB']
                                            item_obj['PG1']=recordObj['PG1']
                                            item_obj['PG2']=recordObj['PG2']
                                            item_obj['PG3']=recordObj['PG3']
                                            item_obj['PG4']=recordObj['PG4']
                                            item_obj['mode__c']=MODE_OF_UPDATE
                                            item_obj['cron_name__c']=cron_name
                                            valuesToBeinserted.push(item_obj)
                                        } else {
                                            dataPush.push({ 'status': 'failed', 'docNo': recordObj['Product Code'], 'data': JSON.stringify(recordObj), 'message': `Record is already there` })
                                            ++failed
                                        }
                                    })
                                    
                                }                                
                                console.log("valuesToBeinserted--------------->",valuesToBeinserted.length)
                                if(valuesToBeinserted.length>0){
                                    await insertManyRecord(fieldsTObeInserted, valuesToBeinserted, table_name)
                                }               
                                console.log("area_roud--->", i)
                                const t1 = performance.now();
                                console.log(`time taking in area ${t1 - t0} milliseconds.`);
                            }  
                        }
                        let get_file=await logs.createProductlogFile(dataPush)     
                        if(failed!=0){
                            sendMailFTP(`Mapping ${file}`,`${failed} records failed in ${file} file mapping`,'product_item',get_file)
                        }
                        fs.unlink(path.join(productDirectoryPath, file), err => {
                            if (err) throw err;
                        });                
                        }
                }
                });
            } catch (e) {
                console.log("error while product_Upload_And_Mapping file", e)
                mail.email_error_log('error while product_Upload_And_Mapping file',e.message)
            }
        },
        {scheduled:true}
        )
        // Fully optimized Area -- 42 15 * * *
        const area_Upload_And_Mapping=cron.schedule(`42 15 * * *`,
        async ()=>{
            try {
                let cron_name = 'area_Upload_And_Mapping'
                const areaDirectoryPath = path.join(__dirname, IN_area_data_PATH);
                fs.readdir(areaDirectoryPath, async (err, files) => {
                    if (err) {
                        return console.log('Unable to load directory: ', err);
                    }
                    console.log("started mapping area")
                    for (let index = 0; index < files.length; index++) {
                        const file = files[index];
                        if(file!='.gitkeep'){
                        let failed = 0;
                        const dataPush = []
                        let file_path = path.join(areaDirectoryPath, file)
                        const table_name = SF_AREA_1_TABLE_NAME
                        const jsonArray = await csv().fromFile(file_path);
                        let temp_j=0;
                        let temp_k=temp_j
                        let i_loop_length=jsonArray.length       
                        if(file.startsWith("Branch")){
                            let area_level = await func.getPicklistSfid('Area1__c', 'Area_Level__c', 4)
                            let area_type = await func.getPicklistSfid('Area1__c', 'Area_Type__c', 'Branch')
                            let branch_type= await func.getPicklistSfid('Area1__c', 'Branch_Type__c', 'Non PV Branch')
                            if(i_loop_length<=1000){
                                let map_branch_code={}
                                let map_branch_sfid={}
                                const fieldsTObeInserted = ['Pg_id__c,Area_Level__c,Area_Type__c,Branch_Type__c,Name,Name__c,mode__c,cron_name__c'];
                                let valuesToBeinserted=[]
                                for (temp_j; temp_j < i_loop_length; temp_j++) {
                                        console.log("temp_j",temp_j)
                                        console.log("i_loop_length",i_loop_length)
                                        const t0 = performance.now();
                                        const recordObj = jsonArray[temp_j];
                                        let validationError = []
                                        recordObj['Branch Name']=apostrophe(recordObj['Branch Name'])
                                        validation.issetNotEmpty(recordObj['Branch Code']) ? true : validationError.push({ "field": "Branch Code", "message": "Branch Code is empty" });
                                        validation.issetNotEmpty(recordObj['Branch Name']) ? true : validationError.push({ "field": "Branch Name", "message": " Branch Name is empty" });
                                        
                                        if (validationError.length == 0 && branch_type!=null) {
                                            map_branch_code[recordObj['Branch Code']]=recordObj
                                        } else {
                                            dataPush.push({ 'status': 'failed', 'docNo': recordObj['Branch Code'], 'data': JSON.stringify(recordObj), 'message': JSON.stringify(validationError) })
                                            ++failed
                                        }
                                    }
                                if(Object.keys(map_branch_code).length>0){
                                    let get_all_record_exist_sql=`select NAME,sfid from salesforce.area1__c where sfid is not null and NAME in ('${Object.keys(map_branch_code).join("','")}') `                                    
                                    let get_all_record_exist_res=await client.query(get_all_record_exist_sql)
                                    console.log("get_all_record_exist_res length",get_all_record_exist_res.rows.length)
                                    if(get_all_record_exist_res.rows.length>0){
                                        get_all_record_exist_res.rows.map((obj)=>{
                                            map_branch_sfid[obj.name]=''  
                                        })}
                                    Object.values(map_branch_code).map((recordObj)=>{
                                        if (!(recordObj['Branch Code'] in map_branch_sfid)) {
                                            let item_obj={}
                                            item_obj['Pg_id__c']=uuidv4()
                                            item_obj['Area_Level__c']=area_level
                                            item_obj['Area_Type__c']=area_type
                                            item_obj['Branch_Type__c']=branch_type
                                            item_obj['NAME']=recordObj['Branch Code']
                                            item_obj['Name__c']=recordObj['Branch Name']
                                            item_obj['mode__c']=MODE_OF_UPDATE
                                            item_obj['cron_name__c']=cron_name
                                            valuesToBeinserted.push(item_obj)
                                        } else {
                                            dataPush.push({ 'status': 'failed', 'docNo': recordObj['Branch Code'], 'data': JSON.stringify(recordObj), 'message': `Record is already there` })
                                            ++failed
                                        }
                                    })    

                                }                                
                                console.log("valuesToBeinserted--------------->",valuesToBeinserted.length)
                                if(valuesToBeinserted.length>0){
                                    await insertManyRecord(fieldsTObeInserted, valuesToBeinserted, table_name)
                                }
                                    console.log("area_roud--->", 1)
                            }else{
                                let j_loop_length=0
                                let map_branch_code={}
                                let map_branch_sfid={}
                                for (let i = 1; i <= (Math.ceil(i_loop_length/1000)); i++) {
                                    const t0 = performance.now();
                                    if((i_loop_length-j_loop_length)>1000){
                                        j_loop_length=(i)*1000
                                    }else{
                                        j_loop_length=i_loop_length
                                    }
                                    let fieldsTObeInserted=['Pg_id__c,Area_Level__c,Area_Type__c,Branch_Type__c,Name,Name__c,mode__c,cron_name__c']
                                    let valuesToBeinserted=[]                                
                                    for (temp_j; temp_j < j_loop_length; temp_j++) {
                                            console.log("temp_j",temp_j)
                                            console.log("i_loop_length",i_loop_length)
                                            const t0 = performance.now();
                                            const recordObj = jsonArray[temp_j];
                                            let validationError = []
                                            recordObj['Branch Name']=apostrophe(recordObj['Branch Name'])
                                            validation.issetNotEmpty(recordObj['Branch Code']) ? true : validationError.push({ "field": "Branch Code", "message": "Branch Code is empty" });
                                            validation.issetNotEmpty(recordObj['Branch Name']) ? true : validationError.push({ "field": "Branch Name", "message": " Branch Name is empty" });
                                            console.log(temp_j,'-------------------')
                                            if (validationError.length == 0 && branch_type!=null) {
                                                map_branch_code[recordObj['Branch Code']]=recordObj
                                            } else {
                                                dataPush.push({ 'status': 'failed', 'docNo': recordObj['Branch Code'], 'data': JSON.stringify(recordObj), 'message': JSON.stringify(validationError) })
                                                ++failed
                                            }
                                        }
                                    if(Object.keys(map_branch_code).length>0){
                                        let get_all_record_exist_sql=`select NAME from salesforce.area1__c where sfid is not null and NAME in ('${Object.keys(map_branch_code).join("','")}') `                                    
                                        let get_all_record_exist_res=await client.query(get_all_record_exist_sql)
                                        console.log("get_all_record_exist_res length",get_all_record_exist_res.rows.length)
                                        if(get_all_record_exist_res.rows.length>0){
                                            get_all_record_exist_res.rows.map((obj)=>{
                                                map_branch_sfid[obj.name]=''  
                                            })}
                                            Object.values(map_branch_code).map((recordObj)=>{
                                                if (!(recordObj['Branch Code'] in map_branch_sfid)) {
                                                    let item_obj={}
                                                    item_obj['Pg_id__c']=uuidv4()
                                                    item_obj['Area_Level__c']=area_level
                                                    item_obj['Area_Type__c']=area_type
                                                    item_obj['Branch_Type__c']=branch_type
                                                    item_obj['NAME']=recordObj['Branch Code']
                                                    item_obj['Name__c']=recordObj['Branch Name']
                                                    item_obj['mode__c']=MODE_OF_UPDATE
                                                    item_obj['cron_name__c']=cron_name
                                                    valuesToBeinserted.push(item_obj)
                                                } else {
                                                    dataPush.push({ 'status': 'failed', 'docNo': recordObj['Branch Code'], 'data': JSON.stringify(recordObj), 'message': `Record is already there` })
                                                    ++failed
                                                }
                                            })
                                    }
                                    if(valuesToBeinserted.length>0){
                                        await insertManyRecord(fieldsTObeInserted, valuesToBeinserted, table_name)
                                    }               
                                    console.log("area_roud--->", i)
                                    const t1 = performance.now();
                                    console.log(`time taking in area ${t1 - t0} milliseconds.`);
                                }  
                            }
                            let get_file= await logs.createArealogFile(dataPush,'branch')
                            if(failed!=0){
                                sendMailFTP(`Mapping ${file}`,`${failed} records failed in ${file} file mapping`,'area',get_file)
                            }
                            fs.unlink(path.join(areaDirectoryPath, file), err => {
                                if (err) throw err;
                            });
                        }
                        if(file.startsWith("Teritorry")){
                            let area_level = await func.getPicklistSfid('Area1__c', 'Area_Level__c', 7)
                            let area_type = await func.getPicklistSfid('Area1__c', 'Area_Type__c', 'Territory')
                            let territory_type=await func.getPicklistSfid('Area1__c', 'Territory_Type__c', 'Primary')
                            if(i_loop_length<=1000){
                                let map_terr_code={}
                                let map_terr_code_sfid={}
                                const fieldsTObeInserted = ['Pg_id__c,Area_Level__c,Area_Type__c,Territory_Type__c,Name,Name__c,mode__c,cron_name__c'];
                                let valuesToBeinserted=[]
                                for (temp_j; temp_j < i_loop_length; temp_j++) {
                                        console.log("temp_j",temp_j)
                                        console.log("i_loop_length",i_loop_length)
                                        const t0 = performance.now();
                                        const recordObj = jsonArray[temp_j];
                                        recordObj['Territory Name']=apostrophe(recordObj['Territory Name'])
                                        let validationError = []
                                        validation.issetNotEmpty(recordObj['Territory Code']) ? true : validationError.push({ "field": "Territory Code", "message": "Territory Code is empty" });
                                        validation.issetNotEmpty(recordObj['Territory Name']) ? true : validationError.push({ "field": "Territory Name", "message": " Territory Name is empty" });
                                        console.log(temp_j,'-------------------')
                                        if (validationError.length == 0 && branch_type!=null) {
                                            map_terr_code[recordObj['Territory Code']]=recordObj
                                        } else {
                                            dataPush.push({ 'status': 'failed', 'docNo': recordObj['Territory Code'], 'data': JSON.stringify(recordObj), 'message': JSON.stringify(validationError) })
                                            ++failed
                                        }
                                    }
                                if(Object.keys(map_terr_code).length>0){
                                    let get_all_record_exist_sql=`select NAME from salesforce.area1__c where sfid is not null and NAME in ('${Object.keys(map_terr_code).join("','")}')`                                    
                                    let get_all_record_exist_res=await client.query(get_all_record_exist_sql)
                                    console.log("get_all_record_exist_res length",get_all_record_exist_res.rows.length)
                                    if(get_all_record_exist_res.rows.length>0){
                                        get_all_record_exist_res.rows.map((obj)=>{
                                            map_terr_code_sfid[obj.name]=''  
                                        })}
                                    Object.values(map_terr_code).map((recordObj)=>{
                                        if (!(recordObj['Territory Code'] in map_terr_code_sfid)) {
                                            let item_obj={}
                                            item_obj['Pg_id__c']=uuidv4()
                                            item_obj['Area_Level__c']=area_level
                                            item_obj['Area_Type__c']=area_type
                                            item_obj['Territory_Type__c']=territory_type
                                            item_obj['NAME']=recordObj['Territory Code']
                                            item_obj['Name__c']=recordObj['Territory Name']
                                            item_obj['mode__c']=MODE_OF_UPDATE
                                            item_obj['cron_name__c']=cron_name
                                            valuesToBeinserted.push(item_obj)
                                        } else {
                                            dataPush.push({ 'status': 'failed', 'docNo': recordObj['Territory Code'], 'data': JSON.stringify(recordObj), 'message': `Record is already there` })
                                            ++failed
                                        }
                                    })
                                    
                                }                                
                                console.log("valuesToBeinserted--------------->",valuesToBeinserted.length)
                                if(valuesToBeinserted.length>0){
                                    await insertManyRecord(fieldsTObeInserted, valuesToBeinserted, table_name)
                                }
                                console.log("area_roud--->", 1)
                            }else{
                                let j_loop_length=0
                                
                                for (let i = 1; i <= (Math.ceil(i_loop_length/1000)); i++) {
                                    const t0 = performance.now();
                                    if((i_loop_length-j_loop_length)>1000){
                                        j_loop_length=(i)*1000
                                    }else{
                                        j_loop_length=i_loop_length
                                    }      
                                    let map_terr_code={}
                                    let map_terr_code_sfid={}                          
                                    const fieldsTObeInserted = ['Pg_id__c,Area_Level__c,Area_Type__c,Territory_Type__c,Name,Name__c,mode__c,cron_name__c'];
                                    const valuesToBeinserted=[]
                                    for (temp_j; temp_j < j_loop_length; temp_j++) {
                                        console.log("temp_j",temp_j)
                                        console.log("j_loop_length",j_loop_length)
                                        const t0 = performance.now();
                                        const recordObj = jsonArray[temp_j];
                                        let validationError = []
                                        recordObj['Territory Name']=apostrophe(recordObj['Territory Name'])
                                        validation.issetNotEmpty(recordObj['Territory Code']) ? true : validationError.push({ "field": "Territory Code", "message": "Territory Code is empty" });
                                        validation.issetNotEmpty(recordObj['Territory Name']) ? true : validationError.push({ "field": "Territory Name", "message": " Territory Name is empty" });
                                        console.log(temp_j,'-------------------')
                                        if (validationError.length == 0) {
                                            map_terr_code[recordObj['Territory Code']]=recordObj
                                        } else {
                                            dataPush.push({ 'status': 'failed', 'docNo': recordObj['Territory Code'], 'data': JSON.stringify(recordObj), 'message': JSON.stringify(validationError) })
                                            ++failed
                                        }
                                    }
                                if(Object.keys(map_terr_code).length>0){
                                    let get_all_record_exist_sql=`select NAME from salesforce.area1__c where sfid is not null and NAME in ('${Object.keys(map_terr_code).join("','")}')`                                    
                                    let get_all_record_exist_res=await client.query(get_all_record_exist_sql)
                                    console.log("get_all_record_exist_res length",get_all_record_exist_res.rows.length)
                                    if(get_all_record_exist_res.rows.length>0){
                                        get_all_record_exist_res.rows.map((obj)=>{
                                            map_terr_code_sfid[obj.name]=''  
                                        })}
                                    Object.values(map_terr_code).map((recordObj)=>{
                                        if (!(recordObj['Territory Code'] in map_terr_code_sfid)) {
                                            let item_obj={}
                                            item_obj['Pg_id__c']=uuidv4()
                                            item_obj['Area_Level__c']=area_level
                                            item_obj['Area_Type__c']=area_type
                                            item_obj['Territory_Type__c']=territory_type
                                            item_obj['NAME']=recordObj['Territory Code']
                                            item_obj['Name__c']=recordObj['Territory Name']
                                            item_obj['mode__c']=MODE_OF_UPDATE
                                            item_obj['cron_name__c']=cron_name
                                            valuesToBeinserted.push(item_obj)
                                        } else {
                                            dataPush.push({ 'status': 'failed', 'docNo': recordObj['Territory Code'], 'data': JSON.stringify(recordObj), 'message': `Record is already there` })
                                            ++failed
                                        }
                                    })
                                }
                                console.log("valuesToBeinserted.length",valuesToBeinserted.length)
                                if(valuesToBeinserted.length > 0){
                                    await insertManyRecord(fieldsTObeInserted, valuesToBeinserted, table_name)
                                }               
                                    console.log("area_roud--->", i)
                                    const t1 = performance.now();
                                    console.log(`time taking in area ${t1 - t0} milliseconds.`);
                                }  
                            }
                            let get_file= await logs.createArealogFile(dataPush,"territory")
                            if(failed!=0){
                                sendMailFTP(`Mapping ${file}`,`${failed} records failed in ${file} file mapping`,'area',get_file)
                            }
                            fs.unlink(path.join(areaDirectoryPath, file), err => {
                                if (err) throw err;
                            });
                        }
                    }}
                });
                console.log("End here")
            } catch (e) {
                console.log("error",e.message)
                mail.email_error_log('error while area_Upload_And_Mapping file',e.message)
            }
        },
        {scheduled:true}
        )
        // Fully optimized Dealer except multiupdate--07 16 * * *

        const dealer_Upload_And_Mapping=cron.schedule(`28 17 * * *`,
        async ()=>{
            try {
                let cron_name = 'dealer_Upload_And_Mapping'
                const delertDirectoryPath = path.join(__dirname, IN_dealer_data_PATH);
                console.log("delertDirectoryPath", delertDirectoryPath)
                fs.readdir(delertDirectoryPath, async (err, files) => {
                    if (err) {
                        return console.log('Unable to load directory: ', err);
                    }
                    console.log("started mapping deler")
                    for (let index = 0; index < files.length; index++){
                        const file = files[index];
                        if(file!='.gitkeep'){
                        let failed = 0;
                        const dataPush = []
                        let file_path = path.join(delertDirectoryPath, file)
                        const account_table_name = SF_ACCOUNT_TABLE_NAME;
                        const jsonArray = await csv().fromFile(file_path);
                        let temp_j=0;
                        let i_loop_length=jsonArray.length
                        let lob_type_code={}
                        let div_type_code={}
                        let lob_type = await func.getPicklistSfid('Category__c','Category_Type__c','LOB')
                        let get_lob_sql = `SELECT LOWER(name) as name,sfid FROM salesforce.category__c where category_type__c='${lob_type}'`
                        console.log("get_lob_sql",get_lob_sql)
                        let get_lob_result = await client.query(get_lob_sql)         
                        if (get_lob_result.rows.length > 0) {
                            get_lob_result.rows.map((obj)=>{
                                if(obj.sfid!=null){
                                    lob_type_code[obj.name]=obj.sfid    
                                }
                                
                            })
                        }
                        let div_type = await func.getPicklistSfid('Category__c','Category_Type__c','Division')
                        let get_div_sql = `SELECT LOWER(name) as name,sfid FROM salesforce.category__c where category_type__c='${div_type}'`
                        console.log("get_div_sql",get_div_sql)
                        let get_div_result = await client.query(get_div_sql)         
                        if (get_div_result.rows.length > 0) {
                            get_div_result.rows.map((obj)=>{
                                if(obj.sfid!=null){
                                    div_type_code[obj.name]=obj.sfid
                                }
                                
                            })
                        }                   
                        console.log("i_loop_length in delertDirectoryPath",i_loop_length)
                        if(i_loop_length<=1000){
                            console.log("if part")
                            const t0 = performance.now();
                                let fieldsTObeInserted=['Pgid__c,Account_ID__c,Name,GST_IN__c,PAN_No__c,Billing_Address__c,Branch_Code__c,Territory__c,Pin_Code__c,LOB__c,E_mail__c,Mobile1__c,Phone,division__c,Total_Outstanding__c,mode__c,cron_name__c']
                                let valuesToBeinserted=[]
                                let valuesToBeUpdated={}
                                let map_sale_office={}
                                let map_sale_office_sfid={}
                                let map_terr_code={}
                                let map_terr_code_sfid={}
                                let map_customer_code={}
                                let map_customer_sfid={}
                                for (temp_j; temp_j < i_loop_length; temp_j++) {
                                    const recordObj = jsonArray[temp_j];
                                    console.log("temp_j",temp_j)
                                    let validationError = []
                                    recordObj['Customer Name']=apostrophe(recordObj['Customer Name'])
                                    recordObj['Street']=apostrophe(recordObj['Street'])
                                    validation.issetNotEmpty(recordObj['Customer']) ? true : validationError.push({ "field": "Customer", "message": "Customer  is empty" });
                                    validation.issetNotEmpty(recordObj['LOB']) ? true : validationError.push({ "field": "LOB", "message": "LOB is empty" });
                                    validation.issetNotEmpty(recordObj['Sales Office']) ? true : validationError.push({ "field": "sales office", "message": "sales office is empty" });
                                    validation.issetNotEmpty(recordObj['TERRITORY CODE']) ? true : validationError.push({ "field": "TERRITORY CODE", "message": "TERRITORY CODE is empty" });
                                    // validation.issetNotEmpty(recordObj['Div']) ? true : validationError.push({ "field": "Div", "message": "Div is empty" });
                                    console.log("recordObj['Div']}",recordObj['Div'])
                                    if (validationError.length == 0 && recordObj['LOB'].toLowerCase() in lob_type_code && recordObj['Div'].toLowerCase() in div_type_code){
                                        recordObj['Customer']=trimzero(recordObj['Customer'])
                                        console.log("recordObj['Customer']",recordObj['Customer'])
                                        map_customer_code[`${recordObj['Customer']}${recordObj['LOB']}${recordObj['Div']}`]=recordObj
                                        map_sale_office[recordObj['Sales Office']]=''
                                        map_terr_code[recordObj['TERRITORY CODE']]=''
                                        recordObj['LOB']=lob_type_code[recordObj['LOB'].toLowerCase()]
                                        recordObj['Div']=div_type_code[recordObj['Div'].toLowerCase()]
                                    }else{
                                        ++failed
                                        if(validationError.length==0){
                                            validationError.push({"field":"LOB","message":"LOB dosen't exist"})
                                        }
                                        dataPush.push({ 'status': 'failed', 'docNo': recordObj['Customer'], 'data': JSON.stringify(recordObj), 'message': JSON.stringify(validationError) })
                                    }
                                }
                                if(Object.keys(map_sale_office).length>0 && Object.keys(map_terr_code).length>0 && Object.keys(map_customer_code).length>0 ){
                                    let get_territory_name_sql = `select sfid,name from salesforce.area1__c where sfid is not null and name IN ('${Object.keys(map_terr_code).join("','")}')`
                                    let get_territory_name_res = await client.query(get_territory_name_sql);
                                    if (get_territory_name_res.rows.length > 0) {
                                        get_territory_name_res.rows.map((obj)=>{
                                            if(obj.sfid!=null){
                                                map_terr_code_sfid[obj.name]=obj.sfid
                                            }
                                        })
                                    }

                                    let get_branch_sql = `select sfid,name from salesforce.area1__c where name IN ('${Object.keys(map_sale_office).join("','")}')`
                                    let get_branch_res = await client.query(get_branch_sql);
                                    if (get_branch_res.rows.length > 0) {
                                        get_branch_res.rows.map((obj)=>{
                                            if(obj.sfid!=null){
                                                map_sale_office_sfid[obj.name]=obj.sfid
                                            }
                                        })
                                    }

                                    let get_name_of_cust_sql = `select sfid,account_id__c from salesforce.account where sfid is not null and uniquecombination__c IN ('${Object.keys(map_customer_code).join("','")}') `
                                    let get_name_of_cust_result = await client.query(get_name_of_cust_sql);
                                    if (get_name_of_cust_result.rows.length > 0) {
                                        get_name_of_cust_result.rows.map((obj)=>{
                                            if(obj.sfid!=null){
                                                map_customer_sfid[obj.account_id__c]=obj.sfid
                                            }
                                                
                                        })
                                    }
                                    console.log("map_customer_code",Object.values(map_customer_code))
                                    for (let temp_k = 0; temp_k < Object.values(map_customer_code).length; temp_k++) {
                                        const recordObj = Object.values(map_customer_code)[temp_k];
                                        console.log("temp_k-->",temp_k)
                                        if(recordObj['Customer'] in map_customer_sfid){
                                            if(recordObj['Sales Office'] in map_sale_office_sfid && recordObj['TERRITORY CODE'] in map_terr_code_sfid ){
                                                // let item_obj={}
                                                let fieldValue = [
                                                    { "field": "Account_ID__c", "value": recordObj['Customer'] },
                                                    { "field": "Name", "value": recordObj['Customer Name'] },
                                                    { "field": "GST_IN__c", "value": recordObj['GSTNO'] },
                                                    { "field": "Billing_Address__c", "value": recordObj['Street'] },
                                                    { "field": "Branch_Code__c", "value": map_sale_office_sfid[recordObj['Sales Office']] },
                                                    { "field": "Territory__c", "value": map_terr_code_sfid[recordObj['TERRITORY CODE']] },
                                                    { "field": "Pin_Code__c", "value": recordObj['Postal Code'] },
                                                    { "field": "LOB__c", "value": recordObj['LOB']},
                                                    { "field": "division__c", "value": recordObj['Div']},
                                                    { "field": "E_mail__c", "value": recordObj['MailID'] },
                                                    { "field": "Mobile1__c", "value": recordObj['Mobile Number'] },
                                                    { "field": "Phone", "value": recordObj['Telephone Number'] },
                                                    { "field": "mode__c", "value": MODE_OF_UPDATE },
                                                    { "field": "cron_name__c", "value": cron_name }
                                                    
                                                ]
                                                if(checkPan(recordObj['PAN No.'])){
                                                    // let obj = {
                                                    //     'PAN_No__c':recordObj['PAN No.']
                                                    // }
                                                    let obj = { "field": "PAN_No__c", "value": recordObj['PAN No.'] }
                                                    fieldValue.push(obj)
                                                }
                                                let whereClouse = [{ "field": "sfid", "value": map_customer_sfid[recordObj['Customer']]}]
                                                let record_updated = await qry.updateRecord(account_table_name, fieldValue, whereClouse);
                                                console.log("dealer table update record ::::::::::::::::::::::::", record_updated)   
                                            }else{
                                                ++failed
                                                dataPush.push({ 'status': 'failed', 'docNo': recordObj['Customer'], 'data': JSON.stringify(recordObj), 'message': 'Sales Office or TERRITORY CODE dose not exist'})
                                            }
                                        }else{
                                            if(recordObj['Sales Office'] in map_sale_office_sfid && recordObj['TERRITORY CODE'] in map_terr_code_sfid ){
                                                let item_obj={}
                                                item_obj['Pg_id__c']=uuidv4()
                                                item_obj['Account_ID__c']=recordObj['Customer']
                                                item_obj['Name']=recordObj['Customer Name']
                                                item_obj['GST_IN__c']=recordObj['GSTNO']
                                                if(checkPan(recordObj['PAN No.'])){
                                                    item_obj['PAN_No__c']=recordObj['PAN No.']
                                                }
                                                else{
                                                    item_obj['PAN_No__c']=''
                                                }                                        
                                                item_obj['Billing_Address__c']=recordObj['Street']
                                                item_obj['Branch_Code__c']=map_sale_office_sfid[recordObj['Sales Office']]
                                                item_obj['Territory__c']=map_terr_code_sfid[recordObj['TERRITORY CODE']]
                                                item_obj['Pin_Code__c']=recordObj['Postal Code']
                                                item_obj['LOB__c']=recordObj['LOB']
                                                item_obj['E_mail__c']=recordObj['MailID']
                                                item_obj['Mobile1__c']=recordObj['Mobile Number']
                                                item_obj['Phone']=recordObj['Telephone Number']
                                                item_obj['division__c']=recordObj['Div']
                                                item_obj['Total_Outstanding__c']=0
                                                item_obj['mode__c']=MODE_OF_UPDATE
                                                item_obj['cron_name__c']=cron_name
                                                valuesToBeinserted.push(item_obj)
                                            }else{
                                                ++failed
                                                dataPush.push({ 'status': 'failed', 'docNo': recordObj['Customer'], 'data': JSON.stringify(recordObj), 'message': 'Sales Office or TERRITORY CODE dose not exist'})
                                            }
                                        }
                                    }
                                    
                                }
                                console.log("valuesToBeinserted--------------->",valuesToBeinserted.length,valuesToBeinserted)
                                if(valuesToBeinserted.length>0){
                                    await insertManyRecord(fieldsTObeInserted, valuesToBeinserted, account_table_name)
                                }
                                const t1 = performance.now();
                                console.log(`time taking in dealer ${t1 - t0} milliseconds.`);
                            }else{
                            let j_loop_length=0
                            for (let i = 1; i <= (Math.ceil(i_loop_length/1000)); i++) {
                                const t3 = performance.now();
                                if((i_loop_length-j_loop_length)>1000){
                                    j_loop_length=(i)*1000
                                }else{
                                    j_loop_length=i_loop_length
                                }
                                let fieldsTObeInserted=['Pgid__c,Account_ID__c,Name,GST_IN__c,PAN_No__c,Billing_Address__c,Branch_Code__c,Territory__c,Pin_Code__c,LOB__c,E_mail__c,Mobile1__c,Phone,division__c,Total_Outstanding__c,mode__c,cron_name__c']
                                let valuesToBeinserted=[]
                                let valuesToBeUpdated={}
                                let map_sale_office={}
                                let map_sale_office_sfid={}
                                let map_terr_code={}
                                let map_terr_code_sfid={}
                                let map_customer_code={}
                                let map_customer_sfid={}
                                for (temp_j; temp_j < j_loop_length; temp_j++) {
                                    const recordObj = jsonArray[temp_j];
                                    console.log("recordObj",recordObj)
                                    console.log("temp_j",temp_j)
                                    let validationError = []
                                    recordObj['Customer Name']=apostrophe(recordObj['Customer Name'])
                                    recordObj['Street']=apostrophe(recordObj['Street'])
                                    validation.issetNotEmpty(recordObj['Customer']) ? true : validationError.push({ "field": "Customer", "message": "Customer  is empty" });
                                    validation.issetNotEmpty(recordObj['LOB']) ? true : validationError.push({ "field": "LOB", "message": "LOB is empty" });
                                    validation.issetNotEmpty(recordObj['Sales Office']) ? true : validationError.push({ "field": "sales office", "message": "sales office is empty" });
                                    validation.issetNotEmpty(recordObj['TERRITORY CODE']) ? true : validationError.push({ "field": "TERRITORY CODE", "message": "TERRITORY CODE is empty" });
                                    // validation.issetNotEmpty(recordObj['Div']) ? true : validationError.push({ "field": "Div", "message": "Div is empty" });
                                    if (validationError.length == 0 && recordObj['LOB'].toLowerCase() in lob_type_code && recordObj['Div'].toLowerCase() in div_type_code){
                                        recordObj['Customer']=trimzero(recordObj['Customer'])
                                        map_customer_code[`${recordObj['Customer']}${recordObj['LOB']}${recordObj['Div']}`]=recordObj
                                        map_sale_office[recordObj['Sales Office']]=''
                                        map_terr_code[recordObj['TERRITORY CODE']]=''
                                        recordObj['LOB']=lob_type_code[recordObj['LOB'].toLowerCase()]
                                        recordObj['Div']=lob_type_code[recordObj['Div'].toLowerCase()]
                                    }else{
                                        ++failed
                                        if(validationError.length==0){
                                            validationError.push({"field":"LOB","message":"LOB dosen't exist"})
                                        }
                                        dataPush.push({ 'status': 'failed', 'docNo': recordObj['Customer'], 'data': JSON.stringify(recordObj), 'message': JSON.stringify(validationError) })
                                    }
                                }
                                console.log("map_customer_code length",Object.keys(map_customer_code).length)
                                if(Object.keys(map_sale_office).length>0 && Object.keys(map_terr_code).length>0 && Object.keys(map_customer_code).length>0 ){
                                    let get_territory_name_sql = `select sfid,name from salesforce.area1__c where  name IN ('${Object.keys(map_terr_code).join("','")}')`
                                    let get_territory_name_res = await client.query(get_territory_name_sql);
                                    if (get_territory_name_res.rows.length > 0) {
                                        get_territory_name_res.rows.map((obj)=>{
                                            if(obj.sfid!=null){
                                                map_terr_code_sfid[obj.name]=obj.sfid
                                            }
                                        })
                                    }

                                    let get_branch_sql = `select sfid,name from salesforce.area1__c where name IN ('${Object.keys(map_sale_office).join("','")}')`
                                    let get_branch_res = await client.query(get_branch_sql);
                                    if (get_branch_res.rows.length > 0) {
                                        get_branch_res.rows.map((obj)=>{
                                            if(obj.sfid!=null){
                                                map_sale_office_sfid[obj.name]=obj.sfid
                                            }
                                        })
                                    }

                                    let get_name_of_cust_sql = `select sfid,account_id__c from salesforce.account where sfid is not null and uniquecombination__c IN ('${Object.keys(map_customer_code).join("','")}')`
                                    let get_name_of_cust_result = await client.query(get_name_of_cust_sql);
                                    if (get_name_of_cust_result.rows.length > 0) {
                                        get_name_of_cust_result.rows.map((obj)=>{
                                            if(obj.sfid!=null){
                                                map_customer_sfid[obj.account_id__c]=obj.sfid
                                            }
                                                
                                        })
                                    }
                                    console.log("map_customer_sfid",Object.keys(map_customer_sfid).length)
                                    for (let temp_k = 0; temp_k < Object.values(map_customer_code).length; temp_k++) {
                                        const recordObj = Object.values(map_customer_code)[temp_k];
                                        console.log("temp_k",temp_k)
                                        if(recordObj['Customer'] in map_customer_sfid){
                                            if(recordObj['Sales Office'] in map_sale_office_sfid && recordObj['TERRITORY CODE'] in map_terr_code_sfid ){
                                                let fieldValue = [
                                                    { "field": "Account_ID__c", "value": recordObj['Customer'] },
                                                    { "field": "Name", "value": recordObj['Customer Name'] },
                                                    { "field": "GST_IN__c", "value": recordObj['GSTNO'] },
                                                    { "field": "Billing_Address__c", "value": recordObj['Street'] },
                                                    { "field": "Branch_Code__c", "value": map_sale_office_sfid[recordObj['Sales Office']] },
                                                    { "field": "Territory__c", "value": map_terr_code_sfid[recordObj['TERRITORY CODE']] },
                                                    { "field": "Pin_Code__c", "value": recordObj['Postal Code'] },
                                                    { "field": "LOB__c", "value": recordObj['LOB']},
                                                    { "field": "division__c", "value": recordObj['Div']},
                                                    { "field": "E_mail__c", "value": recordObj['MailID'] },
                                                    { "field": "Mobile1__c", "value": recordObj['Mobile Number'] },
                                                    { "field": "Phone", "value": recordObj['Telephone Number'] },
                                                    { "field": "mode__c", "value": MODE_OF_UPDATE },
                                                    { "field": "cron_name__c", "value": cron_name },
                                                    { "field": "division__c", "value": recordObj['Div']}
                                                ]
                                                if(checkPan(recordObj['PAN No.'])){
                                                    // fieldValue.push(item_obj['PAN_No__c']=recordObj['PAN No.'])
                                                    let obj = { "field": "PAN_No__c", "value": recordObj['PAN No.'] }
                                                    fieldValue.push(obj)
                                                }
                                                let whereClouse = [{ "field": "sfid", "value": map_customer_sfid[recordObj['Customer']]}]
                                                let record_updated = await qry.updateRecord(account_table_name, fieldValue, whereClouse);
                                                console.log("dealer table update record ::::::::::::::::::::::::", record_updated)   
                                            }else{
                                                ++failed
                                                dataPush.push({ 'status': 'failed', 'docNo': recordObj['Customer'], 'data': JSON.stringify(recordObj), 'message': 'Sales Office or TERRITORY CODE dose not exist'})
                                            }
                                        }else{
                                            if(recordObj['Sales Office'] in map_sale_office_sfid && recordObj['TERRITORY CODE'] in map_terr_code_sfid  ){
                                                let item_obj={}
                                                item_obj['Pg_id__c']=uuidv4()
                                                item_obj['Account_ID__c']=recordObj['Customer']
                                                item_obj['Name']=recordObj['Customer Name']
                                                item_obj['GST_IN__c']=recordObj['GSTNO']
                                                
                                                if(checkPan(recordObj['PAN No.'])){
                                                    item_obj['PAN_No__c']=recordObj['PAN No.']
                                                }
                                                else{
                                                    item_obj['PAN_No__c']='';
                                                }
                                                item_obj['Billing_Address__c']=recordObj['Street']
                                                item_obj['Branch_Code__c']=map_sale_office_sfid[recordObj['Sales Office']]
                                                item_obj['Territory__c']=map_terr_code_sfid[recordObj['TERRITORY CODE']]
                                                item_obj['Pin_Code__c']=recordObj['Postal Code']
                                                item_obj['LOB__c']=recordObj['LOB']
                                                item_obj['E_mail__c']=recordObj['MailID']
                                                item_obj['Mobile1__c']=recordObj['Mobile Number']
                                                item_obj['Phone']=recordObj['Telephone Number']
                                                item_obj['division__c']=recordObj['Div']
                                                item_obj['Total_Outstanding__c']=0
                                                item_obj['mode__c']=MODE_OF_UPDATE
                                                item_obj['cron_name__c']=cron_name
                                                valuesToBeinserted.push(item_obj)
                                            }else{
                                                ++failed
                                                dataPush.push({ 'status': 'failed', 'docNo': recordObj['Customer'], 'data': JSON.stringify(recordObj), 'message': 'Sales Office or TERRITORY CODE dose not exist'})
                                            }
                                        }
                                    }   
                                }
                                const t4=performance.now()
                                console.log("valuesToBeinserted----------->",valuesToBeinserted.length,valuesToBeinserted)
                                if(valuesToBeinserted.length>0){
                                    await insertManyRecord(fieldsTObeInserted, valuesToBeinserted, account_table_name)
                                }
                                console.log("time taken in loop -----------> ",t4-t3)
                                console.log("roud--->", i)
                            }  
                        }
                        let get_file=await logs.createDealerlogFile(dataPush)
                        // send mail to 
                        console.log("failed",failed)
                        if(failed!=0){
                            sendMailFTP(`Mapping ${file}`,`${failed} records failed in ${file} file mapping`,'dealer',get_file)
                        }                
                        // //delete all the file from folder   
                        // fs.unlink(path.join(delertDirectoryPath, file), err => {
                        //     if (err) throw err;
                        // });
                    }}
                });
                console.log("End here")
            } catch (e) {
                console.log("error",e.message)
                mail.email_error_log('error while dealer_Upload_And_Mapping file',e.message)

            }
        },
        {scheduled:true}
        )
        // Fully optimized Pending order--40 10 1 * *
        const pendingorder_Upload_And_Mapping=cron.schedule(`28 17 * * *`,
        async ()=>{
            try {
                let cron_name = 'pendingorder_Upload_And_Mapping'
                const pendingOrderdirectoryPath = path.join(__dirname, IN_pending_order_DATA_PATH);
                console.log('pendingOrderdirectoryPath', pendingOrderdirectoryPath)
                fs.readdir(pendingOrderdirectoryPath, async (err, files) => {
                    if (err) {
                        return console.log('Unable to load directory: ', err);
                    }
                    console.log('started mapping pending order')
                    const order_table_name = SF_PENDING_ORDER_TABLE_NAME;
                    let delete_sql = `DELETE FROM salesforce.${order_table_name}`
                    console.log("delete_sql in pendinng order---->", delete_sql)
                    let delter_res = await client.query(delete_sql)
                    for (let index = 0; index < files.length; index++) {
                        const file = files[index];
                        if(file!='.gitkeep'){
                        let failed = 0;
                        const dataPush = []
                        let file_path = path.join(pendingOrderdirectoryPath, file)
                        const jsonArray = await csv().fromFile(file_path);
                        let temp_j=0;
                        let i_loop_length=jsonArray.length
                        if(i_loop_length<=1000){
                            const t0 = performance.now();
                            let fieldsTObeInserted=['Pgid__c,Dealer_Code__c,Dealer_Name__c,Design__c,Finish__c,Item_No__c,Material_Code__c,Name_Of_The_Material__c,Order_Date__c,Order_Delivery_Date__c,Name,Ordered_Qty_FA__c,Ordered_Qty_NA__c,Ordered_Quantity_In_UOM__c,Pending_Quantity__c,Pending_Quantity_FA__c,Pending_Quantity_NA__c,Pending_SO_Value_AFT__c,Pending_SO_Value_BFT__c,PG1__c,PG2__c,PG3__c,PG4__c,unit__c,Thickness__c,mode__c,cron_name__c']
                            let valuesToBeinserted=[]
                            let map_customer_code={}
                            let map_customer_sfid={}
                            let temp_k=temp_j; 
                            for (temp_j; temp_j < i_loop_length; temp_j++) {
                                const recordObj = jsonArray[temp_j];
                                let validationError = []
                                validation.issetNotEmpty(recordObj['cust code']) ? true : validationError.push({ "field": "cust code", "message": " cust code is empty." });
                                validation.issetNotEmpty(recordObj['Unit']) ? true : validationError.push({ "field": "Unit", "message": "Unit is empty" });
                                validation.issetNotEmpty(recordObj['Name of the material']) ? true : validationError.push({ "field": "Name of the material", "message": "Unit is empty" });
                                if (validationError.length == 0) {
                                    console.log(temp_j,'-------------------')
                                    map_customer_code[recordObj['cust code']]=recordObj['cust code']
                                } else {
                                    dataPush.push({ 'status': 'failed', 'docNo': recordObj['Sales Document'], 'data': JSON.stringify(recordObj), 'message': JSON.stringify(validationError) })
                                    ++failed
                                }
                                }
                            if(Object.keys(map_customer_code).length>0){
                                let get_all_customer_sfid_sql=`select sfid,account_id__c from salesforce.account where account_id__c IN ('${Object.keys(map_customer_code).join("','")}')`
                                console.log("get_all_customer_sfid_sql",get_all_customer_sfid_sql)
                                let get_all_customer_sfid_res=await client.query(get_all_customer_sfid_sql)
                                if(get_all_customer_sfid_res.rows.length>0){
                                    get_all_customer_sfid_res.rows.map((obj)=>{
                                        if(obj.sfid!=null){
                                            map_customer_sfid[obj.account_id__c]=obj.sfid
                                        }
                                    })
                                    for (temp_k; temp_k < i_loop_length; temp_k++){
                                        const recordObj = jsonArray[temp_k];
                                        if(recordObj['cust code'] in map_customer_sfid){
                                            let item_obj={}
                                            item_obj['Pgid__c']=uuidv4()
                                            item_obj['Dealer_Code__c']=recordObj['cust code']
                                            item_obj['Dealer_Name__c']=map_customer_sfid[recordObj['cust code']]
                                            item_obj['Design__c']=recordObj['Design']
                                            item_obj['Finish__c']=recordObj['Finish']
                                            item_obj['Item_No__c']=recordObj['Item No']
                                            item_obj['Material_Code__c']=recordObj['Material']
                                            item_obj['Name_Of_The_Material__c']=recordObj['Name of the material']
                                            item_obj['Order_Date__c']=dateZeroFix(recordObj['Document Date'])
                                            item_obj['Order_Delivery_Date__c']=dateZeroFix2(recordObj.Ord['Delivery Date'])
                                            item_obj['Name']=recordObj['Sales Document']
                                            item_obj['Ordered_Qty_FA__c']=recordObj['Ordered Qty(FA)']
                                            item_obj['Ordered_Qty_NA__c']=recordObj['Ordered Qty(NA)']
                                            item_obj['Ordered_Quantity_In_UOM__c']=recordObj['Ordered Qty in order UOM']
                                            item_obj['Pending_Quantity__c']=recordObj['Pending Quantity']
                                            item_obj['Pending_Quantity_FA__c']=recordObj['Pending Quantity(FA)']
                                            item_obj['Pending_Quantity_NA__c']=recordObj['Pending Quantity(NA)']
                                            item_obj['Pending_SO_Value_AFT__c']=recordObj['Pending SO value(AFT)']
                                            item_obj['Pending_SO_Value_BFT__c']=recordObj['pending SO Value (BFT)']
                                            item_obj['PG1__c']=recordObj['PG1']
                                            item_obj['PG2__c']= recordObj['PG2']
                                            item_obj['PG3__c']=recordObj['PG3']
                                            item_obj['PG4__c']= recordObj['PG4']
                                            item_obj['unit__c']=recordObj['Unit']
                                            item_obj['Thickness__c']= recordObj['Thickness']  
                                            item_obj['mode__c']= MODE_OF_UPDATE
                                            item_obj['cron_name__c']= cron_name
                                            valuesToBeinserted.push(item_obj)
                                            }else{
                                            dataPush.push({ 'status': 'failed', 'docNo': recordObj['Sales Document'], 'data': JSON.stringify(recordObj), 'message': `cust code dosen't exist` })
                                            ++failed
                                            }
                                    }       
                                }
                            }
                            console.log("valuesToBeinserted----------->",valuesToBeinserted.length)
                            if(valuesToBeinserted.length>0){
                                await insertManyRecord(fieldsTObeInserted, valuesToBeinserted, order_table_name)
                            }
                            const t1 = performance.now();
                            console.log(`time taking in pendingOrderdirectoryPath ${t1 - t0} milliseconds.`);
                            console.log("roud--->", 1)
                        }else{
                            let j_loop_length=0
                            for (let i = 1; i <= (Math.ceil(i_loop_length/1000)); i++) {
                                const t0 = performance.now();
                                if((i_loop_length-j_loop_length)>1000){
                                    j_loop_length=(i)*1000
                                }else{
                                    j_loop_length=i_loop_length
                                }
                                let fieldsTObeInserted=['Pgid__c,Dealer_Code__c,Dealer_Name__c,Design__c,Finish__c,Item_No__c,Material_Code__c,Name_Of_The_Material__c,Order_Date__c,Order_Delivery_Date__c,Name,Ordered_Qty_FA__c,Ordered_Qty_NA__c,Ordered_Quantity_In_UOM__c,Pending_Quantity__c,Pending_Quantity_FA__c,Pending_Quantity_NA__c,Pending_SO_Value_AFT__c,Pending_SO_Value_BFT__c,PG1__c,PG2__c,PG3__c,PG4__c,unit__c,Thickness__c,mode__c,cron_name__c']
                                let valuesToBeinserted=[]
                                let map_customer_code={}
                                let map_customer_sfid={} 
                                let temp_k=temp_j;
                                for (temp_j; temp_j < j_loop_length; temp_j++) {
                                    const recordObj = jsonArray[temp_j];
                                    let validationError = []
                                    validation.issetNotEmpty(recordObj['cust code']) ? true : validationError.push({ "field": "cust code", "message": " cust code is empty." });
                                    validation.issetNotEmpty(recordObj['Unit']) ? true : validationError.push({ "field": "Unit", "message": "Unit is empty" });
                                    validation.issetNotEmpty(recordObj['Name of the material']) ? true : validationError.push({ "field": "Name of the material", "message": "Unit is empty" });
                                    if (validationError.length == 0) {
                                        console.log(temp_j,'-------------------')
                                        map_customer_code[recordObj['cust code']]=recordObj['cust code']
                                    } else {
                                        dataPush.push({ 'status': 'failed', 'docNo': recordObj['Sales Document'], 'data': JSON.stringify(recordObj), 'message': JSON.stringify(validationError) })
                                        ++failed
                                    }
                                    }
                                if(Object.keys(map_customer_code).length>0){
                                    let get_all_customer_sfid_sql=`select sfid,account_id__c from salesforce.account where account_id__c IN ('${Object.keys(map_customer_code).join("','")}')`
                                    console.log("get_all_customer_sfid_sql",get_all_customer_sfid_sql)
                                    let get_all_customer_sfid_res=await client.query(get_all_customer_sfid_sql)
                                    if(get_all_customer_sfid_res.rows.length>0){
                                        get_all_customer_sfid_res.rows.map((obj)=>{
                                            if(obj.sfid!=null){
                                                map_customer_sfid[obj.account_id__c]=obj.sfid
                                            }                                })
                                        for (temp_k; temp_k < j_loop_length; temp_k++){
                                            const recordObj = jsonArray[temp_k];
                                            if(recordObj['cust code'] in map_customer_sfid){
                                                let item_obj={}
                                                item_obj['Pgid__c']=uuidv4()
                                                item_obj['Dealer_Code__c']=recordObj['cust code']
                                                item_obj['Dealer_Name__c']=map_customer_sfid[recordObj['cust code']]
                                                item_obj['Design__c']=recordObj['Design']
                                                item_obj['Finish__c']=recordObj['Finish']
                                                item_obj['Item_No__c']=recordObj['Item No']
                                                item_obj['Material_Code__c']=recordObj['Material']
                                                item_obj['Name_Of_The_Material__c']=recordObj['Name of the material']
                                                item_obj['Order_Date__c']=dateZeroFix(recordObj['Document Date'])
                                                item_obj['Order_Delivery_Date__c']=dateZeroFix2(recordObj.Ord['Delivery Date'])
                                                item_obj['Name']=recordObj['Sales Document']
                                                item_obj['Ordered_Qty_FA__c']=recordObj['Ordered Qty(FA)']
                                                item_obj['Ordered_Qty_NA__c']=recordObj['Ordered Qty(NA)']
                                                item_obj['Ordered_Quantity_In_UOM__c']=recordObj['Ordered Qty in order UOM']
                                                item_obj['Pending_Quantity__c']=recordObj['Pending Quantity']
                                                item_obj['Pending_Quantity_FA__c']=recordObj['Pending Quantity(FA)']
                                                item_obj['Pending_Quantity_NA__c']=recordObj['Pending Quantity(NA)']
                                                item_obj['Pending_SO_Value_AFT__c']=recordObj['Pending SO value(AFT)']
                                                item_obj['Pending_SO_Value_BFT__c']=recordObj['pending SO Value (BFT)']
                                                item_obj['PG1__c']=recordObj['PG1']
                                                item_obj['PG2__c']= recordObj['PG2']
                                                item_obj['PG3__c']=recordObj['PG3']
                                                item_obj['PG4__c']= recordObj['PG4']
                                                item_obj['unit__c']=recordObj['Unit']
                                                item_obj['Thickness__c']= recordObj['Thickness']  
                                                item_obj['mode__c']= MODE_OF_UPDATE
                                                item_obj['cron_name__c']= cron_name
                                                valuesToBeinserted.push(item_obj)
                                                }else{
                                                dataPush.push({ 'status': 'failed', 'docNo': recordObj['Sales Document'], 'data': JSON.stringify(recordObj), 'message': `cust code dosen't exist` })
                                                ++failed
                                                }
                                        }       
                                    }
                                }
                                console.log("valuesToBeinserted----------->",valuesToBeinserted.length)
                                if(valuesToBeinserted.length>0){
                                    await insertManyRecord(fieldsTObeInserted, valuesToBeinserted, order_table_name)
                                }                    
                                console.log("roud--->", i)
                                const t1 = performance.now();
                                console.log(`time taking in pendingOrderdirectoryPath ${t1 - t0} milliseconds.`);
                            }  
                        }
                        console.log("pendingOrderdirectoryPath in dataPush", dataPush)
                        let get_file=await logs.createPendingOrderlogFile(dataPush)
                        sendMailFTP(`Mapping ${file}`,`${failed} records failed in ${file} file mapping`,'pending_order',get_file)
                        // delete all the file from folder   
                        fs.unlink(path.join(pendingOrderdirectoryPath, file), err => {
                            if (err) throw err;
                        });
                    }}
                });
                console.log("End here")
            } catch (e) {
                console.log("error",e.message)
                mail.email_error_log('error while pendingorder_Upload_And_Mapping file',e.message)
            }
        },
        {scheduled:true}
        )
        // Need to optimize primarysale--06 16 * * *
        const primarysale_Upload_And_Mapping=cron.schedule(`32 16 * * *`,
        async ()=>{
            try {
                let cron_name = 'primarysale_Upload_And_Mapping'
                const primarySalesdirectoryPath = path.join(__dirname, IN_Primary_sales_DATA_PATH);
                console.log("primarySalesdirectoryPath", primarySalesdirectoryPath)
                fs.readdir(primarySalesdirectoryPath, async (err, files) => {
                    if (err) {
                        return console.log('Unable to load directory: ', err);
                    }
                    console.log('started mapping primarySales')
                    for (let index = 0; index < files.length; index++) {
                        const file = files[index];
                        if(file!='.gitkeep'){
                        let failed = 0;
                        const dataPush = []
                        let file_path = path.join(primarySalesdirectoryPath, file)
                        const table_name= SF_PRIMARY_SALES_TABLE_NAME;
                        const jsonArray = await csv().fromFile(file_path);
                        let temp_j=0;
                        let i_loop_length=jsonArray.length
                        console.log("i_loop_length",i_loop_length)
                        if(i_loop_length<=1000){
                            console.log("if part")
                                let fieldsTObeInserted=['Pgid__c,Before_Tax_Amount__c,Bill_Amount__c,Bill_Quantity__c,Dealer_Code__c,dealer_order__c,Design__c,Division_Text__c,Due_Date__c,Finish_LAM__c,HSNNO__c,Invoice_Date__c,Name,Line_Item__c,Material__c,Material_Description__c,PG1__c,PG2__c,PG3__c,PG4__c,Tax_Amount__c,TCS__c,Territory_Name__c,Thickness__c,Unit__c,mode__c,cron_name__c']
                                let valuesToBeinserted=[]
                                let Customer_Code_map={}
                                let Customer_Code_sfid={}
                                let Material_map={}
                                let Material_sfid={}
                                let record_name_map={}
                                let record_name_sfid={}
                                for (temp_j; temp_j < i_loop_length; temp_j++) {
                                    const recordObj = jsonArray[temp_j];
                                    let validationError = []
                                    validation.issetNotEmpty(recordObj['Customer Code']) ? true : validationError.push({ "field": "Customer Code", "message": "Customer Code is empty." });
                                    validation.issetNotEmpty(recordObj['Material']) ? true : validationError.push({ "field": "Material", "message": "Territory Code is empty" });
                                    validation.issetNotEmpty(recordObj['SALESUNIT']) ? true : validationError.push({ "field": "SALESUNIT", "message": "SALESUNIT is empty" });    
                                    validation.issetNotEmpty(recordObj.Inv['No']) ? true : validationError.push({ "field": "Inv.No", "message": "Inv.NO is empty" });    
                                    validation.issetNotEmpty(recordObj['Line Item']) ? true : validationError.push({ "field": "Line Item", "message": "Line Item is empty" });    
                                    if (validationError.length == 0) {
                                        Customer_Code_map[recordObj['Customer Code']]=''
                                        Material_map[recordObj['Material']]=''
                                        record_name_map[recordObj.Inv['No'].toString()+recordObj['Line Item'].toString()]=recordObj
                                    }else{
                                        ++failed
                                        dataPush.push({ 'status': 'failed', 'docNo': recordObj.Inv['No'], 'data': JSON.stringify(recordObj), 'message': JSON.stringify(validationError) })
                                    }
                                }
                                if(Object.keys(Customer_Code_map).length>0 && Object.keys(Material_map).length>0 && Object.keys(record_name_map).length>0){
                                    let get_name_of_cust_sql = `select sfid,account_id__c from salesforce.account where account_id__c IN ('${Object.keys(Customer_Code_map).join("','")}')`
                                    console.log("get_name_of_cust_sql->",get_name_of_cust_sql)
                                    let get_name_of_cust_result = await client.query(get_name_of_cust_sql);
                                    if (get_name_of_cust_result.rows.length > 0) {
                                        get_name_of_cust_result.rows.map((obj)=>{
                                            if(obj.sfid!=null){
                                                Customer_Code_sfid[obj.account_id__c]=obj.sfid
                                            }
                                        })
                                    }
                                    let material_sql = `select sfid,name from salesforce.product_item__c where name IN ('${Object.keys(Material_map).join("','")}')`
                                    console.log("material_sql->",material_sql)
                                    let material_sql_result = await client.query(material_sql)
                                    if (material_sql_result.rows.length > 0) {
                                        material_sql_result.rows.map((obj)=>{
                                            if(obj.sfid!=null){
                                                Material_sfid[obj.name]=obj.sfid
                                            }
                                        })
                                    }
                                    let get_record_sql=`select sfid,name from salesforce.${table_name} where sfid is not null and name IN ('${Object.keys(record_name_map).join("','")}')`
                                    console.log("get_record_sql->",get_record_sql)
                                    let get_record_res=await client.query(get_record_sql)
                                    if(get_record_res.rows.length>0){
                                        get_record_res.rows.map((obj)=>{
                                            if(obj.sfid!=null){
                                                record_name_sfid[obj.name]=obj.sfid
                                            }
                                        })
                                    }
                                    for (let temp_k = 0; temp_k < Object.values(record_name_map).length; temp_k++) {
                                        const recordObj = Object.values(record_name_map)[temp_k];
                                        console.log("temp_k->",temp_k)
                                        if((recordObj.Inv['No'].toString()+recordObj['Line Item'].toString()) in record_name_sfid ){
                                            if(recordObj['Customer Code'] in Customer_Code_sfid && recordObj['Material'] in Material_sfid){
                                                let objDate=dateZeroFix2(recordObj.Inv['Date'])
                                                let fieldValue = [
                                                    { "field": "Before_Tax_Amount__c", "value": recordObj['Before Tax Amount'] },
                                                    { "field": "Bill_Amount__c", "value": recordObj['BILLAMT'] },
                                                    { "field": "Bill_Quantity__c", "value": recordObj['BILLQTY'] },
                                                    { "field": "Dealer_Code__c", "value": recordObj['Customer Code'] },
                                                    { "field": "dealer_order__c", "value": Customer_Code_sfid[recordObj['Customer Code']]},
                                                    { "field": "Design__c", "value": recordObj['DESIGN'] },
                                                    { "field": "Division_Text__c", "value": recordObj['Division Text'] },
                                                    { "field": "Due_Date__c", "value": objDate },
                                                    { "field": "Finish_LAM__c", "value": recordObj['Finish(LAM)'] },
                                                    { "field": "HSNNO__c", "value": recordObj['HSNNO'] },
                                                    { "field": "Invoice_Date__c", "value": objDate },
                                                    { "field": "Name", "value": `${recordObj.Inv['No'].toString()}${recordObj['Line Item'].toString()}` },
                                                    { "field": "Line_Item__c", "value": recordObj['Line Item'] },
                                                    { "field": "Material__c", "value": Material_sfid[recordObj['Material']] },
                                                    { "field": "Material_Description__c", "value": recordObj['Material Desc.'] },
                                                    { "field": "PG1__c", "value": recordObj['PG1'] },
                                                    { "field": "PG2__c", "value": recordObj['PG2'] },
                                                    { "field": "PG3__c", "value": recordObj['PG3'] },
                                                    { "field": "PG4__c", "value": recordObj['PG4'] },
                                                    { "field": "Tax_Amount__c", "value": recordObj['Tax Amount'] },
                                                    { "field": "TCS__c", "value": recordObj['TCS'] },
                                                    { "field": "Territory_Name__c", "value": recordObj['TERRITORY'] },
                                                    { "field": "Thickness__c", "value": recordObj['THICKNESS'] },
                                                    { "field": "Unit__c", "value": recordObj['SALESUNIT'] },
                                                    { "field": "mode__c", "value": MODE_OF_UPDATE },
                                                    { "field": "cron_name__c", "value": cron_name }
                                                ]
                                                let whereClouse = [{ "field": "sfid", "value": recordObj.Inv['No'].toString()+recordObj['Line Item'].toString() }]
                                                let record_updated = await qry.updateRecord(table_name, fieldValue, whereClouse);
                                            }else{
                                                ++failed
                                                dataPush.push({ 'status': 'failed', 'docNo': recordObj.Inv['No'], 'data': JSON.stringify(recordObj), 'message': 'Customer Code or Material dose not exist'})
                                            }
                                        }else{
                                            console.log("insert record")
                                            if(recordObj['Customer Code'] in Customer_Code_sfid && recordObj['Material'] in Material_sfid){
                                                let objDate = dateZeroFix2(recordObj.Inv['Date'])
                                                let item_obj={}
                                                item_obj['Pgid__c']=uuidv4()
                                                item_obj['Before_Tax_Amount__c']=recordObj['Before Tax Amount']
                                                item_obj['Bill_Amount__c']=recordObj['BILLAMT']
                                                item_obj['Bill_Quantity__c']=recordObj['BILLQTY']
                                                item_obj['Dealer_Code__c']=recordObj['Customer Code']
                                                item_obj['dealer_order__c']= Customer_Code_sfid[recordObj['Customer Code']]
                                                item_obj['Design__c']=recordObj['DESIGN']
                                                item_obj['Division_Text__c']=recordObj['Division Text']
                                                item_obj['Due_Date__c']=objDate
                                                item_obj['Finish_LAM__c']= recordObj['Finish(LAM)']
                                                item_obj['HSNNO__c']= recordObj['HSNNO']
                                                item_obj['Invoice_Date__c']=objDate
                                                item_obj['Name']=`${recordObj.Inv['No'].toString()}${recordObj['Line Item'].toString()}`
                                                item_obj['Line_Item__c']=recordObj['Line Item']
                                                item_obj['Material__c']= Material_sfid[recordObj['Material']]
                                                item_obj['Material_Description__c']= recordObj['Material Desc.']
                                                item_obj['PG1__c']= recordObj['PG1']
                                                item_obj['PG2__c']=recordObj['PG2']
                                                item_obj['PG3__c']=recordObj['PG3']
                                                item_obj['PG4__c']= recordObj['PG4']
                                                item_obj['Tax_Amount__c']=recordObj['Tax Amount']
                                                item_obj['TCS__c']= recordObj['TCS']
                                                item_obj['Territory_Name__c']=recordObj['TERRITORY']
                                                item_obj['Thickness__c']=recordObj['THICKNESS']
                                                item_obj['Unit__c']=recordObj['SALESUNIT']
                                                item_obj['mode__c']=MODE_OF_UPDATE
                                                item_obj['cron_name__c']=cron_name
                                                valuesToBeinserted.push(item_obj)
                                            }else{
                                                ++failed
                                                dataPush.push({ 'status': 'failed', 'docNo': recordObj.Inv['No'], 'data': JSON.stringify(recordObj), 'message': 'Customer Code or Material dose not exist'})
                                            }
                                        }                                                                
                                    }     
                            }
                                console.log("valuesToBeinserted--------------->",valuesToBeinserted.length,valuesToBeinserted)
                                console.log("datapush--------------->",dataPush.length,dataPush)
                                if(valuesToBeinserted.length>0){
                                    await insertManyRecord(fieldsTObeInserted, valuesToBeinserted, table_name)
                                }
                                console.log("round--->", 1)
                        }else{
                            let j_loop_length=0
                            for (let i = 1; i <= (Math.ceil(i_loop_length/1000)); i++) {  
                                const t3=performance.now()
                                if((i_loop_length-j_loop_length)>1000){
                                    j_loop_length=(i)*1000
                                }else{
                                    j_loop_length=i_loop_length
                                }                        
                                // Unit__c
                                let fieldsTObeInserted=['Pgid__c,Before_Tax_Amount__c,Bill_Amount__c,Bill_Quantity__c,Dealer_Code__c,dealer_order__c,Design__c,Division_Text__c,Due_Date__c,Finish_LAM__c,HSNNO__c,Invoice_Date__c,Name,Line_Item__c,Material__c,Material_Description__c,PG1__c,PG2__c,PG3__c,PG4__c,Tax_Amount__c,TCS__c,Territory_Name__c,Thickness__c,Unit__c,mode__c,cron_name__c']
                                let valuesToBeinserted=[]
                                let Customer_Code_map={}
                                let Customer_Code_sfid={}
                                let Material_map={}
                                let Material_sfid={}
                                let record_name_map={}
                                let record_name_sfid={}
                                for (temp_j; temp_j < j_loop_length; temp_j++) {
                                    console.log("temp_j->",temp_j)
                                    const recordObj = jsonArray[temp_j];
                                    let validationError = []
                                    validation.issetNotEmpty(recordObj['Customer Code']) ? true : validationError.push({ "field": "Customer Code", "message": "Customer Code is empty." });
                                    validation.issetNotEmpty(recordObj['Material']) ? true : validationError.push({ "field": "Material", "message": "Territory Code is empty" });
                                    validation.issetNotEmpty(recordObj['SALESUNIT']) ? true : validationError.push({ "field": "SALESUNIT", "message": "SALESUNIT is empty" });    
                                    validation.issetNotEmpty(recordObj.Inv['No']) ? true : validationError.push({ "field": "Inv.No", "message": "Inv.NO is empty" });    
                                    validation.issetNotEmpty(recordObj['Line Item']) ? true : validationError.push({ "field": "Line Item", "message": "Line Item is empty" });    
                                    if (validationError.length == 0) {
                                        Customer_Code_map[recordObj['Customer Code']]=''
                                        Material_map[recordObj['Material']]=''
                                        record_name_map[recordObj.Inv['No'].toString()+recordObj['Line Item'].toString()]=recordObj
                                    }else{
                                        ++failed
                                        dataPush.push({ 'status': 'failed', 'docNo': recordObj.Inv['No'], 'data': JSON.stringify(recordObj), 'message': JSON.stringify(validationError) })
                                    }
                                }
                                if(Object.keys(Customer_Code_map).length>0 && Object.keys(Material_map).length>0 && Object.keys(record_name_map).length>0){
                                        let get_name_of_cust_sql = `select sfid,account_id__c from salesforce.account where account_id__c IN ('${Object.keys(Customer_Code_map).join("','")}')`
                                        console.log("get_name_of_cust_sql->",get_name_of_cust_sql)
                                        let get_name_of_cust_result = await client.query(get_name_of_cust_sql);
                                        if (get_name_of_cust_result.rows.length > 0) {
                                            get_name_of_cust_result.rows.map((obj)=>{
                                                if(obj.sfid!=null){
                                                    Customer_Code_sfid[obj.account_id__c]=obj.sfid
                                                }
                                            })
                                        }
                                        let material_sql = `select sfid,name from salesforce.product_item__c where name IN ('${Object.keys(Material_map).join("','")}')`
                                        console.log("material_sql->",material_sql)
                                        let material_sql_result = await client.query(material_sql)
                                        if (material_sql_result.rows.length > 0) {
                                            material_sql_result.rows.map((obj)=>{
                                                if(obj.sfid!=null){
                                                    Material_sfid[obj.name]=obj.sfid
                                                }
                                            })
                                        }
                                        let get_record_sql=`select sfid,name from salesforce.${table_name} where sfid is not null and name IN ('${Object.keys(record_name_map).join("','")}')`
                                        console.log("get_record_sql->",get_record_sql)
                                        let get_record_res=await client.query(get_record_sql)
                                        if(get_record_res.rows.length>0){
                                            get_record_res.rows.map((obj)=>{
                                                if(obj.sfid!=null){
                                                    record_name_sfid[obj.name]=obj.sfid
                                                }
                                            })
                                        }
                                        for (let temp_k = 0; temp_k < Object.values(record_name_map).length; temp_k++) {
                                            const recordObj = Object.values(record_name_map)[temp_k];
                                            console.log("temp_k->",temp_k)
                                            if((recordObj.Inv['No'].toString()+recordObj['Line Item'].toString()) in record_name_sfid ){
                                                if(recordObj['Customer Code'] in Customer_Code_sfid && recordObj['Material'] in Material_sfid){
                                                    let objDate=dateZeroFix2(recordObj.Inv['Date'])
                                                    let fieldValue = [
                                                        { "field": "Before_Tax_Amount__c", "value": recordObj['Before Tax Amount'] },
                                                        { "field": "Bill_Amount__c", "value": recordObj['BILLAMT'] },
                                                        { "field": "Bill_Quantity__c", "value": recordObj['BILLQTY'] },
                                                        { "field": "Dealer_Code__c", "value": recordObj['Customer Code'] },
                                                        { "field": "dealer_order__c", "value": Customer_Code_sfid[recordObj['Customer Code']] },
                                                        { "field": "Design__c", "value": recordObj['DESIGN'] },
                                                        { "field": "Division_Text__c", "value": recordObj['Division Text'] },
                                                        { "field": "Due_Date__c", "value": objDate },
                                                        { "field": "Finish_LAM__c", "value": recordObj['Finish(LAM)'] },
                                                        { "field": "HSNNO__c", "value": recordObj['HSNNO'] },
                                                        { "field": "Invoice_Date__c", "value": objDate },
                                                        { "field": "Name", "value": `${recordObj.Inv['No'].toString()}${recordObj['Line Item'].toString()}` },
                                                        { "field": "Line_Item__c", "value": recordObj['Line Item'] },
                                                        { "field": "Material__c", "value": Material_sfid[recordObj['Material']] },
                                                        { "field": "Material_Description__c", "value": recordObj['Material Desc.'] },
                                                        { "field": "PG1__c", "value": recordObj['PG1'] },
                                                        { "field": "PG2__c", "value": recordObj['PG2'] },
                                                        { "field": "PG3__c", "value": recordObj['PG3'] },
                                                        { "field": "PG4__c", "value": recordObj['PG4'] },
                                                        { "field": "Tax_Amount__c", "value": recordObj['Tax Amount'] },
                                                        { "field": "TCS__c", "value": recordObj['TCS'] },
                                                        { "field": "Territory_Name__c", "value": recordObj['TERRITORY'] },
                                                        { "field": "Thickness__c", "value": recordObj['THICKNESS'] },
                                                        { "field": "Unit__c", "value": recordObj['SALESUNIT'] },
                                                        { "field": "mode__c", "value": MODE_OF_UPDATE },
                                                        { "field": "cron_name__c", "value": cron_name }
                                                    ]
                                                    let whereClouse = [{ "field": "sfid", "value": recordObj.Inv['No'].toString()+recordObj['Line Item'].toString() }]
                                                    let record_updated = await qry.updateRecord(table_name, fieldValue, whereClouse);
                                                }else{
                                                    ++failed
                                                    dataPush.push({ 'status': 'failed', 'docNo': recordObj.Inv['No'], 'data': JSON.stringify(recordObj), 'message': 'Customer Code or Material dose not exist'})
                                                }
                                            }else{
                                                console.log("insert record")
                                                if(recordObj['Customer Code'] in Customer_Code_sfid && recordObj['Material'] in Material_sfid){
                                                    let objDate = dateZeroFix2(recordObj.Inv['Date'])
                                                    let item_obj={}
                                                    item_obj['Pgid__c']=uuidv4()
                                                    item_obj['Before_Tax_Amount__c']=recordObj['Before Tax Amount']
                                                    item_obj['Bill_Amount__c']=recordObj['BILLAMT']
                                                    item_obj['Bill_Quantity__c']=recordObj['BILLQTY']
                                                    item_obj['Dealer_Code__c']=recordObj['Customer Code']
                                                    item_obj['dealer_order__c']= Customer_Code_sfid[recordObj['Customer Code']]
                                                    item_obj['Design__c']=recordObj['DESIGN']
                                                    item_obj['Division_Text__c']=recordObj['Division Text']
                                                    item_obj['Due_Date__c']=objDate
                                                    item_obj['Finish_LAM__c']= recordObj['Finish(LAM)']
                                                    item_obj['HSNNO__c']= recordObj['HSNNO']
                                                    item_obj['Invoice_Date__c']=objDate
                                                    item_obj['Name']=`${recordObj.Inv['No'].toString()}${recordObj['Line Item'].toString()}`
                                                    item_obj['Line_Item__c']=recordObj['Line Item']
                                                    item_obj['Material__c']= Material_sfid[recordObj['Material']]
                                                    item_obj['Material_Description__c']= recordObj['Material Desc.']
                                                    item_obj['PG1__c']= recordObj['PG1']
                                                    item_obj['PG2__c']=recordObj['PG2']
                                                    item_obj['PG3__c']=recordObj['PG3']
                                                    item_obj['PG4__c']= recordObj['PG4']
                                                    item_obj['Tax_Amount__c']=recordObj['Tax Amount']
                                                    item_obj['TCS__c']= recordObj['TCS']
                                                    item_obj['Territory_Name__c']=recordObj['TERRITORY']
                                                    item_obj['Thickness__c']=recordObj['THICKNESS']
                                                    item_obj['Unit__c']=recordObj['SALESUNIT']
                                                    item_obj['mode__c']=MODE_OF_UPDATE
                                                    item_obj['cron_name__c']=cron_name
                                                    valuesToBeinserted.push(item_obj)
                                                }else{
                                                    ++failed
                                                    dataPush.push({ 'status': 'failed', 'docNo': recordObj.Inv['No'], 'data': JSON.stringify(recordObj), 'message': 'Customer Code or Material dose not exist'})
                                                }
                                            }                                                                
                                        }     
                                }
                                const t4=performance.now()
                                console.log("valuesToBeinserted----------->",valuesToBeinserted.length,valuesToBeinserted)
                                if(valuesToBeinserted.length>0){
                                    await insertManyRecord(fieldsTObeInserted, valuesToBeinserted, table_name)
                                }
                                console.log("time taken in loop -----------> ",t4-t3)
                                console.log("roud--->", i)
                            }  
                        }
                        let get_file=await logs.createPrimarySaleslogFile(dataPush)
                        // send mail to 
                        if(failed!=0){
                            sendMailFTP(`Mapping ${file}`,`${failed} records failed in ${file} file mapping`,'primary_sales',get_file)
                        }                
                        // delete all the file from folder   
                        fs.unlink(path.join(primarySalesdirectoryPath, file), err => {
                            if (err) throw err;
                        });
                    }
                    }
                });
                console.log("End here")
            } catch (e) {
                console.log("error",e.message)
                await email_error_log('error while primarysale_Upload_And_Mapping file',e)

            }
        },
        {scheduled:true}
        )
        // Fully optimized outstanding--03 16 1 * *
        const outstanding_Upload_And_Mapping=cron.schedule(`22 10 * * *`,
        async ()=>{
            try {
                let cron_name = 'outstanding_Upload_And_Mapping'
                const outStandingPath = path.join(__dirname, IN_outstanding_data_PATH);
                console.log("outStandingPath", outStandingPath)
                fs.readdir(outStandingPath, async (err, files) => {
                    if (err) {
                        return console.log('Unable to load directory: ', err);
                    }
                    console.log('started mapping outStanding')
                    for (let index = 0; index < files.length; index++) {
                        const file = files[index];
                        if(file!='.gitkeep'){
                        let failed = 0
                            let file_path = path.join(outStandingPath, file)
                            let dataPush = []
                            const jsonArray = await csv().fromFile(file_path);
                            const table_name = SF_OUT_STANDING_TABLE_NAME;
                            let delete_sql = `DELETE FROM salesforce.${SF_OUT_STANDING_TABLE_NAME}`
                            let delter_res = await client.query(delete_sql)
                            let temp_j=0;
                            let i_loop_length=jsonArray.length
                            if(i_loop_length<=1000){
                                const t0 = performance.now();
                                let fieldsTObeInserted=['Pgid__c,customer__c,bill_number__c,Name,bill_date__c,net_outstanding__c,customer_id__c,mode__c,cron_name__c']
                                let valuesToBeinserted=[]
                                let valuesToBeUpdated={}
                                let map_customer_code={}
                                let map_customer_sfid={}
                                let map_bil_no={}
                                let map_bil_sfid={}
                                let outstanding_amount_map={}
                                let temp_k=temp_j; 
                                for (temp_j; temp_j < i_loop_length; temp_j++) {
                                    const recordObj = jsonArray[temp_j];
                                    let validationError = []
                                    validation.issetNotEmpty(recordObj['Customer']) ? true : validationError.push({ "field": "Customer", "message": "Customer  is empty" });
                                    validation.issetNotEmpty(recordObj['Bil NO']) ? true : validationError.push({ "field": "Bil NO", "message": "Bil NO  is empty" });
                                        if (validationError.length == 0) {
                                            console.log(temp_j,'-------------------')
                                            map_customer_code[recordObj['Customer']]=''
                                            map_bil_no[recordObj['Bil NO']]=''
                                            recordObj['Bil Date']= dateZeroFix2(recordObj['Bil Date'])
                                        } else {
                                            dataPush.push({ 'status': 'failed', 'docNo': recordObj['Sales Document'], 'data': JSON.stringify(recordObj), 'message': JSON.stringify(validationError) })
                                            ++failed
                                        }
                                }
                                if(Object.keys(map_customer_code).length>0 && Object.keys(map_bil_no).length>0){
                                    let get_all_customer_sfid_sql = `select sfid,account_id__c,total_outstanding__c from salesforce.account where account_id__c IN ('${Object.keys(map_customer_code).join("','")}')`
                                    console.log("get_all_customer_sfid_sql",get_all_customer_sfid_sql)
                                    let get_all_customer_sfid_res=await client.query(get_all_customer_sfid_sql)
                                    get_all_customer_sfid_res.rows.map((obj)=>{
                                        map_customer_sfid[obj.account_id__c]=obj.sfid
                                        console.log("obj.total_outstanding__c",obj.total_outstanding__c)
                                        outstanding_amount_map[obj.account_id__c]=obj.total_outstanding__c
                                    })
                                    let get_all_bil_sfid_sql = `select sfid,name from salesforce.primary_sales__c where name IN ('${Object.keys(map_bil_no).join("','")}')`
                                    console.log("get_all_bil_sfid_sql",get_all_bil_sfid_sql)
                                    let get_all_bil_sfid_sql_res=await client.query(get_all_bil_sfid_sql)
                                    get_all_bil_sfid_sql_res.rows.map((obj)=>{
                                        map_bil_sfid[obj.name]=obj.sfid
                                    })   
                                    for (temp_k; temp_k < i_loop_length; temp_k++){
                                            const recordObj = jsonArray[temp_k];
                                        if(recordObj['Customer'] in map_customer_sfid && recordObj['Bil NO'] in map_bil_sfid){
                                            let item_obj={}
                                            item_obj['Pgid__c']=uuidv4()
                                            item_obj['customer__c']=map_customer_sfid[recordObj['Customer']]
                                            item_obj['bill_number__c']=map_bil_sfid[recordObj['Bil NO']]
                                            item_obj['Name']=recordObj['Document No']
                                            item_obj['bill_date__c']=recordObj['Bil Date']
                                            item_obj['net_outstanding__c']=recordObj['Net Outstanding']
                                            item_obj['customer_id__c']=recordObj['Customer']
                                            item_obj['mode__c']=MODE_OF_UPDATE
                                            item_obj['cron_name__c']=cron_name
                                            valuesToBeinserted.push(item_obj)
                                            let todayDate = dtUtil.todayDate()
                                            let diff_time = moment(todayDate).diff(recordObj['Bil Date'])
                                            console.log("diff_time",diff_time)
                                            let days = Number(diff_time / 86400000)
                                            console.log("difference_days", days)
                                            let spcl_clm = ''
                                            if (days <= 20) {
                                                spcl_clm = 'a0_20_days__c'
                                            }
                                            if (20 < days && days < 30) {
                                                spcl_clm = 'a21_29_days__c'
                                            }
                                            if (29 < days && days < 45) {
                                                spcl_clm = 'a30_44_days__c'
                                            }
                                            if (44 < days && days < 60) {
                                                spcl_clm = 'a45_59_days__c'
                                            }
                                            if (days >= 60) {
                                                spcl_clm = 'a60_above_days__c'
                                            }
                                            if (spcl_clm != '') {
                                                if(spcl_clm in valuesToBeUpdated){
                                                    valuesToBeUpdated[spcl_clm].push({'sfid':map_customer_sfid[recordObj['Customer']],value:recordObj['Net Outstanding']})
                                                }else{
                                                    valuesToBeUpdated[spcl_clm]=[{'sfid':map_customer_sfid[recordObj['Customer']],value:recordObj['Net Outstanding']}]
                                                }
                                                if('Total_Outstanding__c' in valuesToBeUpdated){
                                                    valuesToBeUpdated['Total_Outstanding__c'].push({'sfid':map_customer_sfid[recordObj['Customer']],value:Number(outstanding_amount_map[recordObj['Customer']])+Number(recordObj['Net Outstanding'])})
                                                }else{
                                                    valuesToBeUpdated['Total_Outstanding__c']=[{'sfid':map_customer_sfid[recordObj['Customer']],value:Number(outstanding_amount_map[recordObj['Customer']])+Number(recordObj['Net Outstanding'])}]
                                                }
                                            }
                                            }else{
                                            dataPush.push({ 'status': 'failed', 'docNo': recordObj['Document No'], 'data': JSON.stringify(recordObj), 'message': `Customer or Bil NO dosen't exist ` })
                                            ++failed
                                            }
                                    }
                
                                }
                                console.log("valuesToBeinserted----------->",valuesToBeinserted.length)
                                console.log("valuesToBeUpdated----------->",valuesToBeUpdated)
                                if(valuesToBeinserted.length>0){
                                    await insertManyRecord(fieldsTObeInserted, valuesToBeinserted, table_name)
                                }
                                if(Object.keys(valuesToBeUpdated).length>0){
                                    await updateManyCustom(valuesToBeUpdated,table_name)
                                }
                                const t1 = performance.now();
                                console.log(`time taking in outStandingPath ${t1 - t0} milliseconds.`);
                                console.log("roud--->", 1)
                            }else{
                                let j_loop_length=0
                                for (let i = 1; i <= (Math.ceil(i_loop_length/1000)); i++) {
                                    const t0 = performance.now();
                                    if((i_loop_length-j_loop_length)>1000){
                                        j_loop_length=(i)*1000
                                    }else{
                                        j_loop_length=i_loop_length
                                    }
                                    let fieldsTObeInserted=['Pgid__c,customer__c,bill_number__c,Name,bill_date__c,net_outstanding__c,customer_id__c,mode__c,cron_name__c']
                                    let valuesToBeinserted=[]
                                    let valuesToBeUpdated={}
                                    let map_customer_code={}
                                    let map_customer_sfid={}
                                    let map_bil_no={}
                                    let map_bil_sfid={}
                                    let outstanding_amount_map={}
                                    let temp_k=temp_j; 
                                    for (temp_j; temp_j < j_loop_length; temp_j++) {
                                        const recordObj = jsonArray[temp_j];
                                        let validationError = []
                                        validation.issetNotEmpty(recordObj['Customer']) ? true : validationError.push({ "field": "Customer", "message": "Customer  is empty" });
                                        validation.issetNotEmpty(recordObj['Bil NO']) ? true : validationError.push({ "field": "Bil NO", "message": "Bil NO  is empty" });
                                            if (validationError.length == 0) {
                                                console.log(temp_j,'-------------------')
                                                map_customer_code[recordObj['Customer']]=''
                                                map_bil_no[recordObj['Bil NO']]=''
                                                recordObj['Bil Date']= dateZeroFix2(recordObj['Bil Date'])
                                            } else {
                                                dataPush.push({ 'status': 'failed', 'docNo': recordObj['Sales Document'], 'data': JSON.stringify(recordObj), 'message': JSON.stringify(validationError) })
                                                ++failed
                                            }
                                    }
                                    if(Object.keys(map_customer_code).length>0 && Object.keys(map_bil_no).length>0){
                                        let get_all_customer_sfid_sql = `select sfid,account_id__c,total_outstanding__c from salesforce.account where account_id__c IN ('${Object.keys(map_customer_code).join("','")}')`
                                        console.log("get_all_customer_sfid_sql",get_all_customer_sfid_sql)
                                        let get_all_customer_sfid_res=await client.query(get_all_customer_sfid_sql)
                                        get_all_customer_sfid_res.rows.map((obj)=>{
                                            map_customer_sfid[obj.account_id__c]=obj.sfid
                                            outstanding_amount_map[obj.account_id__c]=obj.total_outstanding__c
                                        })
                                        let get_all_bil_sfid_sql = `select sfid,name from salesforce.primary_sales__c where name IN ('${Object.keys(map_bil_no).join("','")}')`
                                        console.log("get_all_bil_sfid_sql",get_all_bil_sfid_sql)
                                        let get_all_bil_sfid_sql_res=await client.query(get_all_bil_sfid_sql)
                                        get_all_bil_sfid_sql_res.rows.map((obj)=>{
                                            map_bil_sfid[obj.name]=obj.sfid
                                        })   
                                        for (temp_k; temp_k < j_loop_length; temp_k++){
                                                const recordObj = jsonArray[temp_k];
                                            if(recordObj['Customer'] in map_customer_sfid && recordObj['Bil NO'] in map_bil_sfid){
                                                let item_obj={}
                                                item_obj['Pgid__c']=uuidv4()
                                                item_obj['customer__c']=map_customer_sfid[recordObj['Customer']]
                                                item_obj['bill_number__c']=map_bil_sfid[recordObj['Bil NO']]
                                                item_obj['Name']=recordObj['Document No']
                                                item_obj['bill_date__c']=recordObj['Bil Date']
                                                item_obj['net_outstanding__c']=recordObj['Net Outstanding']
                                                item_obj['customer_id__c']=recordObj['Customer']
                                                item_obj['mode__c']=MODE_OF_UPDATE
                                                item_obj['cron_name__c']=cron_name
                                                valuesToBeinserted.push(item_obj)
                                                let todayDate = dtUtil.todayDate()
                                                let diff_time = moment(todayDate).diff(recordObj['Bil Date'])
                                                let days = Number(diff_time / 86400000)
                                                console.log("difference_days", days)
                                                let spcl_clm = ''
                                                if (days <= 20) {
                                                    spcl_clm = 'a0_20_days__c'
                                                }
                                                if (20 < days && days < 30) {
                                                    spcl_clm = 'a21_29_days__c'
                                                }
                                                if (29 < days && days < 45) {
                                                    spcl_clm = 'a30_44_days__c'
                                                }
                                                if (44 < days && days < 60) {
                                                    spcl_clm = 'a45_59_days__c'
                                                }
                                                if (days >= 60) {
                                                    spcl_clm = 'a60_above_days__c'
                                                }
                                                if (spcl_clm != '') {
                                                    if(spcl_clm in valuesToBeUpdated){
                                                        valuesToBeUpdated[spcl_clm].push({'sfid':map_customer_sfid[recordObj['Customer']],value:''})
                                                    }else{
                                                        valuesToBeUpdated[spcl_clm]=[{'sfid':map_customer_sfid[recordObj['Customer']],value:''}]
                                                    }
                                                    console.log("outstanding_amount_map[recordObj['Customer']]",outstanding_amount_map[recordObj['Customer']])
                                                    if('Total_Outstanding__c' in valuesToBeUpdated){
                                                        valuesToBeUpdated['Total_Outstanding__c'].push({'sfid':map_customer_sfid[recordObj['Customer']],value:Number(outstanding_amount_map[recordObj['Customer']])+Number(recordObj['Net Outstanding'])})
                                                    }else{
                                                        valuesToBeUpdated['Total_Outstanding__c']=[{'sfid':map_customer_sfid[recordObj['Customer']],value:Number(outstanding_amount_map[recordObj['Customer']])+Number(recordObj['Net Outstanding'])}]
                                                    }
                                                }
                                                }else{
                                                dataPush.push({ 'status': 'failed', 'docNo': recordObj['Document No'], 'data': JSON.stringify(recordObj), 'message': `Customer or Bil NO dosen't exist ` })
                                                ++failed
                                                }
                                        }
                    
                                    }
                                    console.log("valuesToBeinserted----------->",valuesToBeinserted.length)
                                    console.log("valuesToBeUpdated----------->",Object.keys(valuesToBeUpdated).length)
                                    if(valuesToBeinserted.length>0){
                                        await insertManyRecord(fieldsTObeInserted, valuesToBeinserted, table_name)
                                    }
                                    if(Object.keys(valuesToBeUpdated).length>0){
                                        await updateManyCustom(valuesToBeUpdated,table_name)
                                    }
                                    const t1 = performance.now();
                                    console.log(`time taking in outStandingPath ${t1 - t0} milliseconds.`);
                                }  
                            }
                            let get_file=await logs.createoutstandinglogFile(dataPush)
                            if(failed!=0){
                                sendMailFTP(`Mapping ${file}`,`${failed} records failed in ${file} file mapping`,'outstanding',get_file)
                            }
                            // fs.unlink(path.join(outStandingPath, file), err => {
                            //     if (err) throw err;
                            // });
                    }
                    }
                        
                });
                console.log("End here")
            } catch (e) {
                console.log("error",e.message)
                mail.email_error_log('error while outstanding_Upload_And_Mapping file',e.message)
            }
        },
        {scheduled:true}
        )
        const creditlimit_Upload_And_Mapping=cron.schedule(`00 10 1 * *`,
        async ()=>{
            try {
                let cron_name = 'creditlimit_Upload_And_Mapping'
                const creditLimittDirectoryPath = path.join(__dirname, '/files/IN/credit_limit');
                fs.readdir(creditLimittDirectoryPath, async (err, files) => {
                    if (err) {
                        return console.log('Unable to load directory: ', err);
                    }
                    console.log('started mapping creditLimit')
                    for (let index = 0; index < files.length; index++) {
                        const file = files[index];
                        if(file!='.gitkeep'){
                        const dataPush = []
                        let file_path = path.join(creditLimittDirectoryPath, file)
                        const jsonArray = await csv().fromFile(file_path);
                        let failed=0
                        for (let j = 0; j < jsonArray.length; j++) {
                            const recordObj = jsonArray[j];
                            let validationError = []
                            validation.issetNotEmpty(recordObj['Dealer Code']) ? true : validationError.push({ "field": "Dealer Code", "message": "Dealer Code is empty." });
                            validation.issetNotEmpty(recordObj['Total Limit']) ? true : validationError.push({ "field": "Total Limit", "message": "Total Limit is empty" });
                            if (validationError.length == 0) {
                                let customer_id = null;
                                let get_name_of_cust_sql = `select sfid from salesforce.account where account_id__c='${recordObj['Customer']}'`
                                let get_name_of_cust_result = await client.query(get_name_of_cust_sql);
                                if (get_name_of_cust_result.rows.length > 0) {
                                    customer_id = get_name_of_cust_result.rows[0]['customer_id']
                                } else {
                                    validationError.push({ "field": "Dealer Code", "message": "Dealer Code dosen't exist" })
                                }
                                if (validationError.length == 0) {
                                    let account_table_name = SF_ACCOUNT_TABLE_NAME
                                    let fieldValue = [
                                        { "field": 'balance_credit_limit__c', "value": recordObj['Total Limit'] },
                                        { "field": 'mode__c', "value": MODE_OF_UPDATE },
                                        { "field": 'cron_name__c', "value": cron_name }
                                    ]
                                    let whereClouse = [{ "field": "sfid", "value": customer_id }]
                                    //const fieldValue = [customer_id,recordObj['Bil NO'],recordObj['Document No'],recordObj['Bil Date'],lob,recordObj['Net_Outstanding__c'],recordObj['Customer']];
                                    let account_update = await qry.updateRecord(account_table_name, fieldValue, whereClouse);
                                    console.log("outstanding amount in account table ", account_update.success)
                                    dataPush.push({ 'status': 'updated', 'docNo': recordObj['Dealer Code'], 'data': JSON.stringify(recordObj), 'message': JSON.stringify(validationError) })
                                } else {
                                    ++failed
                                    dataPush.push({ 'status': 'failed', 'docNo': recordObj['Dealer Code'], 'data': JSON.stringify(recordObj), 'message': JSON.stringify(validationError) })
                                }
            
                            } else {
                                ++failed
                                dataPush.push({ 'status': 'failed', 'docNo': recordObj['Dealer Code'], 'data': JSON.stringify(recordObj), 'message': JSON.stringify(validationError) })
                            }
                        }    
                        let get_file=await logs.createCreditLimitlogFile(dataPush)
                        if(failed!=0){
                            sendMailFTP(`Mapping ${file}`,`${failed} records failed in ${file} file mapping`,'credit_limit',get_file)
                        }    
                        //delete all the file from folder   
                        fs.unlink(path.join(creditLimittDirectoryPath, file), err => {
                            if (err) throw err;
                        });
                        
                    }}
                });
                console.log("End here")
            } catch (e) {
                console.log("error",e.message)
                mail.email_error_log('error while creditlimit_Upload_And_Mapping file',e.message)
            }
        },
        {scheduled:true}
        )
        const all_file_upload=cron.schedule(`47 16 * * *`,
        async ()=>{
            try {
                var c = new Client_ftp();
                let config = {
                    host: '10.12.100.2',
                    port: 1993,
                    user: 'zoxima',
                    password: 'Cpil#1986'
                }
                c.connect(config);
                c.on('ready', function () {
                    // upload area logs
                    fs.readdir(path.join(__dirname, OUT_area_PATH), (err, files) => {
                        if (err) throw err
                        for (let i = 0; i < files.length; i++) {
                            const fileName = files[i];
                            console.log("fileName", path.join(__dirname, OUT_area_PATH, fileName))
                            c.append(path.join(__dirname, OUT_area_PATH, fileName), `/zoxima/upload_logs/${fileName}`, false, function (err) {
                                if (err) throw err;
                                c.end();
                            });
                            fs.unlink(path.join(__dirname, OUT_area_PATH, fileName), err => {
                                if (err) throw err;
                            });
                        }
                    })
                    // upload dealer logs
                    fs.readdir(path.join(__dirname, OUT_dealer_DATA_PATH), (err, files) => {
                        if (err) throw err
                        for (let i = 0; i < files.length; i++) {
                            const fileName = files[i];
                            console.log("fileName", path.join(__dirname, OUT_dealer_DATA_PATH, fileName))
                            c.append(path.join(__dirname, OUT_dealer_DATA_PATH, fileName), `/zoxima/upload_logs/${fileName}`, false, function (err) {
                                if (err) throw err;
                                c.end();
                            });
                            fs.unlink(path.join(__dirname, OUT_dealer_DATA_PATH, fileName), err => {
                                if (err) throw err;
                            });
                        }
                    })
                    // upload outstanding logs
                    fs.readdir(path.join(__dirname, OUT_outstanding_DATA_PATH), (err, files) => {
                        if (err) throw err
                        for (let i = 0; i < files.length; i++) {
                            const fileName = files[i];
                            console.log("fileName", path.join(__dirname, OUT_outstanding_DATA_PATH, fileName))
                            c.append(path.join(__dirname, OUT_outstanding_DATA_PATH, fileName), `/zoxima/upload_logs/${fileName}`, false, function (err) {
                                if (err) throw err;
                                c.end();
                            });
                            fs.unlink(path.join(__dirname, OUT_outstanding_DATA_PATH, fileName), err => {
                                if (err) throw err;
                            });
                        }
                    })
                    // upload pending order
                    fs.readdir(path.join(__dirname, OUT_pending_order_DATA_PATH), (err, files) => {
                        if (err) throw err
                        for (let i = 0; i < files.length; i++) {
                            const fileName = files[i];
                            console.log("fileName", path.join(__dirname, OUT_pending_order_DATA_PATH, fileName))
                            c.append(path.join(__dirname, OUT_pending_order_DATA_PATH, fileName), `/zoxima/upload_logs/${fileName}`, false, function (err) {
                                if (err) throw err;
                                c.end();
                            });
                            fs.unlink(path.join(__dirname, OUT_pending_order_DATA_PATH, fileName), err => {
                                if (err) throw err;
                            });
                        }
                    })
                    // upload credit limit
                    fs.readdir(path.join(__dirname, OUT_credit_limit_PATH), (err, files) => {
                        if (err) throw err
                        for (let i = 0; i < files.length; i++) {
                            const fileName = files[i];
                            console.log("fileName", path.join(__dirname, OUT_credit_limit_PATH, fileName))
                            c.append(path.join(__dirname, OUT_credit_limit_PATH, fileName), `/zoxima/upload_logs/${fileName}`, false, function (err) {
                                if (err) throw err;
                                c.end();
                            });
                            fs.unlink(path.join(__dirname, OUT_credit_limit_PATH, fileName), err => {
                                if (err) throw err;
                            });
                        }
                    })
                    // upload pending order
                    fs.readdir(path.join(__dirname, OUT_product_item_DATA_PATH), (err, files) => {
                        if (err) throw err
                        for (let i = 0; i < files.length; i++) {
                            const fileName = files[i];
                            console.log("fileName", path.join(__dirname, OUT_product_item_DATA_PATH, fileName))
                            c.append(path.join(__dirname, OUT_product_item_DATA_PATH, fileName), `/zoxima/upload_logs/${fileName}`, false, function (err) {
                                if (err) throw err;
                                c.end();
                            });
                            fs.unlink(path.join(__dirname, OUT_product_item_DATA_PATH, fileName), err => {
                                if (err) throw err;
                            });
                        }
                    })
                });
                console.log("End here")
            } catch (e) {
                console.log("error while uploading file", e)
            }
        },
        {scheduled:true}
        )
        module.exports = {
            all_file_download,
            all_file_upload,
            product_Upload_And_Mapping,
            area_Upload_And_Mapping,
            dealer_Upload_And_Mapping,
            pendingorder_Upload_And_Mapping,
            primarysale_Upload_And_Mapping,
            outstanding_Upload_And_Mapping,
            creditlimit_Upload_And_Mapping   
        }
