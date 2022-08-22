const nodemailer = require('nodemailer');
const path = require('path');

const to_mail = 'abhishek.tiwari@zoxima.com';
const cc_mail = 'ajay.verma@zoxima.com';
const temp_cc = ''
// const to_mail = 'jyoti.kumari@zoxima.com';
// const cc_mail = 'jyoti.kumari@zoxima.com';

module.exports = {
    email_error_log,
    email_lead_drop_log,
    sendMailFTP
};
async function sendMailFTP(subject, text, folder_name, file_name) {
    const FileDirectoryPath = path.join(__dirname, `../FTP/files/OUT/${folder_name}/${file_name}`);


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

function email_error_log(subject, text) {
    console.log("***************----------entered in mail")
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
        text: `${text}`
    };

    transporter.sendMail(mailOptions, function (error, info) {
        if (error) {
            console.log(error);
        } else {
            console.log('Email sent: ' + info.response);
        }
    });
}
function email_lead_drop_log(subject, text, id) {
    console.log(`Inside Email`);
    var transporter = nodemailer.createTransport({
        service: 'gmail',
        auth: {
            user: 'api.backend@zoxima.com',
            pass: 'sqrzcqidvbhfowbj',
        }
    });

    var mailOptions = {
        //team member
        to: `${id}`,
        cc: `${temp_cc}`,
        subject: `${subject}`,
        text: `${text}`
    };

    transporter.sendMail(mailOptions, function (error, info) {
        if (error) {
            console.log(error);
        } else {
            console.log('Email sent: ' + info.response);
        }
    });
}