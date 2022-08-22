
require("dotenv").config();
var request = require("request"); 

var otpGenerator = require('otp-generator');

// var twilio = require('twilio');

// Find your account sid and auth token in your Twilio account Console.



function sendOTPLogin(mobile,otp)
{
     //*********************************************************** twillio*/
// Send the text message.
// const accountSid = process.env.TWILIO_ACCOUNT_SID;
// const authToken = process.env.TWILIO_AUTH_TOKEN;
// var client = new twilio(accountSid,authToken);


//         client.messages.create({
//         to: '+91'+mobile,
//         from: '+18324302571',
//         body: "Your OTP for login is"+otp
//         }).then(message => console.log(message.sid)).catch(err=>{
//             console.log(err)
//         });
//          //*********************************************************** textlocal*/
    // let options = {
    //     'apikey': `${process.env.TEXTLCL}`,
    //     'message':"This is a test message your one time password for activateing your Textlocal account is:",
    //     'sender': 'txtlcl',
    //     'numbers': mobile
    // }
    // return new Promise((resolve, reject) => {
    //     request.post({ url: 'https://api.textlocal.in/send/', form: options }, (error, response, body) => {
    //         if (error) {
    //             return reject(error);
    //         }
    //         console.log(body)
    //         resolve({ response, body });
    //     })
    // })
        //*********************************************************** msg91*/
        
    // console.log("/api/sendotp.php?authkey="+authkeyOtpApi+"&mobile="+data.phoneCode+data.phoneNumber+"&sender=***{sender name}***&country="+data.phoneCode+"&otp="+data.otp);
    // var options = {
    //   "method": "POST",
    //   "hostname": "control.msg91.com",
    //   "port": null,
    //   "path": "/api/sendotp.php?authkey="+authkeyOtpApi+"&mobile="+data.phoneCode+data.phoneNumber+"&sender=***{sender name}***&country="+data.phoneCode+"&otp="+data.otp,
    //   "headers": {}
    // }; 
    // var req = http.request(options, function (res) {
    // var chunks = [];
    //   res.on("data", function (chunk) {
    //     chunks.push(chunk);  
    //   });
    //   res.on("end", function () {
    //     var body = Buffer.concat(chunks); 
    //      console.log(body.toString());  
    //   });
    // }); 

    // req.end();
    var options = {
      'method': 'GET',
      // "hostname": "http://sms6.rmlconnect.net",
      // "port": 8080,
      'url': `http://sms6.rmlconnect.net:8080/bulksms/bulksms?username=centuryt&password=nh7rsegs&type=0&dlr=1&destination=91${mobile}&source=DM-CENTPY&message=Your OTP to login is ${otp}.\nDo not share the OTP to anyone -Century Plyboards India Ltd&entityid=1101541810000032089&tempid=1107163729933007602`,
      'headers': {
      }
    };
    request(options, function (error, response) {
      if (error) throw new Error(error);
      console.log(response.body);
    });
}

const generateOTP = () => { 
  return otpGenerator.generate(4, { upperCase: false, specialChars: false,alphabets:false});
};


  module.exports = {
    sendOTPLogin,
    generateOTP
};
