
require("dotenv").config();
let request = require("request"); 

async function sendWhatsAppMsg(senderNumber, apiName, templateName, placeholders_arr = []){
    /**
     * Params:
     * senderNumber: 919818508785
     * templateName: centuryply_intro
     * apiName: whatsapp/1/message/template
     * authorization: 'App 040de3d98c19c2f08f05cf2945ced6ea-658ff3ca-ecef-4f19-ac1d-9077fc6b2f52'
     * placeholders_arr: [ "Test", "test" ]
     */
    let options = {
    'method': 'POST',
    'url': `https://gy91ee.api.infobip.com/${apiName}`,
    'headers': {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'Authorization': process.env.WHATSAPP_AUTHORIZATION
    },
    body: JSON.stringify({
        "messages": [
            {
              "from": "916292241663",
              "to": `'91${senderNumber}'`,
              "messageId": "a28dd97c-1ffb-4fcf-99f1-0b557ed381da",
              "content": {
                "templateName": templateName,
                "templateData": {
                  "body": {
                    "placeholders": placeholders_arr
                  },
                  "header": {
                    "type": "VIDEO",
                    "mediaUrl": "https://dl3.pushbulletusercontent.com/XHoU4bFv3DgADPTwihuCbBmDNP6TJqzP/Ganesh%20Grains%20RCS.mp4"
                  }
                },
                "language": "en"
              },
              "callbackData": "Callback data",
              "notifyUrl": "https://www.example.com/whatsapp"
            }
          ]
    })

    };
    request(options, function (error, response) {
    if (error) throw new Error(error);
        console.log(response.body);
    });

}
async function sendWhatsAppMsg_intro(senderNumber, apiName, templateName, placeholders_arr = []){
  /**
   * Params:
   * senderNumber: 919818508785
   * templateName: centuryply_intro
   * apiName: whatsapp/1/message/template
   * authorization: 'App 040de3d98c19c2f08f05cf2945ced6ea-658ff3ca-ecef-4f19-ac1d-9077fc6b2f52'
   * placeholders_arr: [ "Test", "test" ]
   */
  let options = {
  'method': 'POST',
  'url': `https://gy91ee.api.infobip.com/${apiName}`,
  'headers': {
      'Content-Type': 'application/json',
      'Accept': 'application/json',
      'Authorization': process.env.WHATSAPP_AUTHORIZATION
  },
  body: JSON.stringify({
      "messages": [
          {
            "from": "916292241663",
            "to": `'91${senderNumber}'`,
            "messageId": "a28dd97c-1ffb-4fcf-99f1-0b557ed381da",
            "content": {
              "templateName": templateName,
              "templateData": {
                "body": {
                  "placeholders": placeholders_arr
                }
              },
              "language": "en"
            },
            "callbackData": "Callback data",
            "notifyUrl": "https://www.example.com/whatsapp"
          }
        ]
  })

  };
  request(options, function (error, response) {
  if (error) throw new Error(error);
      console.log(response.body);
  });

}

module.exports = {
    sendWhatsAppMsg,
    sendWhatsAppMsg_intro
};
