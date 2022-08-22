var config = {
    "aws" : {
          "bucket": process.env.AWS_BUCKET,
          "path": process.env.CLOUDCUBE_BUCKET,
          "credentials" : {
              "accessKeyId": process.env.CLOUDCUBE_ACCESS_KEY_ID,
              "secretAccessKey": process.env.CLOUDCUBE_SECRET_ACCESS_KEY
          }
      }
  };

var AWS = require('aws-sdk');

var getUniqueFilename = function(config) {
    var timestamp = (new Date()).getTime();
    var randomInteger = Math.floor((Math.random() * 1000000) + 1);
    return config.aws.path + timestamp + '_' + randomInteger + '.png';
};

const base64FileUpload = async(base64EncodedImage)=> {
    console.log('config ::::', config)

    return new Promise((resolve, reject) => {

        var buf = new  Buffer.from(base64EncodedImage, 'base64');
        AWS.config = config.aws.credentials;
        var key = getUniqueFilename(config);
        var s3 = new AWS.S3();
      
        s3.putObject({
            Bucket: config.aws.bucket,
            Key: key,
            Body: buf,
            ACL: 'public-read',
            ContentType: "image/png",
            ContentEncoding: "base64"
        }, function(error, result) {
               if (error) {  
                 reject(error);  
                 //console.log(error)
               } else {     
                   resolve(key);
               }
           });
   });
};

module.exports = base64FileUpload;

