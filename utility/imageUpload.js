var express = require('express'),
    aws = require('aws-sdk'),
    bodyParser = require('body-parser'),
    multer = require('multer'),
    multerS3 = require('multer-s3');

aws.config.update({
    secretAccessKey: process.env.CLOUDCUBE_SECRET_ACCESS_KEY,
    accessKeyId: process.env.CLOUDCUBE_ACCESS_KEY_ID,
    region: process.env.AWS_REGION
});

var app = express(),
    s3 = new aws.S3();
app.use(bodyParser.json());



const fileFilter = (req, file, cb) => {
  if (file.mimetype === "image/jpeg" || file.mimetype === "image/png") {
    cb(null, true);
  } else {
    cb(new Error("Invalid file type, only JPEG and PNG is allowed!"), false);
  }
};

const upload = multer({
  fileFilter,
  storage: multerS3({
    acl: 'public-read',
    s3,
    bucket: process.env.AWS_BUCKET,
    key: function(req, file, cb) {
      // console.log(file);
      const myFileName = process.env.CLOUDCUBE_BUCKET + Math.floor((Math.random() * 1000000) + 1)+"_"+ Date.now()+file.originalname;
      // console.log(myFileName);
      cb(null, myFileName);
    }
  }),
});

module.exports = upload;