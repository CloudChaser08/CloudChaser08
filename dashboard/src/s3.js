var AWS = require('aws-sdk');
var async = require('async');

var providers = require('./providers.js');

var s3 = new AWS.S3();

exports.getS3Calls = function() {
  var s3Calls = [];

  return providers.config.map(function(providerConf) {
    return function(callback) {
      var params = {
        Bucket : 'healthverity',
        Prefix : 'incoming/' + providerConf.incomingBucket
      };

      s3.listObjects(params, function(err, data) {
        if (err) callback(err);
        else callback(null, data.Contents.map(function(key) {
          return key.Key;
        }));
      });
    };
  });
};
