var AWS = require('aws-sdk');
var async = require('async');

var providers = require('./providers.js');

var s3 = new AWS.S3();

exports.getS3Calls = function() {
  return providers.config.map(function(providerConf) {
    return function(callback) {
      var results = [];

      function recursiveList(continuationToken) {
        var params = {
          Bucket : 'healthverity',
          Prefix : 'incoming/' + providerConf.incomingBucket + '/',
          Delimiter : '/'
        };
        if (continuationToken) {
          params.ContinuationToken = continuationToken;
        }
        var p = s3.listObjectsV2(params, function(err, data) {
          if (err) callback(err);
          else {
            results = results.concat(data.Contents.map(function(key) {
              return key.Key;
            }));
            if (!data.IsTruncated) {
              callback(null, results);
            } else {
              recursiveList(data.NextContinuationToken);
            }
          }
        });
      };

      recursiveList();
    };
  });
};

exports.uploadFile = function(content, filename, callback) {
  var params = {
    Body: content, 
    Bucket: filename.split('/')[2], 
    Key: filename.split('/').slice(3).reduce(function(x, y) {return x + '/' + y;}), 
    ServerSideEncryption: "AES256"
  };

  s3.putObject(params, callback);
};
