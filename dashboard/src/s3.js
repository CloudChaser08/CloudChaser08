/*
 * Functions for interacting with data in S3
 */

var AWS = require('aws-sdk');
var async = require('async');

var providers = require('./providers.js');

var s3 = new AWS.S3();

/**
 * Get the 'incoming bucket' for this path, defined here as the chunk
 * of the path between incoming and the actual filename. 
 *
 * Ex: incoming/<incoming-bucket>/filename.gz => <incoming-bucket>
 */
exports.getIncomingBucket = function(s3Prefix) {
  return s3Prefix.split('/').slice(1, s3Prefix.split('/').length - 1).reduce(function(b1, b2) {
    return b1 + '/' + b2;
  });
};

/**
 * Get a list of calls that will be made out to s3 - one for each
 * provider in the providers.config map. Each call will return a list
 * of files in that provider's incoming bucket.
 */
exports.getS3Calls = function() {
  return providers.config.map(function(providerConf) {
    return function(callback) {
      var results = [];

      // this function is used for pagination - the aws sdk only
      // returns 1000 files per request.
      function recursiveList(continuationToken) {
        var params = {
          Bucket : 'healthverity',
          Prefix : 'incoming/' + providerConf.providerPrefix + '/',
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

/**
 * Upload text content to an HTML file in s3. This function is
 * asynchronous, it will call the provided 'callback' function when it
 * is finished.
 */
exports.uploadFile = function(content, filename, callback) {
  var params = {
    Body: content, 
    Bucket: filename.split('/')[2], 
    Key: filename.split('/').slice(3).reduce(function(x, y) {return x + '/' + y;}), 
    ServerSideEncryption: "AES256",
    ContentType: 'text/html'
  };

  s3.putObject(params, callback);
};
