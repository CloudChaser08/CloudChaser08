/*
 * Functions for interacting with data in S3
 */

var AWS = require('aws-sdk');
var async = require('async');

var providers = require('./providers.js');

var s3 = new AWS.S3();

/**
 * Get a list of calls that will be made out to s3 - one for each
 * provider in the providers.config map. Each call will return a list
 * of files in that provider's incoming bucket that apply to this
 * provider.
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
              return {
                "key": key.Key,
                "date": key.LastModified
              };
            }));
            if (!data.IsTruncated) {
              // attach provider id to a list of relevant files found
              // in this provider's incoming bucket
              var output = {
                providerId: providerConf.id,
                files: results.filter(function(filename) {
                  return providerConf.expectedFilenameRegex.test(filename.key);
                })
              };
              callback(null, output);
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
    Key: filename.split('/').slice(3).join('/'),
    ServerSideEncryption: "AES256",
    ContentType: 'text/html'
  };

  s3.putObject(params, callback);
};
