var AWS = require('aws-sdk');
var async = require('async');

var html = '<html><head><title>Provider Status Dashboard</title></head>' +
    '<body>' +
    '{{CONTENT}}' +
    '</body>' +
    '</html>';

var providers = [
  {
    displayName: 'Practice Insight',
    incomingBucket: 'practiceinsight'
  }
];

exports.handler = function(event, context) {
  var s3 = new AWS.S3();

  var calls = [];

  providers.map(function(prov) {
    return prov.incomingBucket;
  }).forEach(function(provider){
    calls.push(function(callback) {
      var params = {
        Bucket : 'healthverity',
        Prefix : 'incoming/' + provider
      };

      s3.listObjects(params, callback);
    });
  });

  async.parallel(calls, function(err, result) {
    if (err) console.log('err');
    else {
      console.log(result.map(function(awsResponse) {
        return awsResponse.Contents.map(function(key) {
          return key.Key;
        });
      }));
      context.succeed(html);
    }
  });

};
