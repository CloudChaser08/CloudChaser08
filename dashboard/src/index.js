var async = require('async');
var s3 = require('./s3.js');

// HTML to be displayed
var html = '<html><head><title>Provider Status Dashboard</title></head>' +
    '<body>' +
    '{{CONTENT}}' +
    '</body>' +
    '</html>';

// handler entry point
exports.handler = function(event, context) {

  var s3Calls = s3.getS3Calls();

  async.parallel(s3Calls, function(err, result) {
    if (err) context.fail(err);
    else {
      context.succeed(result);
    }
  });
};
