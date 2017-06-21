var async = require('async');
var s3 = require('./s3.js');
var airflow = require('./airflow.js');

// HTML to be displayed
var html = '<html><head><title>Provider Status Dashboard</title></head>' +
    '<body>' +
    '{{CONTENT}}' +
    '</body>' +
    '</html>';

// handler entry point
exports.handler = function(event, context) {

  var calls = s3.getS3Calls();
  calls.push(airflow.getAirflowCall());

  async.parallel(calls, function(err, result) {
    if (err) context.fail(err);
    else {
      context.succeed(result);
    }
  });
};
