var async = require('async');
var fs = require('fs');
var path = require('path');

var s3 = require('./s3.js');
var airflow = require('./airflow.js');
var providers = require('./providers.js');
var helpers = require('./helpers.js');

// HTML to be displayed
var html = fs.readFileSync(path.join(__dirname, './index.html'), 'utf-8');
var css = fs.readFileSync(path.join(__dirname, './style.css'), 'utf-8');

function joinAirflowToS3(airflowResults, providerIncomingFiles) {
  var providerColumnIndex = airflowResults.fields.findIndex(function(airflowResultField) {
    return airflowResultField.name == providerIncomingFiles[0].split('/')[1];
  });

  var providerConf = providers.config.filter(function(provider) {
    return provider.incomingBucket == providerIncomingFiles[0].split('/')[1];
  })[0];

  return providerIncomingFiles.map(function(filename) {
    var airflowData = airflowResults.rows.filter(function(resultRow) {
      var date = new Date(resultRow[0]);
      var formatted = (
        (1900 + date.getYear()) + '-'
          + helpers.leftZPad((date.getMonth() + 1).toString()) + '-'
          + helpers.leftZPad((date.getDate() + 1).toString())
      );
      return formatted === providerConf.filenameToDate(filename);
    })[0];

    var ingested = typeof airflowData !== 'undefined' && airflowData[providerColumnIndex] === "1";

    return {
      file: filename,
      ingested: ingested
    };
    
  });
}

// lambda entry point
exports.handler = function(event, context) {

  var calls = s3.getS3Calls();
  calls.push(airflow.getAirflowCall());

  // execute all calls in parallel
  async.parallel(calls, function(err, result) {
    if (err) context.fail(err);
    else {
      // pop off airflow query result
      var airflowRes = result.pop();
      console.log(airflowRes.fields);

      // each provider gets their own section
      var content = result.map(function (providerS3Res) {
        var providerConf = providers.config.filter(function(provider) {
          return provider.incomingBucket == providerS3Res[0].split('/')[1];
        })[0];
        var relevantIncomingFiles = providerS3Res.filter(function(filename) {
          return providerConf.expectedFilenameRegex.test(filename);
        });

        var joined = joinAirflowToS3(airflowRes, relevantIncomingFiles);

        console.log(joined);

        return '<tr>' +
          '<td>' + providerConf.displayName + '</td>' +
          '<td>' + relevantIncomingFiles[relevantIncomingFiles.length-1] + '</td>' +
          '<td>[LAST INGESTED]</td>' +
          '</tr>';
      }).reduce(function(el1, el2) {
        return el1 + el2;
      });

      var output = html.replace('{{CONTENT}}', content).replace('{{CSS}}', css);

      fs.writeFile(path.join(__dirname, './test.html'), output, function(err) {
        if(err) {
          console.log(err);
        }

        console.log("The file was saved!");
        // context.succeed(html.replace('{{CONTENT}}', content));
        context.succeed(output);
      }); 
    }
  });
};
