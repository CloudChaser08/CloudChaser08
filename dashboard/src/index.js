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

function getIncomingBucket(s3Prefix) {
  return s3Prefix.split('/').slice(1, s3Prefix.split('/').length - 1).reduce(function(b1, b2) {
    return b1 + '/' + b2;
  });
}

function joinAirflowToS3(airflowResults, providerIncomingFiles) {
  var providerColumnIndex = airflowResults.fields.findIndex(function(airflowResultField) {
    return airflowResultField.name == getIncomingBucket(providerIncomingFiles[0]);
  });

  var providerConf = providers.config.filter(function(provider) {
    return provider.incomingBucket == getIncomingBucket(providerIncomingFiles[0]);
  })[0];

  return providerIncomingFiles.map(function(filename) {
    var airflowData = airflowResults.rows.filter(function(resultRow) {
      var date = new Date(resultRow[0]);
      var formatted = (
        (1900 + date.getYear()) + '-'
          + helpers.leftZPad((date.getMonth() + 1).toString()) + '-'
          + helpers.leftZPad(date.getDate().toString())
      );

      return formatted === providerConf.filenameToExecutionDate(filename);
    })[0];

    var ingested = typeof airflowData !== 'undefined' && airflowData[providerColumnIndex].toString().trim() === "1";

    return {
      executionDate: providerConf.filenameToExecutionDate(filename),
      filename: filename,
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

      // each provider gets their own section
      var content = result.map(function (providerS3Res) {
        var providerConf = providers.config.filter(function(provider) {
          return provider.incomingBucket == getIncomingBucket(providerS3Res[0]);
        })[0];
        var relevantIncomingFiles = providerS3Res.filter(function(filename) {
          return providerConf.expectedFilenameRegex.test(filename);
        });

        var joined = joinAirflowToS3(airflowRes, relevantIncomingFiles).sort(function(row1, row2) {
          if (row1.executionDate < row2.executionDate) return 1;
          else if (row1.executionDate > row2.executionDate) return -1;
          else return 0;
        });

        return '<tr>' +
          '<td>' + providerConf.displayName + '</td>' +
          '<td>' + joined[0].filename + '</td>' +
          '<td>' + joined.filter(function(row) {
            return row.ingested;
          })[0].filename + '</td>' +
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
