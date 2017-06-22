var async = require('async');
var fs = require('fs');
var path = require('path');

var s3 = require('./s3.js');
var airflow = require('./airflow.js');
var providers = require('./providers.js');
var helpers = require('./helpers.js');

// HTML to be displayed
var html = fs.readFileSync(path.join(__dirname, './index.html'), 'utf-8')
    .replace('{{CSS}}', fs.readFileSync(path.join(__dirname, './style.css'), 'utf-8'))
    .replace('{{JAVASCRIPT}}', fs.readFileSync(path.join(__dirname, './main.js'), 'utf-8'));

function getIncomingBucket(s3Prefix) {
  return s3Prefix.split('/').slice(1, s3Prefix.split('/').length - 1).reduce(function(b1, b2) {
    return b1 + '/' + b2;
  });
}

// join airflow data to s3 and add in dates that are missing
function buildFullDataset(airflowResults, providerIncomingFiles) {
  var providerColumnIndex = airflowResults.fields.findIndex(function(airflowResultField) {
    return airflowResultField.name == getIncomingBucket(providerIncomingFiles[0]);
  });

  var providerConf = providers.config.filter(function(provider) {
    return provider.incomingBucket == getIncomingBucket(providerIncomingFiles[0]);
  })[0];

  // enumerate all execution dates for this provider
  var executionDates = [providerConf.startDate];
  function nextDate() {
    return providerConf.schedule(executionDates[executionDates.length-1]);
  }
  while (nextDate() <= Date.now()) {
    executionDates.push(nextDate());
  } 

  return executionDates.map(function(exDate) {
    var airflowData = airflowResults.rows.filter(function(resultRow) {
      var date = new Date(resultRow[0]);
      var formatted = (
        (1900 + date.getYear()) + '-'
          + helpers.leftZPad((date.getMonth() + 1).toString()) + '-'
          + helpers.leftZPad(date.getDate().toString())
      );

      return formatted === helpers.formatDate(exDate);
    })[0];

    var ingested = typeof airflowData !== 'undefined' && airflowData[providerColumnIndex].toString().trim() === "1";

    var incomingFiles = providerIncomingFiles.filter(function(file) {
      return providerConf.filenameToExecutionDate(file) == helpers.formatDate(exDate);
    });

    return {
      executionDate: helpers.formatDate(exDate),
      incomingFiles: incomingFiles,
      ingested: ingested
    };

  });
}

function displayJoinedObject(obj) {
  return obj.executionDate;
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

        var allData = buildFullDataset(airflowRes, relevantIncomingFiles).sort(function(row1, row2) {
          if (row1.executionDate < row2.executionDate) return 1;
          else if (row1.executionDate > row2.executionDate) return -1;
          else return 0;
        });

        var existingFiles = allData.filter(function(d) {
          return d.incomingFiles.length > 0;
        });

        var providerCSSId = providerConf.displayName.toLowerCase().replace(' ', '-');

        return {
          // date ingested HTML for this provider
          dateIngestedContent: '<tr>' +
            '<td><a href="#" id="' + providerCSSId + '">' + providerConf.displayName + '</a></td>' +
            '<td>' + displayJoinedObject(existingFiles[0])+ '</td>' +
            '<td>' + displayJoinedObject(
              existingFiles.filter(function(row) {
                return row.ingested;
              })[0]
            ) + '</td>' +
            '</tr>',

          // time series HTML for this provider
          timeSeriesContent: '<ul id="' + providerCSSId + '">' +
            allData.map(function(d) {
              var dateClass;
              if (d.incomingFiles.length === 0) dateClass = 'not-sent';
              else if (!d.ingested) dateClass = 'not-ingested';
              else dateClass = 'fully-loaded';
              return '<li class=' + dateClass + '>' +
                d.executionDate + '</li>';
            }).reduce(function (el1, el2) {
              return el1 + el2;
            }) + '</ul>'
        };
      });

      var output = html.replace('{{DATE_INGESTED_CONTENT}}', content.map(function(c) {
        return c.dateIngestedContent;
      }).reduce(function(el1, el2) {
        return el1 + el2;
      })).replace('{{TIME_SERIES_CONTENT}}', content.map(function(c) {
        return c.timeSeriesContent;
      }).reduce(function(el1, el2) {
        return el1 + el2;
      }));

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
