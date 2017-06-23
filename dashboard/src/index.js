/*
 * Main file containing the entry point for the lambda job
 */

var async = require('async');
var fs = require('fs');
var path = require('path');

var s3 = require('./s3.js');
var airflow = require('./airflow.js');
var providers = require('./providers.js');
var helpers = require('./helpers.js');

// HTML to be displayed
var html = fs.readFileSync(path.join(__dirname, './public/index.html'), 'utf-8')
    .replace('{{CSS}}', fs.readFileSync(path.join(__dirname, './public/style.css'), 'utf-8'))
    .replace('{{JAVASCRIPT}}', fs.readFileSync(path.join(__dirname, './public/main.js'), 'utf-8'));

/**
 * Combines airflow data for a single provider to the list of files in
 * the s3 incoming bucket for that provider
 */
function buildFullDataset(airflowResults, providerIncomingFiles) {

  // the index of the column in the tabular airflow data that
  // corresponds to this provider
  var providerColumnIndex = airflowResults.fields.findIndex(function(airflowResultField) {
    return airflowResultField.name == s3.getIncomingBucket(providerIncomingFiles[0]);
  });

  // the configuration for this provider
  var providerConf = providers.config.filter(function(provider) {
    return provider.incomingBucket == s3.getIncomingBucket(providerIncomingFiles[0]);
  })[0];

  // enumerate all execution dates for this provider
  var executionDates = [providerConf.startDate];
  function nextDate() {
    return providerConf.schedule(executionDates[executionDates.length-1]);
  }
  while (providerConf.schedule(nextDate()) <= Date.now()) {
    executionDates.push(nextDate());
  } 

  // return an array containing one entry for each execution date
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

    // this execution date will be considered 'ingested' if the
    // corresponding value in the airflow data is '1'
    var ingested = typeof airflowData !== 'undefined' && airflowData[providerColumnIndex].toString().trim() === "1";

    // grab list of incoming files for this execution date
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

/**
 * Simple function for disploying a 'joined' object (in the form
 * output from the buildFullDataset function)
 */
function displayJoinedObject(obj) {
  return obj.executionDate;
}

/**
 * Entry point for lambda job
 */
exports.handler = function(event, context) {

  // assembly all asynchronous calls
  var calls = s3.getS3Calls();
  calls.push(airflow.getAirflowCall());

  // execute all calls in parallel
  async.parallel(calls, function(err, result) {
    if (err) context.fail(err);
    else {
      // pop off airflow query result
      var airflowRes = result.pop();

      // assemble a 'content' object for each provider - each provider
      // will get an html snippet for the 'last ingested date' table,
      // as well as one for the date series table.
      var content = result.map(function (providerS3Res) {

        // conf for this provider
        var providerConf = providers.config.filter(function(provider) {
          return provider.incomingBucket == s3.getIncomingBucket(providerS3Res[0]);
        })[0];

        // filter incoming files based on expected filename regex
        var relevantIncomingFiles = providerS3Res.filter(function(filename) {
          return providerConf.expectedFilenameRegex.test(filename);
        });

        // join relevant incoming file list to the airflow data for
        // this provider
        var allData = buildFullDataset(airflowRes, relevantIncomingFiles).sort(function(row1, row2) {
          if (row1.executionDate < row2.executionDate) return 1;
          else if (row1.executionDate > row2.executionDate) return -1;
          else return 0;
        });

        // filter out days with no incoming files
        var existingFiles = allData.filter(function(d) {
          return d.incomingFiles.length > 0;
        });

        // convert the provider display name to a CSS id (cannot
        // contain spaces)
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

            // each execution date will get an <li> element
            allData.map(function(d) {

              // calculate class based on the status for this
              // execution date
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

      // insert provider content into the large HTML output blob
      var output = html.replace('{{DATE_INGESTED_CONTENT}}', content.map(function(c) {
        return c.dateIngestedContent;
      }).reduce(function(el1, el2) {
        return el1 + el2;
      })).replace('{{TIME_SERIES_CONTENT}}', content.map(function(c) {
        return c.timeSeriesContent;
      }).reduce(function(el1, el2) {
        return el1 + el2;
      }));

      // upload the HTML blob to s3
      s3.uploadFile(output, 's3://hvstatus.healthverity.com/test/provider-status-dash/index.html', function(err, data) {
        if(err) context.fail(err);
        else context.succeed();
      });

      // output file for testing
      // fs.writeFile(path.join(__dirname, 'test.html'), output, 'utf-8', function(err, data) {
        // if(err) context.fail(err);
        // else context.succeed();
      // });
    }
  });
};
