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

/**
 * Combines airflow data for a single provider to the list of files in
 * the s3 incoming bucket for that provider
 */
function buildFullDataset(airflowResults, providerIncoming) {

  // the configuration for this provider
  var providerConf = providers.config.filter(function(provider) {
    return provider.id == providerIncoming.providerId;
  })[0];

  if (!providerConf.airflowPipelineName)
  {
    // Create dataset for non-automated providers
    return providerIncoming.files.map(function(f) {
      return {
        executionDate: helpers.formatDate(f.date),
        incomingFiles: [f.key],
        expectedFile: null,
        ingested: false
      };
    });
  }
  else
  {
    // the index of the column in the tabular airflow data that
    // corresponds to this provider
    var providerColumnIndex = airflowResults.fields.findIndex(function(airflowResultField) {
      return airflowResultField.name == providerIncoming.providerId;
    });

    // enumerate all execution dates for this provider
    var executionDates = [providerConf.startDate];
    function nextDate() {
      return providerConf.schedule(executionDates[executionDates.length-1]);
    }

    while (nextDate() <= Date.now()) {
      executionDates.push(nextDate());
    }

    function getExpectedFilename(date) {
      return providerConf.executionDateToFilename(date);
    }
    // return an array containing one entry for each execution date
    return executionDates.map(function(exDate) {
      var airflowData = airflowResults.rows.filter(function(resultRow) {
        var date = new Date(resultRow[0]);

        // offset the execution date
        if (!providerConf.hasOwnProperty('noAirflowOffset') || !providerConf.noAirflowOffset) {
          date = providerConf.schedule(date);
        }

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
      var incomingFiles = providerIncoming.files.filter(function(file) {
        return providerConf.filenameToExecutionDate(file.key) == helpers.formatDate(exDate);
      });

      return {
        executionDate: helpers.formatDate(exDate),
        incomingFiles: incomingFiles.map(function(f) {
          return f.key;
        }),
        expectedFile: getExpectedFilename(exDate),
        ingested: ingested
      };
    });
  }
}

/*
 * Estimate the general 'health' of a provider
 */
function estimateProviderHealth(providerData, conf) {
  var periodsToConsider;
  if (conf.schedule === providers.schedule.DAILY) {
    periodsToConsider = 60;
  } else if (conf.schedule === providers.schedule.WEEKLY) {
    periodsToConsider = 8;
  } else if (conf.schedule === providers.schedule.BIWEEKLY) {
    periodsToConsider = 4;
  } else if (conf.schedule === providers.schedule.MONTHLY) {
    periodsToConsider = 2;
  }

  periodsToConsider = Math.min(periodsToConsider, providerData.length);

  var negativePeriods = providerData.slice(0, periodsToConsider).reduce(function(acc, el) {
    return (!el.ingested || el.incomingFiles.length === 0) ? (acc + 1) : acc;
  }, 0);

  return (periodsToConsider - negativePeriods)*1.0/periodsToConsider * 100;
}

/**
 * Main entry point for this lambda job
 */
exports.handler = function(event, context) {

  // HTML to be displayed
  var html = fs.readFileSync(path.join(__dirname, './public/index.html'), 'utf-8')
      .replace('{{CSS}}', fs.readFileSync(path.join(__dirname, './public/style.css'), 'utf-8'))
      .replace('{{JAVASCRIPT}}', fs.readFileSync(path.join(__dirname, './public/main.js'), 'utf-8'))
      .replace('{{TITLE}}', event.dev ? 'Provider Status Dashboard - Dev' : 'Provider Status Dashboard')
      .replace('{{HEADER}}', event.dev ? 'Provider Status Dashboard - Dev' : 'Provider Status Dashboard');

  // assemble all asynchronous calls
  var calls = s3.getS3Calls();
  calls.push(airflow.getAirflowCall());

  // execute calls in parallel
  async.parallel(calls, function(err, result) {
    if (err) context.fail(err);

    // all calls were successful
    else {

      // pop off airflow query result
      var airflowRes = result.pop();

      // assemble a 'content' object for each provider - each provider
      // will get an html snippet for the 'last ingested date' table,
      // as well as one for the date series table.
      var content = result.map(function (providerS3Res) {

        // conf for this provider
        var providerConf = providers.config.filter(function(provider) {
          return provider.id == providerS3Res.providerId;
        })[0];

        // join relevant incoming file list to the airflow data for
        // this provider
        var allData = buildFullDataset(airflowRes, providerS3Res).sort(function(row1, row2) {
          return -row1.executionDate.localeCompare(row2.executionDate);
        });

        // filter out days with no incoming files
        var existingFiles = allData.filter(function(d) {
          return d.incomingFiles.length > 0;
        });

        var healthLabel;
        if (!providerConf.airflowPipelineName)
        {
          healthLabel = [3, 'Not Automated'];
        }
        else
        {
          var providerHealthPercentage = estimateProviderHealth(allData, providerConf);
          if (providerHealthPercentage >= 75) healthLabel = [0, 'Healthy'];
          else if (providerHealthPercentage >= 25 && providerHealthPercentage < 75) healthLabel = [1, 'Moderately Healthy'];
          else healthLabel = [2, 'Unhealthy'];
        }

        var ingestedFiles = existingFiles.filter(function(file) {
          return file.ingested;
        });
        var latestIngestionDate = ingestedFiles.length ? ingestedFiles[0].executionDate : "Never";
        if (!providerConf.airflowPipelineName)
        {
          latestIngestionDate = "N/A";
        }

        function dateSortNum(d) {
          if (d === "Never") {
            return 0;
          } else {
            return parseInt(d.split("-").join(""));
          }
        }

        return {
          // date ingested HTML for this provider
          dateIngestedContent: '<tr id="' + providerConf.id + '">' +
            '<td><a href="#">' + providerConf.displayName + '</a></td>' +
            '<td data-sortnumber=' + dateSortNum(
              ((0 in existingFiles) ? existingFiles[0].executionDate : '0000-01-01')
            ) + '>' + ((0 in existingFiles) ? existingFiles[0].executionDate : 'No files') + '</td>' +
            '<td data-sortnumber=' + dateSortNum(latestIngestionDate) + '>' + latestIngestionDate + '</td>' +
            '<td data-sortnumber=' + healthLabel[0] + '>' + healthLabel[1] + '</td>' +
            '</tr>',

          // time series HTML for this provider
          timeSeriesContent: '<ul id="' + providerConf.id + '-timeseries">' +

            // each execution date will get an <li> element
            allData.map(function(d) {

              // calculate class based on the status for this
              // execution date
              var dateClass;
              if (!providerConf.airflowPipelineName) dateClass = 'not-automated';
              else if (d.incomingFiles.length === 0) dateClass = 'not-sent';
              else if (!d.ingested) dateClass = 'not-ingested';
              else dateClass = 'fully-loaded';
              if (dateClass === 'not-sent') {
                return '<li class=' + dateClass + '>' +
                  '<b>' + d.executionDate + '</b><br/>' +
                  d.expectedFile + '</li>';
              }
              else {
                return '<li class=' + dateClass + '>' +
                  '<b>' + d.executionDate + '</b><br/>' +
                  d.incomingFiles.join('<br/>') + '</li>';
              }
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
      var upload_url = event.dev ? 's3://hvstatus.healthverity.com/test/provider-status-dash/index.html' : 's3://hvstatus.healthverity.com/provider-status-dash/index.html'
      s3.uploadFile(output, upload_url, function(err, data) {
        if(err) context.fail(err);
        else context.succeed();
      });

      // // output file for testing
      // fs.writeFile(path.join(__dirname, 'test.html'), output, 'utf-8', function(err, data) {
      //   if(err) context.fail(err);
      //   else context.succeed();
      // });
    }
  });
};
