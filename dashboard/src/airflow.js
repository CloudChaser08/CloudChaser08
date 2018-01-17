/*
 * Functions for retreiving data from the Airflow database
 */

var psql = require('pg');
var fs = require('fs');
var path = require('path');

var providers = require('./providers.js');

var client = new psql.Client({
  user: 'airflowreader',
  host: 'airflow-prod.aws.healthverity.com',
  port: '5432',
  database: 'airflow'
});

// using the query template found in provider-ingestion.sql, insert a
// statement for each provider
const query = {
  text: fs.readFileSync(path.join(__dirname, './provider-ingestion.sql'), 'utf-8').replace(
    '{{PROVIDERS}}', providers.config.filter(function(provider) {
      return provider.airflowPipelineName;
    }).map(function (provider) {
      return 'MAX(CASE ' +
        'WHEN true_dag_id = \'' + provider.airflowPipelineName + '\' ' + 
        'AND total_success_count > 0 ' +
        'AND total_nonsuccess_count = 0 ' +
        'THEN 1 ELSE 0 END' +
        ') as "' + provider.id + '"';
    }).reduce(function(k1, k2) {
      return k1 + ', ' + k2;
    })),
  rowMode: 'array'
};

/**
 * Get a callable asynchronous function that will return the results
 * from running the query above against the airflow database
 */
exports.getAirflowCall = function() {
  return function(callback) {
    // connect to the DB
    client.connect(function (err) {
      if (err) callback(err);

      // execute the query
      client.query(query, [], function (err, result) {
        if (err) callback(err);
        else callback(null, result);

        // disconnect the client
        client.end(function (err) {
          if (err) callback(err);
        });
      });
    });
  };
};
