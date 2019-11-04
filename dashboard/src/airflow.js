/*
 * Functions for retreiving data from the Airflow database
 */

var AWS = require('aws-sdk')
var psql = require('pg');
var fs = require('fs');
var path = require('path');

var providers = require('./providers.js');

var ssm = new AWS.SSM();

// using the query template found in provider-ingestion.sql, insert a
// statement for each provider
const query = {
  text: fs.readFileSync(path.join(__dirname, './provider-ingestion.sql'), 'utf-8').replace(
    '{{PROVIDERS}}', providers.config.filter(function(provider) {
      // This filters out any non-automated providers
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

var db = {
  client: null,
  getClient: async function() {
    if (this.client !== null) {
        return this.client;
    }

    var params = {
        Names: ['/prod/rds/airflow/sql_alchemy_conn_string'],
        WithDecryption: true
    }
    try {
      var data = await ssm.getParameters(params).promise()
      var connectionString = data.Parameters[0].Value;
      this.client = new psql.Client({
          connectionString: connectionString
      })
    } catch (e) {
      console.log(e);
    }
    return this.client;
  }
};

/**
 * Get a callable asynchronous function that will return the results
 * from running the query above against the airflow database
 */
exports.getAirflowCall = function() {
  return function(callback) {
    db.getClient().then(
      function(client) {
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
      }, function(error) {
        console.log(error)
      });
    });
  };
};
