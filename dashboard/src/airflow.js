var psql = require('pg');
var fs = require('fs');
var path = require('path');

var providers = require('./providers.js');

var client = new psql.Client({
  user: 'airflowreader',
  host: 'airflow-dev.awsdev.healthverity.com',
  port: '5432',
  database: 'airflow'
});

const query = {
  text: fs.readFileSync(path.join(__dirname, './provider-ingestion.sql'), 'utf-8').replace(
    '{{PROVIDERS}}', providers.config.map(function (provider) {
      return "MAX(CASE " +
        "WHEN true_dag_id = '" + provider.airflowPipelineName + "' " + 
        "AND total_success_count > 0 " +
        "AND total_nonsuccess_count = 0 " +
        "THEN 1 ELSE 0 END" +
        ") as " + provider.incomingBucket;
    }).reduce(function(k1, k2) {
      return k1 + ', ' + k2;
    })),
  rowMode: 'array'
};

exports.getAirflowCall = function() {
  return function(callback) {
    // connect to our database
    client.connect(function (err) {
      if (err) throw err;

      // execute a query on our database
      client.query(query, [], function (err, result) {
        if (err) callback(err);
        else callback(null, result);

        // disconnect the client
        client.end(function (err) {
          if (err) throw err;
        });
      });
    });
  };
}






