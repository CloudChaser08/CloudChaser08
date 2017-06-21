var psql = require('pg');

var client = new psql.Client({
  user: 'airflowreader',
  host: 'airflow-dev.awsdev.healthverity.com',
  port: '5432',
  database: 'airflow'
});

exports.getAirflowCall = function() {
  return function(callback) {
    // connect to our database
    client.connect(function (err) {
      if (err) throw err;

      console.log('Connected!');

      // execute a query on our database
      client.query('SELECT dag_id as dag from task_instance', [], function (err, result) {
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






