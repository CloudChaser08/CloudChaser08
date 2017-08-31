var helpers = require('./helpers.js');

// for each airflow schedule, this object provides a function that can
// be used to increment a date by the correct time period
exports.schedule = {
  DAILY: helpers.addDays(1),
  WEEKLY: helpers.addDays(7),
  BIWEEKLY: helpers.addDays(14),
  MONTHLY: helpers.addMonths(1)
};

// Each provider gets a configuration object that describes all of the
// nuances of this provider as they relate to both s3 and airflow.
//
// Adding new providers to this dashboard requires only that you add a
// new configuration for the new provider to this config list - the
// dashboard is built by iterating over this array.
//
// The configuration for a new provider should include the following:
//   id                       -> A unique identifier for this provider
//   displayName              -> Provider name as it will be displayed on the dashboard
//   providerPrefix           -> The string between healthverity/incoming/ and all of the incoming files for this provider.
//                               May contain slashes.
//                               Ex: s3://healthverity/incoming/<incoming bucket>/incomingFile.gz
//   schedule                 -> The airflow schedule for this provider's DAG (from the schedule object above)
//   startDate                -> The date from which to enumerate all of this provider's execution dates for the purposes
//                               of this dashboard
//   airflowPipelineName      -> The name of this provider's airflow pipeline
//   expectedFilenameRegex    -> A regex describing the structure of this provider's incoming file names
//   filenameToExecutionDate  -> A function to be used to convert an incoming file name to an execution date of the
//                               form YYYY-mm-dd
exports.config = [
  {
    id: 'practice_insight_dx',
    displayName: 'Practice Insight',
    providerPrefix: 'practiceinsight',
    schedule: this.schedule.MONTHLY,
    startDate: new Date('2017-01-02'),
    airflowPipelineName: 'practice_insight_pipeline',
    expectedFilenameRegex: /^.*HV\.data\.837\.[0-9]{4}\.[a-z]{3}\.csv\.gz$/,
    filenameToExecutionDate: function(filename) {
      var months = [
        'jan', 'feb', 'mar', 'apr', 'may', 'jun',
        'jul', 'aug', 'sep', 'oct', 'nov', 'dec'
      ];
      var monthNum = (months.indexOf(filename.split('.')[4]) + 1).toString();
      return filename.split('.')[3] + '-' + helpers.leftZPad(monthNum) + '-02';
    },
    executionDateToFilename: function(date) {
      var months = [
        'jan', 'feb', 'mar', 'apr', 'may', 'jun',
        'jul', 'aug', 'sep', 'oct', 'nov', 'dec'
      ];
      var month = months[date.getMonth()];
      return 'incoming/practiceinsight/HV.data.837.' + date.getFullYear() + '.' + month + '.csv.gz';
    }
  },
  {
    id: 'caris_labtests',
    displayName: 'Caris',
    providerPrefix: 'caris',
    schedule: this.schedule.MONTHLY,
    startDate: new Date('2017-01-02'),
    airflowPipelineName: 'caris_pipeline',
    expectedFilenameRegex: /^.*DATA_[0-9]{14}$/,
    filenameToExecutionDate: function(filename) {
      var isolatedDate = filename.split('_')[1];
      var executionDate = helpers.addMonths(-1)(new Date(isolatedDate.substring(0, 4) + '-' + isolatedDate.substring(4, 6) + '-02'));
      return helpers.formatDate(executionDate);
    },
    executionDateToFilename: function(date) {
      var nextDay = new Date(date.getTime() + (24 * 60 * 60 * 1000));
      return 'incoming/caris/DATA_' + nextDay.getFullYear() + helpers.leftZPad(nextDay.getDate(), 2) + '<########>';
    }
  },
  {
    id: 'emdeon_dx',
    displayName: 'EmdeonDX',
    providerPrefix: 'medicalclaims/emdeon/transactions',
    schedule: this.schedule.DAILY,
    startDate: new Date('2017-01-01'),
    airflowPipelineName: 'emdeon_dx_pipeline',
    expectedFilenameRegex: /^.*[0-9]{8}_Claims_US_CF_D_deid\.dat\.gz$/,
    filenameToExecutionDate: function(filename) {
      var isolatedDate = filename.split('/')[4].split('_')[0];
      return isolatedDate.substring(0, 4) + '-' + isolatedDate.substring(4, 6) + '-' + isolatedDate.substring(6, 8);
    },
    executionDateToFilename: function(date) {
      return 'incoming/medicalclaims/emdeon/transactions/' + date.getFullYear() +
        helpers.leftZPad(date.getMonth() + 1, 2) + helpers.leftZPad(date.getDate(), 2) + '_Claims_US_CF_D_deid.dat.gz';
    }
  },
  {
    id: 'emdeon_rx',
    displayName: 'EmdeonRX',
    providerPrefix: 'pharmacyclaims/emdeon/transactions',
    schedule: this.schedule.DAILY,
    startDate: new Date('2017-01-01'),
    airflowPipelineName: 'emdeon_rx_post_matching_pipeline',
    expectedFilenameRegex: /^.*[0-9]{8}_RX_DEID_CF_ON\.dat\.gz/,
    filenameToExecutionDate: function(filename) {
      var isolatedDate = filename.split('/')[4].split('_')[0];
      return isolatedDate.substring(0, 4) + '-' + isolatedDate.substring(4, 6) + '-' + isolatedDate.substring(6, 8);
    },
    executionDateToFilename: function(date) {
      return 'incoming/pharmacyclaims/emdeon/transactions/' + date.getFullYear() +
        helpers.leftZPad(date.getMonth() + 1, 2) + helpers.leftZPad(date.getDate(), 2) + '_RX_DEID_CF_ON.dat.gz';
    }
  },
  {
    id: 'quest_labtests',
    displayName: 'Quest',
    providerPrefix: 'quest',
    startDate: new Date('2017-01-01'),
    schedule: this.schedule.DAILY,
    airflowPipelineName: 'quest_pipeline',
    expectedFilenameRegex: /^.*HealthVerity_[0-9]{12}_2\.gz.zip$/,
    filenameToExecutionDate: function(filename) {
      var isolatedDate = filename.split('_')[1].substring(0, 8);
      var adjustedDate = helpers.addDays(3)(
        new Date(isolatedDate.substring(0, 4) + '-' + isolatedDate.substring(4, 6) + '-' + isolatedDate.substring(6, 8))
      );
      return helpers.formatDate(adjustedDate);
    },
    executionDateToFilename: function(date) {
      var twoDaysPrior = new Date(date.getTime() - (2 * 24 * 60 * 60 * 1000));
      var nextDay = new Date(twoDaysPrior.getTime() + (24 * 60 * 60 * 1000));
      return 'incoming/quest/HealthVerity_' + twoDaysPrior.getFullYear() + helpers.leftZPad(twoDaysPrior.getMonth() + 1, 2) +
        helpers.leftZPad(twoDaysPrior.getDate(), 2) + helpers.leftZPad(nextDay.getMonth() + 1, 2) + helpers.leftZPad(nextDay.getDate(), 2) + '_2.gz.zip';
    }
  },
  {
    id: 'ability_dx',
    displayName: 'Ability',
    providerPrefix: 'ability',
    startDate: new Date('2017-01-01'),
    schedule: this.schedule.DAILY,
    airflowPipelineName: 'ability_pipeline',
    expectedFilenameRegex: /^.*[a-z]+\.from_[0-9]{4}-[0-9]{2}-[0-9]{2}\.to_[0-9]{4}-[0-9]{2}-[0-9]{2}\.zip$/,
    filenameToExecutionDate: function(filename) {
      return filename.split('_')[2].split('.')[0];
    },
    executionDateToFilename: function(date) {
      var previousDay = new Date(date.getTime() - (24 * 60 * 60 * 1000));
      return 'incoming/ability/[app].from_' + previousDay.getFullYear() + '-' + helpers.leftZPad(previousDay.getMonth() + 1, 2) + '-' +
        previousDay.getDate() + '.to_' + date.getFullYear() + '-' + helpers.leftZPad(date.getMonth() + 1, 2) + '-' + helpers.leftZPad(date.getDate(), 2) + '.zip';
    }
  },
  {
    id: 'esi_rx',
    displayName: 'Express Scripts',
    providerPrefix: 'esi',
    schedule: this.schedule.WEEKLY,
    startDate: new Date('2017-01-01'),
    airflowPipelineName: 'express_scripts_pipeline',
    expectedFilenameRegex: /^.*10130X001_HV_RX_Claims_D[0-9]{8}.txt$/,
    filenameToExecutionDate: function(filename) {
      var isolatedDate = filename.split('_')[4].split('.')[0].substring(1);
      var adjusted = helpers.addDays(-6)(new Date(
        isolatedDate.substring(0, 4) + '-' + isolatedDate.substring(4, 6) + '-' + isolatedDate.substring(6, 8)
      ));
      return helpers.formatDate(adjusted);
    },
    executionDateToFilename: function(date) {
      return 'incoming/esi/10130X001_HV_RX_Claims_D' + date.getFullYear() + 
        helpers.leftZPad(date.getMonth() + 1, 2) + helpers.leftZPad(date.getDate(), 2) + '.txt';
    }
  },
  {
    id: 'mckesson_unres_rx',
    displayName: 'McKesson RX',
    providerPrefix: 'mckessonrx',
    schedule: this.schedule.DAILY,
    startDate: new Date('2017-06-01'),
    airflowPipelineName: 'mckessonrx_pipeline',
    expectedFilenameRegex: /^.*HVUnRes.Record.[0-9]{8}$/,
    filenameToExecutionDate: function(filename) {
      var isolatedDate = filename.split('.')[2];
      return isolatedDate.substring(0, 4) + '-' + isolatedDate.substring(4, 6) + '-' + isolatedDate.substring(6, 8);
    },
    executionDateToFilename: function(date) {
      return 'incoming/mckessonrx/HVUnRes.Record.' + date.getFullYear() + helpers.leftZPad(date.getMonth() + 1, 2) + helpers.leftZPad(date.getDate(), 2);
    }
  },
  {
    id: 'mckesson_res_rx',
    displayName: 'McKesson RX Restricted',
    providerPrefix: 'mckessonrx',
    schedule: this.schedule.DAILY,
    startDate: new Date('2017-06-01'),
    airflowPipelineName: 'mckesson_res_pipeline',
    expectedFilenameRegex: /^.*HVRes.Record.[0-9]{8}$/,
    filenameToExecutionDate: function(filename) {
      var isolatedDate = filename.split('.')[2];
      return isolatedDate.substring(0, 4) + '-' + isolatedDate.substring(4, 6) + '-' + isolatedDate.substring(6, 8);
    },
    executionDateToFilename: function(date) {
      return 'incoming/mckessonrx/HVRes.Record.' + date.getFullYear() + helpers.leftZPad(date.getMonth() + 1, 2) + helpers.leftZPad(date.getDate(), 2);
    }
  }
];

