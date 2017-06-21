var helpers = require('./helpers.js');

// global configuration object
exports.config = [
  {
    displayName: 'Practice Insight',
    incomingBucket: 'practiceinsight',
    airflowPipelineName: 'practice_insight_pipeline',
    expectedFilenameRegex: /^.*HV\.data\.837\.[0-9]{4}\.[a-z]{3}\.csv\.gz$/,
    filenameToExecutionDate: function(filename) {
      var months = [
        'jan', 'feb', 'mar', 'apr', 'may', 'jun',
        'jul', 'aug', 'sep', 'oct', 'nov', 'dec'
      ];
      var monthNum = (months.indexOf(filename.split('.')[4]) + 1).toString();
      return filename.split('.')[3] + '-' + helpers.leftZPad(monthNum) + '-02';
    }
  },
  {
    displayName: 'Caris',
    incomingBucket: 'caris',
    airflowPipelineName: 'caris_pipeline',
    expectedFilenameRegex: /^.*DATA_[0-9]{14}$/,
    filenameToExecutionDate: function(filename) {
      var isolatedDate = filename.split('_')[1];
      var executionDate = helpers.subtractMonths(new Date(isolatedDate.substring(0, 4) + '-' + isolatedDate.substring(4, 6) + '-02'), 1);
      return helpers.formatDate(executionDate);
    }
  },
  {
    displayName: 'EmdeonDX',
    incomingBucket: 'medicalclaims/emdeon/transactions',
    airflowPipelineName: 'emdeon_dx_post_matching_pipeline',
    expectedFilenameRegex: /^.*[0-9]{8}_Claims_US_CF_D_deid\.dat\.gz$/,
    filenameToExecutionDate: function(filename) {
      var isolatedDate = filename.split('/')[4].split('_')[0];
      return isolatedDate.substring(0, 4) + '-' + isolatedDate.substring(4, 6) + '-' + isolatedDate.substring(6, 8);
    }
  },
  {
    displayName: 'Quest',
    incomingBucket: 'quest',
    airflowPipelineName: 'quest_pipeline',
    expectedFilenameRegex: /^.*HealthVerity_[0-9]{12}_2\.gz.zip$/,
    filenameToExecutionDate: function(filename) {
      var isolatedDate = filename.split('_')[1].substring(0, 8);
      return isolatedDate.substring(0, 4) + '-' + isolatedDate.substring(4, 6) + '-' + isolatedDate.substring(6, 8);
    }
  },
  {
    displayName: 'Ability',
    incomingBucket: 'ability',
    airflowPipelineName: 'ability_pipeline',
    expectedFilenameRegex: /^.*[a-z]+\.from_[0-9]{4}-[0-9]{2}-[0-9]{2}\.to_[0-9]{4}-[0-9]{2}-[0-9]{2}\.zip$/,
    filenameToExecutionDate: function(filename) {
      return filename.split('_')[2].split('.')[0];
    }
  },
  {
    displayName: 'Express Scripts',
    incomingBucket: 'esi',
    airflowPipelineName: 'express_scripts_pipeline',
    expectedFilenameRegex: /^.*[0-9X]{9}_HV_RX_Claims_c[0-9]{6}.txt$/,
    filenameToExecutionDate: function(filename) {
      var isolatedDate = filename.split('_')[4].split('.')[0].substring(1);
      return '20' + isolatedDate.substring(0, 2) + '-' + isolatedDate.substring(2, 4) + '-' + isolatedDate.substring(4, 6);
    }
  }
];

