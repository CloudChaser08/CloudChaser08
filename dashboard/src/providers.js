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
//   noAirflowOffset          -> (Optional) A boolean value to indicate whether the automation routine accounted
//                               for the default airflow offset
//   airflowPipelineName      -> The name of this provider's airflow pipeline
//   expectedFilenameRegex    -> A regex describing the structure of this provider's incoming file names
//   filenameToExecutionDate  -> A function to be used to convert an incoming file name to an execution date of the
//                               form YYYY-mm-dd
exports.config = [
  {
    id: 'practice_insight_dx',
    displayName: 'Practice Insight DX',
    providerPrefix: 'practiceinsight',
    schedule: this.schedule.MONTHLY,
    startDate: new Date('2017-01-16'),
    airflowPipelineName: 'practice_insight_pipeline',
    expectedFilenameRegex: /^.*HV\.data\.837\.[0-9]{4}\.[a-z]{3}\.csv\.gz$/,
    filenameToExecutionDate: function(filename) {
      var months = [
        'jan', 'feb', 'mar', 'apr', 'may', 'jun',
        'jul', 'aug', 'sep', 'oct', 'nov', 'dec'
      ];
      var monthNum = (months.indexOf(filename.split('.')[4]) + 1).toString();
      var date = filename.split('.')[3] + '-' + helpers.leftZPad(monthNum) + '-16';
      var adjusted = helpers.addMonths(1)(new Date(date));
      return helpers.formatDate(adjusted);
    },
    executionDateToFilename: function(date) {
      var adjusted = helpers.addMonths(-1)(date);
      var months = [
        'jan', 'feb', 'mar', 'apr', 'may', 'jun',
        'jul', 'aug', 'sep', 'oct', 'nov', 'dec'
      ];
      var month = months[adjusted.getUTCMonth()];
      return 'incoming/practiceinsight/HV.data.837.' + adjusted.getUTCFullYear() + '.' + month + '.csv.gz';
    }
  },
  {
    id: 'caris_labtests',
    displayName: 'Caris Lab',
    providerPrefix: 'caris',
    schedule: this.schedule.MONTHLY,
    startDate: new Date('2017-01-02'),
    airflowPipelineName: 'caris_pipeline',
    expectedFilenameRegex: /^.*DATA_[0-9]{14}$/,
    filenameToExecutionDate: function(filename) {
      var isolatedDate = filename.split('_')[1];
      return helpers.formatDate(new Date(isolatedDate.substring(0, 4) + '-' + isolatedDate.substring(4, 6) + '-02'));
    },
    executionDateToFilename: function(date) {
      return 'incoming/caris/DATA_' + date.getUTCFullYear() + helpers.leftZPad(date.getUTCMonth() + 1, 2) + '[0-9]{8}';
    }
  },
  {
    id: 'emdeon_dx',
    displayName: 'Emdeon DX',
    providerPrefix: 'medicalclaims/emdeon/transactions',
    schedule: this.schedule.DAILY,
    startDate: new Date('2017-01-01'),
    airflowPipelineName: 'emdeon_dx_pipeline',
    expectedFilenameRegex: /^.*[0-9]{8}_Claims_US_CF_D_deid\.dat\.gz$/,
    filenameToExecutionDate: function(filename) {
      var isolatedDate = filename.split('/')[4].split('_')[0];
      var adjusted = helpers.addDays(2)(new Date(
        isolatedDate.substring(0, 4) + '-' + isolatedDate.substring(4, 6) + '-' + isolatedDate.substring(6, 8)
      ));
      return helpers.formatDate(adjusted);
    },
    executionDateToFilename: function(date) {
      var adjusted = helpers.addDays(-2)(date);
      return 'incoming/medicalclaims/emdeon/transactions/' + date.getUTCFullYear() +
        helpers.leftZPad(adjusted.getUTCMonth() + 1, 2) + helpers.leftZPad(adjusted.getUTCDate(), 2) + '_Claims_US_CF_D_deid.dat.gz';
    }
  },
  {
    id: 'emdeon_rx',
    displayName: 'Emdeon RX',
    providerPrefix: 'pharmacyclaims/emdeon/transactions',
    schedule: this.schedule.DAILY,
    startDate: new Date('2017-01-01'),
    airflowPipelineName: 'emdeon_rx_post_matching_pipeline',
    expectedFilenameRegex: /^.*[0-9]{8}_RX_DEID_CF_ON\.dat\.gz/,
    filenameToExecutionDate: function(filename) {
      var isolatedDate = filename.split('/')[4].split('_')[0];
      var adjusted = helpers.addDays(2)(new Date(
        isolatedDate.substring(0, 4) + '-' + isolatedDate.substring(4, 6) + '-' + isolatedDate.substring(6, 8)
      ));
      return helpers.formatDate(adjusted);
    },
    executionDateToFilename: function(date) {
      var adjusted = helpers.addDays(-2)(date);
      return 'incoming/pharmacyclaims/emdeon/transactions/' + date.getUTCFullYear() +
        helpers.leftZPad(adjusted.getUTCMonth() + 1, 2) + helpers.leftZPad(adjusted.getUTCDate(), 2) + '_RX_DEID_CF_ON.dat.gz';
    }
  },
  {
    id: 'emdeon_era',
    displayName: 'Emdeon ERA',
    providerPrefix: 'era/emdeon/transactions',
    schedule: this.schedule.DAILY,
    startDate: new Date('2017-01-01'),
    airflowPipelineName: 'emdeon_era_pipeline',
    expectedFilenameRegex: /^.*[0-9]{8}_AF_ERA_CF_ON_CS_deid\.dat\.gz/,
    filenameToExecutionDate: function(filename) {
      var isolatedDate = filename.split('/')[4].split('_')[0];
      var adjusted = helpers.addDays(2)(new Date(
        isolatedDate.substring(0, 4) + '-' + isolatedDate.substring(4, 6) + '-' + isolatedDate.substring(6, 8)
      ));
      return helpers.formatDate(adjusted);
    },
    executionDateToFilename: function(date) {
      var adjusted = helpers.addDays(-2)(date);
      return 'incoming/era/emdeon/transactions/' + adjusted.getUTCFullYear() +
        helpers.leftZPad(adjusted.getUTCMonth() + 1, 2) + helpers.leftZPad(adjusted.getUTCDate(), 2) + '_AF_ERA_CF_ON_CS_deid.dat.gz';
    }
  },
  {
    id: 'quest_labtests',
    displayName: 'Quest Lab',
    providerPrefix: 'quest',
    startDate: new Date('2017-01-01'),
    schedule: this.schedule.DAILY,
    airflowPipelineName: 'quest_pipeline',
    expectedFilenameRegex: /^.*HealthVerity_[0-9]{12}_2\.gz.zip$/,
    filenameToExecutionDate: function(filename) {
      var isolatedDate = filename.split('_')[1].substring(0, 8);
      var adjusted = helpers.addDays(4)(new Date(
        isolatedDate.substring(0, 4) + '-' + isolatedDate.substring(4, 6) + '-' + isolatedDate.substring(6, 8)
      ));
      return helpers.formatDate(adjusted);
    },
    executionDateToFilename: function(date) {
      var adjusted = helpers.addDays(-4)(date);
      var nextDay = helpers.addDays(1)(adjusted);
      return 'incoming/quest/HealthVerity_' + adjusted.getUTCFullYear() + helpers.leftZPad(adjusted.getUTCMonth() + 1, 2) +
        helpers.leftZPad(adjusted.getUTCDate(), 2) + helpers.leftZPad(nextDay.getUTCMonth() + 1, 2) + helpers.leftZPad(nextDay.getUTCDate(), 2) + '_2.gz.zip';
    }
  },
  {
    id: 'ability_dx',
    displayName: 'Ability DX',
    providerPrefix: 'ability',
    startDate: new Date('2017-01-01'),
    schedule: this.schedule.DAILY,
    airflowPipelineName: 'ability_pipeline',
    expectedFilenameRegex: /^.*[a-z]+\.from_[0-9]{4}-[0-9]{2}-[0-9]{2}\.to_[0-9]{4}-[0-9]{2}-[0-9]{2}\.zip$/,
    filenameToExecutionDate: function(filename) {
      var isolatedDate = filename.split('_')[2].split('.')[0];
      var adjusted = helpers.addDays(1)(new Date(isolatedDate));
      return helpers.formatDate(adjusted);
    },
    executionDateToFilename: function(date) {
      var adjusted = helpers.addDays(-1)(date);
      var previousDay = helpers.addDays(-1)(adjusted);
      return 'incoming/ability/[app].from_' + previousDay.getUTCFullYear() + '-' + helpers.leftZPad(previousDay.getUTCMonth() + 1, 2) + '-'
        + previousDay.getUTCDate() + '.to_' + adjusted.getUTCFullYear() + '-' + helpers.leftZPad(adjusted.getUTCMonth() + 1, 2)
        + '-' + helpers.leftZPad(adjusted.getUTCDate(), 2) + '.zip';
    }
  },
  {
    id: 'esi_rx',
    displayName: 'Express Scripts RX',
    providerPrefix: 'esi',
    schedule: this.schedule.WEEKLY,
    startDate: new Date('2017-01-01'),
    airflowPipelineName: 'express_scripts_pipeline',
    expectedFilenameRegex: /^.*10130X001_HV_RX_Claims_D[0-9]{8}.txt$/,
    filenameToExecutionDate: function(filename) {
      var isolatedDate = filename.split('_')[4].split('.')[0].substring(1);
      var adjusted = helpers.addDays(1)(new Date(
        isolatedDate.substring(0, 4) + '-' + isolatedDate.substring(4, 6) + '-' + isolatedDate.substring(6, 8)
      ));
      return helpers.formatDate(adjusted);
    },
    executionDateToFilename: function(date) {
      var adjusted = helpers.addDays(-1)(date);
      return 'incoming/esi/10130X001_HV_RX_Claims_D' + adjusted.getUTCFullYear() +
        helpers.leftZPad(adjusted.getUTCMonth() + 1, 2) + helpers.leftZPad(adjusted.getUTCDate(), 2) + '.txt';
    }
  },
  {
    id: 'esi_enrollment',
    displayName: 'Express Scripts Enrollment',
    providerPrefix: 'esi',
    schedule: this.schedule.WEEKLY,
    startDate: new Date('2017-01-01'),
    airflowPipelineName: 'express_scripts_pipeline',
    expectedFilenameRegex: /^.*10130X001_HV_RX_ENROLLMENT_D[0-9]{8}.txt$/,
    filenameToExecutionDate: function(filename) {
      var isolatedDate = filename.split('_')[4].split('.')[0].substring(1);
      var adjusted = helpers.addDays(1)(new Date(
        isolatedDate.substring(0, 4) + '-' + isolatedDate.substring(4, 6) + '-' + isolatedDate.substring(6, 8)
      ));
      return helpers.formatDate(adjusted);
    },
    executionDateToFilename: function(date) {
      var adjusted = helpers.addDays(-1)(date);
      return 'incoming/esi/10130X001_HV_RX_ENROLLMENT_D' + adjusted.getUTCFullYear() +
        helpers.leftZPad(adjusted.getUTCMonth() + 1, 2) + helpers.leftZPad(adjusted.getUTCDate(), 2) + '.txt';
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
      var adjusted = helpers.addDays(1)(new Date(
        isolatedDate.substring(0, 4) + '-' + isolatedDate.substring(4, 6) + '-' + isolatedDate.substring(6, 8)
      ));
      return helpers.formatDate(adjusted);
    },
    executionDateToFilename: function(date) {
      var adjusted = helpers.addDays(-1)(date);
      return 'incoming/mckessonrx/HVUnRes.Record.' + adjusted.getUTCFullYear() + helpers.leftZPad(adjusted.getUTCMonth() + 1, 2)
        + helpers.leftZPad(adjusted.getUTCDate(), 2);
    }
  },
  {
    id: 'mckesson_res_rx',
    displayName: 'McKesson RX Restricted',
    providerPrefix: 'mckessonrx',
    schedule: this.schedule.DAILY,
    startDate: new Date('2017-06-01'),
    airflowPipelineName: 'mckessonrx_res_pipeline',
    expectedFilenameRegex: /^.*HVRes.Record.[0-9]{8}$/,
    filenameToExecutionDate: function(filename) {
      var isolatedDate = filename.split('.')[2];
      var adjusted = helpers.addDays(1)(new Date(
        isolatedDate.substring(0, 4) + '-' + isolatedDate.substring(4, 6) + '-' + isolatedDate.substring(6, 8)
      ));
      return helpers.formatDate(adjusted);
    },
    executionDateToFilename: function(date) {
      var adjusted = helpers.addDays(-1)(date);
      return 'incoming/mckessonrx/HVRes.Record.' + adjusted.getUTCFullYear() + helpers.leftZPad(adjusted.getUTCMonth() + 1, 2)
        + helpers.leftZPad(adjusted.getUTCDate(), 2);
    }
  },
  {
    id: 'diplomat_rx',
    displayName: 'Diplomat RX',
    providerPrefix: 'diplomat',
    schedule: this.schedule.WEEKLY,
    startDate: new Date('2017-06-06'),
    airflowPipelineName: 'diplomat_rx_pipeline',
    expectedFilenameRegex: /^.*HealthVerityOut_[0-9]{8}.csv$/,
    filenameToExecutionDate: function(filename) {
      var isolatedDate = filename.split('_')[1].split('.')[0];
      var adjusted = helpers.addDays(1)(new Date(
        isolatedDate.substring(0, 4) + '-' + isolatedDate.substring(4, 6) + '-' + isolatedDate.substring(6, 8)
      ));
      return helpers.formatDate(adjusted);
    },
    executionDateToFilename: function(date) {
      var adjusted = helpers.addDays(-1)(date);
      return 'incoming/diplomat/HealthVerityOut_' + adjusted.getUTCFullYear() + helpers.leftZPad(adjusted.getUTCMonth() + 1, 2)
        + helpers.leftZPad(adjusted.getUTCDate(), 2)
        + '.csv';
    }
  },
  {
    id: 'apothecary_by_design',
    displayName: 'Apothecary By Design RX',
    providerPrefix: 'abd',
    schedule: this.schedule.WEEKLY,
    startDate: new Date('2017-06-05'),
    airflowPipelineName: 'apothecary_by_design_pipeline',
    expectedFilenameRegex: /^.*hv_export_data_[0-9]{8}.txt$/,
    filenameToExecutionDate: function(filename) {
      var isolatedDate = filename.split('_')[3].split('.')[0];
      return isolatedDate.substring(0, 4) + '-' + isolatedDate.substring(4, 6) + '-' + isolatedDate.substring(6, 8);
    },
    executionDateToFilename: function(date) {
      return 'incoming/abd/hv_export_data_' + date.getUTCFullYear() + helpers.leftZPad(date.getUTCMonth() + 1, 2)
        + helpers.leftZPad(date.getUTCDate(), 2)
        + '.txt';
    }
  },
  {
    id: 'cardinal_mpi',
    displayName: 'Cardinal MPI',
    providerPrefix: 'cardinal/mpi',
    schedule: this.schedule.DAILY,
    startDate: new Date('2017-06-01'),
    airflowPipelineName: 'cardinal_mpi_pipeline',
    expectedFilenameRegex: /^.*mpi.[0-9]{8}T[0-9]{6}.zip$/,
    filenameToExecutionDate: function(filename) {
      var isolatedDate = filename.split(/[._]/)[1].split('T')[0];
      return isolatedDate.substring(0, 4) + '-' + isolatedDate.substring(4, 6) + '-' + isolatedDate.substring(6, 8);
    },
    executionDateToFilename: function(date) {
      return 'incoming/cardinal/mpi/mpi_' + date.getUTCFullYear() + helpers.leftZPad(date.getUTCMonth() + 1, 2)
        + helpers.leftZPad(date.getUTCDate(), 2)
        + 'T[0-9]{6}.zip';
    }
  },
  {
    id: 'cardinal_pds',
    displayName: 'Cardinal PDS RX',
    providerPrefix: 'cardinal/pds',
    schedule: this.schedule.DAILY,
    startDate: new Date('2017-06-01'),
    airflowPipelineName: 'cardinal_pds_pipeline',
    expectedFilenameRegex: /^.*PDS_record_data_[0-9]{14}$/,
    filenameToExecutionDate: function(filename) {
      var isolatedDate = filename.split('_')[3].substring(0, 8);
      return isolatedDate.substring(0, 4) + '-' + isolatedDate.substring(4, 6) + '-' + isolatedDate.substring(6, 8);
    },
    executionDateToFilename: function(date) {
      return 'incoming/cardinal/pds/PDS_record_data_' + date.getUTCFullYear() + helpers.leftZPad(date.getUTCMonth() + 1, 2)
        + helpers.leftZPad(date.getUTCDate(), 2)
        + '[0-9]{6}';
    }
  },
  {
    id: 'navicure',
    displayName: 'Navicure DX',
    providerPrefix: 'navicure',
    schedule: this.schedule.DAILY,
    startDate: new Date('2017-06-01'),
    airflowPipelineName: 'navicure_pipeline',
    expectedFilenameRegex: /^.*HealthVerity-[0-9]{4}-[0-9]{2}-[0-9]{2}-record-data-Navicure$/,
    filenameToExecutionDate: function(filename) {
      var isolatedDate = filename.split('-').slice(1, 4).join('-');
      var adjusted = helpers.addDays(31)(new Date(isolatedDate));
      return helpers.formatDate(adjusted);
    },
    executionDateToFilename: function(date) {
      var adjusted = helpers.addDays(-31)(date);
      return 'incoming/navicure/HealthVerity-' + adjusted.getUTCFullYear() + '-' + helpers.leftZPad(adjusted.getUTCMonth() + 1, 2)
        + '-' + helpers.leftZPad(adjusted.getUTCDate(), 2) + '-record-data-Navicure';
    }
  },
  {
    id: 'neogenomics',
    displayName: 'Neogenomics Lab',
    providerPrefix: 'neogenomics',
    schedule: this.schedule.WEEKLY,
    startDate: new Date('2017-10-02'),
    airflowPipelineName: 'neogenomics_pipeline',
    expectedFilenameRegex: /^.*NeoG_HV_STD_W_[0-9]{8}_[0-9]{8}_NPHI.txt$/,
    filenameToExecutionDate: function(filename) {
      var isolatedDate = filename.split('_')[5];
      return isolatedDate.substring(0, 4) + '-' + isolatedDate.substring(4, 6) + '-' + isolatedDate.substring(6, 8);
    },
    executionDateToFilename: function(date) {
      var yesterday = helpers.addDays(-1)(date);
      return 'incoming/neogenomics/NeoG_HV_STD_W_' + yesterday.getUTCFullYear() + helpers.leftZPad(yesterday.getUTCMonth() + 1, 2)
        + helpers.leftZPad(yesterday.getUTCDate(), 2) + '_' + date.getUTCFullYear() + helpers.leftZPad(date.getUTCMonth() + 1, 2)
        + helpers.leftZPad(date.getUTCDate(), 2) + '_NPHI.txt';
    }
  },
  {
    id: 'nextgen',
    displayName: 'Nextgen EMR',
    providerPrefix: 'ng-lssa/20',
    schedule: this.schedule.MONTHLY,
    startDate: new Date('2017-07-20'),
    airflowPipelineName: 'nextgen_pipeline',
    expectedFilenameRegex: /^.*ng-lssa\/[0-9]{6}\/deltas.*\/Manifest.txt$/,
    filenameToExecutionDate: function(filename) {
      var isolatedDate = filename.split('/')[2];
      return isolatedDate.substring(0, 4) + '-' + isolatedDate.substring(4, 6) + '-20';
    },
    executionDateToFilename: function(date) {
      return 'incoming/ng-lssa/' + date.getFullYear() + helpers.leftZPad(date.getMonth() + 1, 2) + '/deltas/Manifest.txt';
    },
    listRecursively: true
  },
  {
    id: 'genoa',
    displayName: 'Genoa RX',
    providerPrefix: 'genoa',
    schedule: this.schedule.MONTHLY,
    startDate: new Date('2017-12-03'),
    airflowPipelineName: 'genoa_pipeline',
    expectedFilenameRegex: /^.*Genoa_HealthVerity_[0-9]{8}_[0-9]{6}.zip$/,
    filenameToExecutionDate: function(filename) {
      var isolatedDate = filename.split('_')[2];
      var adjusted = helpers.addDays(2)(new Date(
        isolatedDate.substring(0, 4) + '-' + isolatedDate.substring(4, 6) + '-' + isolatedDate.substring(6, 8)
      ));
      return helpers.formatDate(adjusted);
    },
    executionDateToFilename: function(date) {
      return 'incoming/genoa/Genoa_HealthVerity_' + date.getUTCFullYear() + helpers.leftZPad(date.getUTCMonth() + 1, 2)
        + '01'
        + '_[0-9]{6}.zip';
    },
  },
  {
    id: 'ambry',
    displayName: 'Ambry RX',
    providerPrefix: 'ambry'
  },
  {
    id: 'allscripts',
    displayName: 'Allscripts DX',
    providerPrefix: 'allscripts',
    schedule: this.schedule.DAILY,
    startDate: new Date('2018-06-22'),
    airflowPipelineName: 'allscripts_dx_pipeline',
    expectedFilenameRegex: /^.*HV_CLAIMS_[0-9]{8}_[0-9]{1}.out.zip$/,
    filenameToExecutionDate: function(filename) {
      var isolatedDate = filename.split('_')[2];
      var adjusted = helpers.addDays(2)(new Date(
        isolatedDate.substring(0, 4) + '-' + isolatedDate.substring(4, 6) + '-' + isolatedDate.substring(6, 8)
      ));
      return helpers.formatDate(adjusted);
    },
    executionDateToFilename: function(date) {
      var file_date = helpers.addDays(-2)(date);
      return 'incoming/allscripts/HV_CLAIMS_' + file_date.getUTCFullYear() + helpers.leftZPad(file_date.getUTCMonth() + 1, 2)
        + helpers.leftZPad(file_date.getUTCDate(), 2) + '_1.out.zip';
    },
  },
  {
    id: 'corrona',
    displayName: 'Corrona Registry',
    providerPrefix: 'corrona'
  },
  {
    id: 'cardinal_rcm',
    displayName: 'Cardinal RCM DX',
    providerPrefix: 'cardinal/rcm'
  },
  {
    id: 'courtagen',
    displayName: 'Courtagen Lab',
    providerPrefix: 'courtagen'
  },
  {
    id: 'obit',
    displayName: 'Obituary',
    providerPrefix: 'obituarydata'
  },
  {
    id: 'mindbody',
    displayName: 'MindBody',
    providerPrefix: 'mindbody'
  },
  {
    id: 'epsilon',
    displayName: 'Epsilon',
    providerPrefix: 'epsilon'
  },
  {
    id: 'acxiom',
    displayName: 'Acxiom',
    providerPrefix: 'acxiom'
  },
  {
    id: 'cardinal_vitalpath',
    displayName: 'Cardinal Vitalpath RX',
    providerPrefix: 'cardinal/vitalpath'
  },
  {
    id: 'allscripts_emr',
    displayName: 'Allscripts EMR',
    providerPrefix: 'allscripts',
    schedule: this.schedule.MONTHLY,
    startDate: new Date('2017-12-22'),
    airflowPipelineName: 'allscripts_emr_pipeline',
    expectedFilenameRegex: /^.*Allscripts_[A-Z][a-z]{2}[0-9]{2}_[0-9]{7}_01.zip$/,
    filenameToExecutionDate: function(filename) {
      var isolatedDate = filename.split('_')[1];
      var adjusted = helpers.addMonths(1)(helpers.addDays(21)(new Date(
        Date.parse('01 ' + isolatedDate.substring(0,3) + ' ' + isolatedDate.substring(3, 5))
      )));
      return helpers.formatDate(adjusted);
    },
    executionDateToFilename: function(date) {
      date_parts = helpers.addMonths(-1)(date).toUTCString().split(' ');
      return 'incoming/allscripts/Allscripts_' + date_parts[2] + date.getUTCFullYear().toString().substring(2,4)
        + '_[0-9]{7}_01.zip'
    },
  },
  {
    id: 'visonex',
    displayName: 'Visonex EMR',
    providerPrefix: 'visonex'
  },
  {
    id: 'amazing_charts',
    displayName: 'Amazing Charts EMR',
    providerPrefix: 'amazingcharts'
  },
  {
    id: 'cardinal_tsi',
    displayName: 'Cardinal TSI EMR',
    providerPrefix: 'cardinal/tsi'
  },
  {
    id: 'cardinal_raintree',
    displayName: 'Cardinal Raintree EMR',
    providerPrefix: 'cardinal/emr'
  },
  {
    id: 'treato',
    displayName: 'Treato EMR',
    providerPrefix: 'treato'
  },
  {
    id: 'healthjump',
    displayName: 'HealthJump EMR',
    providerPrefix: 'healthjump'
  },
  {
    id: 'transmed',
    displayName: 'Transmed EMR',
    providerPrefix: 'transmed'
  },
  {
    id: 'pdx',
    displayName: 'PDX RX',
    providerPrefix: 'pdx',
    schedule: this.schedule.WEEKLY,
    startDate: new Date('2018-04-02'),
    airflowPipelineName: 'pdx_pipeline',
    expectedFilenameRegex: /^.*hvfeedfile_po_record_deid_[0-9]{8}[0-9]{6}.hvout$/,
    filenameToExecutionDate: function(filename) {
      var isolatedDate = filename.split('_')[4];
      var adjusted = helpers.addDays(0)(new Date(
        isolatedDate.substring(0, 4) + '-' + isolatedDate.substring(4, 6) + '-' + isolatedDate.substring(6, 8)
      ));
      return helpers.formatDate(adjusted);
    },
    executionDateToFilename: function(date) {
      return 'incoming/pdx/hvfeedfile_po_record_deid_' + date.getUTCFullYear() + helpers.leftZPad(date.getUTCMonth() + 1, 2)
        + helpers.leftZPad(date.getUTCDate(), 2) + '[0-9]{6}.hvout';
    },
  },
  {
    id: 'xifin',
    displayName: 'Xifin',
    providerPrefix: 'xifin',
    schedule: this.schedule.WEEKLY,
    startDate: new Date('2018-07-28'),
    airflowPipelineName: 'xifin_dx_pipeline',
    expectedFilenameRegex: /^.*accn_(billed_procedures|demographics|diagnosis|tests|payors)_[0-9]{8}_[0-9]{1,}.out$/,
    filenameToExecutionDate: function(filename) {
      var isolatedDate = filename.split('_')[2];
      var adjusted = helpers.addDays(0)(new Date(
        isolatedDate.substring(0, 4) + '-' + isolatedDate.substring(4, 6) + '-' + isolatedDate.substring(6, 8)
      ));
      return helpers.formatDate(adjusted);
    },
    executionDateToFilename: function(date) {
      return 'incoming/xifin/accn_(billed_procedures|demographics|diagnosis|tests|payors)_' 
        + date.getUTCFullYear() + helpers.leftZPad(date.getUTCMonth() + 1, 2)
        + helpers.leftZPad(date.getUTCDate(), 2) + '[0-9]{1,}.out';
    },
  },
];
