// global configuration object
exports.config = [
  {
    displayName: 'Practice Insight',
    incomingBucket: 'practiceinsight',
    airflowPipelineName: 'practice_insight_pipeline',
    expectedFilenameRegex: /^.*HV\.data\.837\.[0-9]{4}\.[a-z]{3}\.csv\.gz$/,
    filenameToDate: function(filename) {
      var months = [
        'jan', 'feb', 'mar', 'apr', 'may', 'jun',
        'jul', 'aug', 'sep', 'oct', 'nov', 'dec'
      ];
      var monthNum = (months.indexOf(filename.split('.')[4]) + 1).toString();
      var padded = '00'.substring(0, 2 - monthNum.length) + monthNum;
      return filename.split('.')[3] + '-' + padded + '-02';
    }
  },
  {
    displayName: 'Caris',
    incomingBucket: 'caris',
    airflowPipelineName: 'caris_pipeline',
    expectedFilenameRegex: /^.*DATA_[0-9]{14}$/,
    filenameToDate: function(filename) {
      var isolatedDate = filename.split('_')[1];
      return isolatedDate.substring(0, 4) + '-'
        + isolatedDate.substring(4, 6) + '-'
        + isolatedDate.substring(6, 8);
    }
  }
];

