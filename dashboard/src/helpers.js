exports.leftZPad = function(string, length) {
  length = (typeof length !== 'undefined') ? length : 2;
  var baseString = new Array(length + 1).join( '0' );
  return baseString.substring(0, length - string.length) + string;
};

exports.subtractMonths = function(date, months) {
  date.setMonth(date.getMonth() - months);
  return date;
};

// YYYY-mm-dd
exports.formatDate = function(date) {
  return (1900 + date.getYear()) + '-'
    + this.leftZPad((date.getMonth() + 1).toString()) + '-'
    + this.leftZPad(date.getDate().toString());
};
