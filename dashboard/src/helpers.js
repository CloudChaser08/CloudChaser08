exports.leftZPad = function(string, length) {
  length = (typeof length !== 'undefined') ? length : 2;
  var baseString = new Array(length + 1).join( '0' );
  return baseString.substring(0, length - string.length) + string;
};

// these functions avoid mutating incoming date objects because an
// incoming date may be a constant on a provider that should not be
// modified
exports.addMonths = function(months) {
  return function f(date) {
    var copy = new Date(date.getTime());
    copy.setMonth(copy.getMonth() + months);
    return copy;
  };
};
exports.addDays = function(days) {
  return function f(date) {
    var copy = new Date(date.getTime());
    copy.setDate(copy.getDate() + days);
    return copy;
  };
};

// YYYY-mm-dd
exports.formatDate = function(date) {
  return (1900 + date.getYear()) + '-'
    + this.leftZPad((date.getMonth() + 1).toString()) + '-'
    + this.leftZPad(date.getDate().toString());
};
