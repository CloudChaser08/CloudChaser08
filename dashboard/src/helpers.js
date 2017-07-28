/*
 * Miscellaneous helper functions
 */

/**
 * Left-pad zeros up to <length>
 */
exports.leftZPad = function(string, length) {
  length = (typeof length !== 'undefined') ? length : 2;
  var baseString = new Array(length + 1).join( '0' );
  return baseString.substring(0, length - string.length) + string;
};

/**
 * Date manipulation functions
 *
 * These functions avoid mutating incoming date objects to avoid
 * confusing errors later on
 */
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

/**
 * Convert a Date() object to a formatted text string 'YYYY-mm-dd'
 */
exports.formatDate = function(date) {
  return (1900 + date.getYear()) + '-'
    + this.leftZPad((date.getMonth() + 1).toString()) + '-'
    + this.leftZPad(date.getDate().toString());
};
