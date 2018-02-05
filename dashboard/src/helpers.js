/*
 * Miscellaneous helper functions
 */

/**
 * Left-pad zeros up to <length>
 */
exports.leftZPad = function(string, length) {
  string = (typeof string !== 'string') ? string.toString() : string;
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
    copy.setUTCMonth(copy.getUTCMonth() + months);
    return copy;
  };
};
exports.addDays = function(days) {
  return function f(date) {
    var copy = new Date(date.getTime());
    copy.setUTCDate(copy.getUTCDate() + days);
    return copy;
  };
};

/**
 * Convert a Date() object to a formatted text string 'YYYY-mm-dd'
 */
exports.formatDate = function(date) {
  return (date.getUTCFullYear()) + '-'
    + this.leftZPad((date.getUTCMonth() + 1).toString()) + '-'
    + this.leftZPad(date.getUTCDate().toString());
};
