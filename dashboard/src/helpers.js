exports.leftZPad = function(string, length) {
  length = (typeof length !== 'undefined') ? length : 2;
  var baseString = new Array(length + 1).join( '0' );
  return baseString.substring(0, length - string.length) + string;
};
