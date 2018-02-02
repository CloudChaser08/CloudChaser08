const expect = require('chai').expect;

const helpers = require('./helpers.js');

describe('leftZPad', function () {  
  it('Correctly pads an input string', function() {
    expect(helpers.leftZPad('1')).to.equal('01');
    expect(helpers.leftZPad('01')).to.equal('01');
    expect(helpers.leftZPad('001')).to.equal('001');
  });
});

describe('addMonths', function () {  
  it('Is curried', function() {
    expect(helpers.addMonths(1)).to.be.a('Function');
  });
  it('Can add months to a date', function() {
    var d = helpers.addMonths(1)(new Date('2016-01-01'));

    expect(d.getUTCFullYear()).to.equal(2016);
    expect(d.getUTCMonth()).to.equal(1);
    expect(d.getUTCDate()).to.equal(1);
  });
  it('Does not modify incoming dates', function() {
    var initial = new Date('2016-01-01');
    var added = helpers.addMonths(1)(initial);

    expect(initial.getUTCFullYear()).to.equal(2016);
    expect(initial.getUTCMonth()).to.equal(0);
    expect(initial.getUTCDate()).to.equal(1);
    expect(added.getUTCFullYear()).to.equal(2016);
    expect(added.getUTCMonth()).to.equal(1);
    expect(added.getUTCDate()).to.equal(1);
  });
});

describe('addDays', function () {  
  it('Is curried', function() {
    expect(helpers.addDays(1)).to.be.a('Function');
  });
  it('Can add months to a date', function() {
    var d = helpers.addDays(1)(new Date('2016-01-01'));

    expect(d.getUTCFullYear()).to.equal(2016);
    expect(d.getUTCMonth()).to.equal(0);
    expect(d.getUTCDate()).to.equal(2);
  });
  it('Does not modify incoming dates', function() {
    var initial = new Date('2016-01-01');
    var added = helpers.addDays(1)(initial);

    expect(initial.getUTCFullYear()).to.equal(2016);
    expect(initial.getUTCMonth()).to.equal(0);
    expect(initial.getUTCDate()).to.equal(1);
    expect(added.getUTCFullYear()).to.equal(2016);
    expect(added.getUTCMonth()).to.equal(0);
    expect(added.getUTCDate()).to.equal(2);
  });
});

describe('formatDate', function () {  
  it('Correctly formats incoming dates', function() {
    expect(helpers.formatDate(new Date('2017-02-01'))).to.equal('2017-02-01');
  });
});
