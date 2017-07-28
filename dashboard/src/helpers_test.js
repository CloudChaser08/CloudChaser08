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

    expect(d.getYear()).to.equal(116);
    expect(d.getMonth()).to.equal(1);
    expect(d.getDate()).to.equal(1);
  });
  it('Does not modify incoming dates', function() {
    var initial = new Date('2016-01-01');
    var added = helpers.addMonths(1)(initial);

    expect(initial.getYear()).to.equal(116);
    expect(initial.getMonth()).to.equal(0);
    expect(initial.getDate()).to.equal(1);
    expect(added.getYear()).to.equal(116);
    expect(added.getMonth()).to.equal(1);
    expect(added.getDate()).to.equal(1);
  });
});

describe('addDays', function () {  
  it('Is curried', function() {
    expect(helpers.addDays(1)).to.be.a('Function');
  });
  it('Can add months to a date', function() {
    var d = helpers.addDays(1)(new Date('2016-01-01'));

    expect(d.getYear()).to.equal(116);
    expect(d.getMonth()).to.equal(0);
    expect(d.getDate()).to.equal(2);
  });
  it('Does not modify incoming dates', function() {
    var initial = new Date('2016-01-01');
    var added = helpers.addDays(1)(initial);

    expect(initial.getYear()).to.equal(116);
    expect(initial.getMonth()).to.equal(0);
    expect(initial.getDate()).to.equal(1);
    expect(added.getYear()).to.equal(116);
    expect(added.getMonth()).to.equal(0);
    expect(added.getDate()).to.equal(2);
  });
});

describe('formatDate', function () {  
  it('Correctly formats incoming dates', function() {
    expect(helpers.formatDate(new Date('2017-02-01'))).to.equal('2017-02-01');
  });
});
