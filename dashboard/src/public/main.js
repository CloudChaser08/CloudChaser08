/**
 * Create and configure the bootstrap table
 */
function configureTable() {
  $('#table').bootstrapTable({
    onClickRow: function (row, $element, field) {

      // disable sorting
      $('#table').bootstrapTable(
        'refreshOptions' , {sortable: false}
      );

      // hide all other rows
      $('div#ingestion tbody tr').not('#' + $element.attr('id')).hide();

      // show this provider's time series
      $('div#time-series > ul').hide();
      $('div#time-series > ul#' + $element.attr('id') + '-timeseries').show();
      $('div#time-series').show();

      // reset the height
      $('#table').bootstrapTable(
        'resetView' , {height: 100}
      );
    }
  });
}

/**
 * When the '<<back' link is clicked
 */
$('div#time-series span#back a').click(function() {

  // Undo all jquery dom manipulations
  $('div#time-series > ul').hide();
  $('div#ingestion tbody tr').show();
  $('div#time-series').hide();

  // reset the table
  $('#table').bootstrapTable('destroy');
  configureTable();
});

/**
 * Function for sorting values by their 'true' ordering
 * instead of the arbitrary lexographical ordering
 */
function sortNumSorter(a, b) {
  return a.sortnumber - b.sortnumber;
}

configureTable();
