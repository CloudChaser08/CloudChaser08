$( document ).ready(function() {

  // when any provider is clicked
  $('div#ingestion table a').click(function() {

    // activate the provider row
    $('div#ingestion tbody tr').removeClass('active');
    $(this).parent().parent().addClass('active');
    $('div#ingestion tbody tr').not('.active').hide();

    // hide all other providers, show this provider's time series
    $('div#time-series > ul').hide();
    $('div#time-series > ul#' + $(this).attr('id')).show();
    $('div#time-series').show();
  });

  // when the '<<back' link is clicked, revert everything back to the
  // original state
  $('div#time-series span#back a').click(function() {
    $('div#ingestion tbody tr').removeClass('active');
    $('div#time-series > ul').hide();
    $('div#ingestion tbody tr').show();
    $('div#time-series').hide();
  });
});

