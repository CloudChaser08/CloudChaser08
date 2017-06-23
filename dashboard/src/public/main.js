$( document ).ready(function() {
  $('div#ingestion table a').click(function() {

    $('div#ingestion tbody tr').removeClass('active');
    $(this).parent().parent().addClass('active');
    $('div#ingestion tbody tr').not('.active').hide();

    $('div#time-series > ul').hide();
    $('div#time-series > ul#' + $(this).attr('id')).show();
    $('div.header span.pull-right').show();
    $('div#time-series').show();
  });

  $('div.header span.pull-right').click(function() {
    $('div#ingestion tbody tr').removeClass('active');
    $('div#time-series > ul').hide();
    $('div#ingestion tbody tr').show();
    $('div.header span.pull-right').hide();
    $('div#time-series').hide();
  });
});

