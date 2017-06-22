$( document ).ready(function() {
  $('div#ingestion table a').click(function() {

    $('div#ingestion table tr').removeClass('active');
    $(this).parent().parent().addClass('active');
    $('div#ingestion table tr').not('.active').hide();

    $('div#time-series > ul').hide();
    $('div#time-series > ul#' + $(this).attr('id')).show();
    $('div.header span.pull-right').show();
  });

  $('div.header span.pull-right').click(function() {
    $('div#ingestion table tr').removeClass('active');
    $('div#time-series > ul').hide();
    $('div#ingestion table tr').show();
    $('div.header span.pull-right').hide();
  });
});

