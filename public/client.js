// client-side js
// run by the browser each time your view template is loaded

// by default, you've got jQuery,
// add other scripts at the bottom of index.html

$(function() {
  console.log('hello world :o');
  
  $.get('/dreams', function(dreams) {
    if (dreams.length > 0) {
      dreams.forEach(function(dream) {
        $('<p></p>').text(dream.example).appendTo('div#dreams');
      });
    }
  });

  $('form').submit(function(event) {
    //event.preventDefault();
    var dream = $('input').val();
    $.post('/dreams?' + $.param({dream: dream}), function(dreams) {
      //$('<p></p>').text(dreams[dreams.length - 1].id1).appendTo('div#dreams');
      $('input').val('');
      $('input').focus();
    });
  });

});