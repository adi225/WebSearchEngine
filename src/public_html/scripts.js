var RANKER = "conjunctive";
var ENDPOINT = "http://localhost:25814/";
var AUTOCOMPLETE = "instant";
var SEARCH = "search";
var LOG = "clicktrack";
var DOCUMENT = "document";
var NUMDOCS = 25;

function showLoadingIcon()
{
  var image = "<div class='loading'><div><br><br><img src='images/ajax-loader.gif' /></div></div>";
  $("#results").html(image);
}

function populateResultsHTML(results, time)
{

  var allHtml = "";
  var numberOfResults = results.length;
  if(results != null && numberOfResults != 0)
  {
    for (var i = 0; i < numberOfResults; i++) 
    {
      var singleHtml = generateDocHTML(results[i]); 
      allHtml += singleHtml;
    }

    $("#results").html(allHtml);

    $("#time").html("Your search returned in " + numeral(time).format('0,0') + " ms.");
  }
  else
    $("#results").html("<p class='noresults'> No results found </p>");

}

function generateDocHTML(result)
{

  var singleHtml = "<div class=\"singleresult\"><p><a onclick=\"docClicked('" + result.id + "','" + result.url + "','" + result.query + "');\">";
  singleHtml += result.title;
  singleHtml += "</a></p>";
  singleHtml += "</div>";
  // singleHtml += "<br></br>";

  return singleHtml;
}

function docClicked(id, url, query)
{
  //log click
  $.ajax({
          
    url: ENDPOINT + LOG,
    data: {
      "documentId": id,
      "query" : query
    }
  }); 
  window.location.href = ENDPOINT + DOCUMENT + "/" + url;

}

function toggleHeader()
{
  if (first)
  {
    $("#header").removeClass("centre");
    $("#biglogo").hide();
    $("#logosmall").show();
    $("#query").addClass("left");
    $("#autocomplete").addClass("left");
    $("#gobutton").show();
    $("#results").show();

  }
}

function search()
{
  if($("#autocomplete").val() != "")
  {
    $(".ui-menu-item").hide();
        $.ajax({
          beforeSend : function (XMLHttpRequest)
          {
            showLoadingIcon();
            $("#time").html("");
          },
          url: ENDPOINT + SEARCH,
          dataType: "JSON",
          data: {
            "query": $("#autocomplete").val(),
            "ranker": RANKER,
            "format": "json",
            "numdocs" : NUMDOCS
          },
          error: function (jqXHR, textStatus, errorThrown)
          {
            $("results").html("<p> An error took place </p>");
          },
          success: function( data ) 
          {
            $('#query').val($("#autocomplete").val());

            populateResultsHTML(data.results, data.time);
            
          }
        });
  }
}