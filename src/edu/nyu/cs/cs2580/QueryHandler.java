package edu.nyu.cs.cs2580;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.Vector;

import com.google.common.collect.Maps;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

/**
 * Handles each incoming query, students do not need to change this class except
 * to provide more query time CGI arguments and the HTML output.
 * 
 * N.B. This class is not thread-safe. 
 * 
 * @author congyu
 * @author fdiaz
 */
class QueryHandler implements HttpHandler {

  public final static int SESSION_TIMEOUT = 60000;

  /**
   * CGI arguments provided by the user through the URL. This will determine
   * which Ranker to use and what output format to adopt. For simplicity, all
   * arguments are publicly accessible.
   */
  public static class CgiArguments {
    // The raw user query
    public String _query = "";
    // How many results to return
    private int _numResults = 10;
    
    private int _numTerms = 10;
    
    // The type of the ranker we will be using.
    public enum RankerType {
      NONE,
      FULLSCAN,
      CONJUNCTIVE,
      FAVORITE,
      COSINE,
      PHRASE,
      QL,
      LINEAR,
      COMPREHENSIVE
    }
    public RankerType _rankerType = RankerType.NONE;
    
    // The output format.
    public enum OutputFormat {
      TEXT,
      HTML,
    }
    public OutputFormat _outputFormat = OutputFormat.TEXT;

    public CgiArguments(String uriQuery) {
      String[] params = uriQuery.split("&");
      for (String param : params) {
        String[] keyval = param.split("=", 2);
        if (keyval.length < 2) {
          continue;
        }
        String key = keyval[0].toLowerCase();
        String val = keyval[1];
        if (key.equals("query")) {
          _query = val;
        } else if (key.equals("num") || key.equals("numdocs")) {
          try {
            _numResults = Integer.parseInt(val);
          } catch (NumberFormatException e) {
            // Ignored, search engine should never fail upon invalid user input.
          }
        } else if (key.equals("ranker")) {
          try {
            _rankerType = RankerType.valueOf(val.toUpperCase());
          } catch (IllegalArgumentException e) {
            // Ignored, search engine should never fail upon invalid user input.
          }
        } else if (key.equals("format")) {
          try {
            _outputFormat = OutputFormat.valueOf(val.toUpperCase());
          } catch (IllegalArgumentException e) {
            // Ignored, search engine should never fail upon invalid user input.
          }
        }
        else if (key.equals("numterms")) {
          try {
            _numTerms = Integer.parseInt(val);
          } catch (NumberFormatException e) {
            // Ignored, search engine should never fail upon invalid user input.
          }
        }
      }  // End of iterating over params
    }
  }

  // For accessing the underlying documents to be used by the Ranker. Since 
  // we are not worried about thread-safety here, the Indexer class must take
  // care of thread-safety.
  private Indexer _indexer;

  public QueryHandler(SearchEngine.Options options, Indexer indexer) {
    _indexer = indexer;
  }

  private void respondWithMsg(HttpExchange exchange, final String message) throws IOException {
    Headers responseHeaders = exchange.getResponseHeaders();
    responseHeaders.set("Content-Type", "text/plain");
    exchange.sendResponseHeaders(200, 0); // arbitrary number of bytes
    OutputStream responseBody = exchange.getResponseBody();
    responseBody.write(message.getBytes());
    responseBody.close();
  }
  
  private void respondWithHTML(HttpExchange exchange, final String message) throws IOException {
    Headers responseHeaders = exchange.getResponseHeaders();
    responseHeaders.set("Content-Type", "text/html");
    exchange.sendResponseHeaders(200, 0); // arbitrary number of bytes
    OutputStream responseBody = exchange.getResponseBody();
    responseBody.write(message.getBytes());
    responseBody.close();
  }

  private void constructTextOutput(final Vector<ScoredDocument> docs, StringBuffer response) {
    for(ScoredDocument doc : docs) {
      response.append(response.length() > 0 ? "\n" : "");
      response.append(doc.asTextResult());
    }
    response.append(response.length() > 0 ? "\n" : "");
  }
  
  private void constructEvaluationOutput(final Vector<ScoredDocument> docs, StringBuffer response, String query) {
    for(ScoredDocument doc : docs) {
      response.append(query+"\t"+doc.getDocId()+"\t"+doc.getDocTitle()+"\t"+doc.getScore()+"\n");
    }
    response.append(response.length() > 0 ? "\n" : "");
  }
  
  private void constructHTMLOutput(final Vector<ScoredDocument> docs, StringBuffer response, String query, long timeTaken) {
    response.append("<html><head><style>font-family: arial,sans-serif;</style></head><body>");
    response.append("<p style=\"color:#A8A8A8;font-size:16px\">Your search has returned ");
    response.append(docs.size());
    response.append(" results in ").append(timeTaken).append(" ms </p>");
    response.append("<br/>");
    for(ScoredDocument doc : docs) {
      response.append(doc.asHtmlResult(query));
      response.append("<br/>");
    }
    response.append("</body></html>");
  }

  private Vector<ScoredDocument> searchQuery(HttpExchange exchange, String uriQuery) throws IOException {
    Vector<ScoredDocument> scoredDocs = null;

    // Process the CGI arguments.
    CgiArguments cgiArgs = new CgiArguments(uriQuery);
    if(cgiArgs._query.isEmpty()) {
      respondWithMsg(exchange, "No query is given!");
    }

    // Create the ranker.
    Ranker ranker = Ranker.Factory.getRankerByArguments(cgiArgs, SearchEngine.OPTIONS, _indexer);
    if(ranker == null) {
        respondWithMsg(exchange, "Ranker " + cgiArgs._rankerType.toString() + " is not valid!");
    }

    // Processing the query - check if we have exact match
    // TODO Remove dependency on implementation.
    Query processedQuery;
    if(cgiArgs._query.indexOf('"') == -1) {
      processedQuery = new Query(cgiArgs._query);
    } else {
      processedQuery = new QueryPhrase(cgiArgs._query);
    }
    processedQuery.processQuery();

    // Ranking.
    scoredDocs = ranker.runQuery(processedQuery, cgiArgs._numResults);
    return scoredDocs;
  }
  
  public void handle(HttpExchange exchange) throws IOException {
    String requestMethod = exchange.getRequestMethod();
    if (!requestMethod.equalsIgnoreCase("GET")) { // GET requests only.
      return;
    }

    String sessionId = null;
    // Print the user request header.
    Headers requestHeaders = exchange.getRequestHeaders();
    System.out.print("Incoming request: ");
    for (String key : requestHeaders.keySet()) {
      System.out.print(key + ":" + requestHeaders.get(key) + "; ");
      if(requestHeaders.containsKey("Cookie")) {
        String[] info = requestHeaders.getFirst("Cookie").split("&");
        if(Long.parseLong(info[1]) + SESSION_TIMEOUT > System.currentTimeMillis()) {
          sessionId = info[0];
        }
        break;
      }
    }
    Headers responseHeaders = exchange.getResponseHeaders();
    if(sessionId == null) {
      sessionId = UUID.randomUUID().toString();
    }
    responseHeaders.set("Set-Cookie", sessionId + "&" + System.currentTimeMillis());

    System.out.println();

    // Validate the incoming request.
    String uriQuery = exchange.getRequestURI().getQuery();
    String uriPath = exchange.getRequestURI().getPath();
    if (uriPath == null || uriQuery == null) {
      respondWithMsg(exchange, "Something wrong with the URI!");
    } else if (!uriPath.equals("/search") && !uriPath.equals("/clicktrack") && !uriPath.equals("/prf") && !uriPath.equals("/evaluation")) {
      respondWithMsg(exchange, "Only /search or /prf or /evaluation are handled!");
    } else if(uriPath.equalsIgnoreCase("/clicktrack")) {
      // writing out to files
      String logFileName = "hw3.4-log.tsv";
      FileWriter logFileWriter = new FileWriter("results/" + logFileName, true);
      PrintWriter vsmWriter = new PrintWriter(new BufferedWriter(logFileWriter));
      String[] params = uriQuery.split("&");
      String documentId = "ERROR";
      String query = "ERROR";
      if(params.length == 2) {
        documentId = params[0].split("=").length == 2 ? params[0].split("=")[1] : "ERROR";
        query = params[1].split("=").length == 2 ?  params[1].split("=")[1] : "ERROR";
      }
      
      String logEntry = sessionId +
    		  "\t" + query +
              "\t" + documentId + 
              "\tclick\t" + System.currentTimeMillis();
      vsmWriter.write(logEntry + "\n");
      vsmWriter.close();
      // Construct a simple response.
      responseHeaders.set("Location", exchange.getRequestHeaders().getFirst("Referer"));
      //responseHeaders.set("Location", "file:///" + _indexer.getDoc(Integer.parseInt(documentId)).getUrl());
      //exchange.sendResponseHeaders(302, 0);  // arbitrary number of bytes
      exchange.getResponseBody().close();
      return;
    } else if(uriPath.equalsIgnoreCase("/search")) {
      System.out.println("Query: " + uriQuery);
      long startTime = Calendar.getInstance().getTimeInMillis();
      Vector<ScoredDocument> scoredDocs = searchQuery(exchange, uriQuery);
      long endTime = Calendar.getInstance().getTimeInMillis();

      CgiArguments cgiArgs = new CgiArguments(uriQuery);

      StringBuffer response = new StringBuffer();

      switch (cgiArgs._outputFormat) {
        case TEXT:
          constructTextOutput(scoredDocs, response);
          respondWithMsg(exchange, response.toString());
          break;
        case HTML:
          constructHTMLOutput(scoredDocs, response, cgiArgs._query, endTime-startTime);
          respondWithHTML(exchange, response.toString());
          break;
        default:
          // nothing
      }
      System.out.println("Finished query: " + cgiArgs._query);
    } else if(uriPath.equalsIgnoreCase("/prf")){
      CgiArguments cgiArgs = new CgiArguments(uriQuery);

      System.out.println("Query: " + uriQuery);
      Vector<ScoredDocument> scoredDocs = searchQuery(exchange, uriQuery);

      PseudoRelevanceFeedbackProvider prf = new PseudoRelevanceFeedbackProvider((IndexerInverted)_indexer);
      List<Entry<String, Double>> topWords = prf.getExpansionTermsForDocuments(scoredDocs, cgiArgs._numTerms);

      StringBuffer response = new StringBuffer();
      for (Entry<String, Double> entry : topWords) {
        response.append(entry.getKey()).append("\t").append(entry.getValue()).append("\n");
      }

      respondWithMsg(exchange, response.toString());
      System.out.println("Finished query: " + cgiArgs._query);
    }else if(uriPath.equalsIgnoreCase("/evaluation")) {
      System.out.println("Query: " + uriQuery);

      Vector<ScoredDocument> scoredDocs = searchQuery(exchange, uriQuery);
      if(scoredDocs.size() < 10){
      	System.out.println("Unable to evaluate in case that less than 10 documents are returned.");
      	return;
      }
      
      CgiArguments cgiArgs = new CgiArguments(uriQuery);

      Evaluator evaluator = new Evaluator(cgiArgs._query);
      
      Vector<String> retrievalResults = new Vector<String>();
      for(ScoredDocument scoredDoc : scoredDocs){
      	String retrievalResult = cgiArgs._query + "\t" + scoredDoc.getDocId();
      	retrievalResults.add(retrievalResult);
      }
      
      String evaluationResult = evaluator.getEvaluationAsString(cgiArgs._query, retrievalResults, evaluator.judgements);
      
      respondWithMsg(exchange, evaluationResult);
      
      System.out.println("Finished query: " + cgiArgs._query);
    }
  }
}