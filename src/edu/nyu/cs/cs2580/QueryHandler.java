package edu.nyu.cs.cs2580;

import com.google.common.collect.Sets;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONStringer;
import org.json.JSONWriter;

import java.io.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;

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

	private static final double TOP_WORD_THRESHOLD = 0.05;
  private static final int NUM_ADDED_TOP_WORDS = 1;
  private static final int NUM_SUGGESTED_QUERIES = 5;
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
      JSON
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
  private AutocompleteQueryLog _autocompleter;
  private Ranker _ranker;
  private Query _processedQuery;

  public QueryHandler(SearchEngine.Options options, Indexer indexer) {
    _indexer = indexer;
    _autocompleter = ((IndexerInverted)indexer)._autocompleter;
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
    responseHeaders.set("Access-Control-Allow-Origin", "*");
    exchange.sendResponseHeaders(200, 0); // arbitrary number of bytes
    OutputStream responseBody = exchange.getResponseBody();
    responseBody.write(message.getBytes());
    responseBody.close();
  }

  private void respondWithJSON(HttpExchange exchange, final String message) throws IOException {
    Headers responseHeaders = exchange.getResponseHeaders();
    responseHeaders.set("Content-Type", "application/json");
    responseHeaders.set("Access-Control-Allow-Origin", "*");
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

  private Vector<ScoredDocument> searchQuery(HttpExchange exchange, CgiArguments cgiArgs) throws IOException {
    // Processing the query - check if we have exact match
    // TODO Remove dependency on implementation.
    if (cgiArgs._query.indexOf('"') == -1) {
      _processedQuery = new Query(cgiArgs._query);
    } else {
      _processedQuery = new QueryPhrase(cgiArgs._query);
    }
    _processedQuery.processQuery();

    return _ranker.runQuery(_processedQuery, cgiArgs._numResults);
  }

  private Vector<ScoredDocument> searchQueryPRF(HttpExchange exchange, CgiArguments cgiArgs) throws IOException {
    Vector<ScoredDocument> scoredDocs = searchQuery(exchange, cgiArgs);
    
    PseudoRelevanceFeedbackProvider prf = new PseudoRelevanceFeedbackProvider((IndexerInverted)_indexer);
    List<Entry<String, Double>> topWords = prf.getExpansionTermsForDocuments(scoredDocs, cgiArgs._numTerms);
    
    _processedQuery = new Query(cgiArgs._query);
    // add top words into the original query
    int numTopWordsAdded = 0;
    for (int i = 0; i < topWords.size(); i++) {
    	if(topWords.get(i).getValue() >= TOP_WORD_THRESHOLD){
	      String topWord = topWords.get(i).getKey();
	      if(!_processedQuery._query.toLowerCase().contains(topWord.toLowerCase())){
	      	_processedQuery._query += " " + topWord;
	      	numTopWordsAdded++;
	      }    		
    	}
    }
    _processedQuery.processQuery();
        
    scoredDocs = _ranker.runQuery(_processedQuery, cgiArgs._numResults);
    return scoredDocs;
  }
  
  
  public void handle(HttpExchange exchange) throws IOException {
    String requestMethod = exchange.getRequestMethod();
    if (!requestMethod.equalsIgnoreCase("GET")) { // GET requests only.
      return;
    }
    String uriQuery = exchange.getRequestURI().getQuery();
    String uriPath = exchange.getRequestURI().getPath();

    String sessionId = null;
    // Print the user request header.
    Headers requestHeaders = exchange.getRequestHeaders();
    System.out.print("Incoming request: ");
    for (String key : requestHeaders.keySet()) {
      System.out.print(key + ":" + requestHeaders.get(key) + "; ");
      if(requestHeaders.containsKey("Cookie")) {
        sessionId = requestHeaders.getFirst("Cookie");
        break;
      }
    }
    Headers responseHeaders = exchange.getResponseHeaders();
    if(sessionId == null) {
      sessionId = UUID.randomUUID().toString();
    }
    Date expireAt = new Date(System.currentTimeMillis() + SESSION_TIMEOUT);
    DateFormat dateFormat = new SimpleDateFormat("EEE, d MMM yyyy HH:mm:ss z");
    dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));

    responseHeaders.set("Set-Cookie", sessionId + "; expires=" + dateFormat.format(expireAt));

    System.out.println();

    // Validate the incoming request.
    Set<String> validEndpoints = Sets.newHashSet("/search", "/clicktrack", "/prf", "/prfsearch",
                                                 "/evaluation", "/prfevaluation", "/instant", 
                                                 "/generateretrievalresults", "/prfgenerateretrievalresults");
    if (uriPath == null) {
      respondWithMsg(exchange, "Something wrong with the URI!");
    } else if(uriPath.startsWith("/document/")) {
      String[] tokens = uriPath.split("/");
      String fileName = tokens[tokens.length-1];
      StringBuilder response = new StringBuilder();
      File fileToReturn = new File(SearchEngine.OPTIONS._corpusPrefix + "/" + fileName);
      if(!fileToReturn.exists()) {
        String redirectToURL = "http://wikipedia.org/wiki/" + fileName;
        responseHeaders.set("Location", redirectToURL);
        exchange.sendResponseHeaders(302, 0);  // arbitrary number of bytes
        exchange.getResponseBody().close();
        return;
      }
      Scanner scanner = new Scanner(fileToReturn);
      while(scanner.hasNext()) {
        response.append(scanner.nextLine() + "\n");
      }
      scanner.close();
      respondWithHTML(exchange, response.toString());
    } else if (!validEndpoints.contains(uriPath.toLowerCase())) {
      respondWithMsg(exchange, "Endpoint is not handled!");
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
      try {
//        String redirectToURL = "document/" + _indexer.getDoc(Integer.parseInt(documentId)).getUrl();
//        responseHeaders.set("Location", redirectToURL);
//        exchange.sendResponseHeaders(302, 0);  // arbitrary number of bytes
//        exchange.getResponseBody().close();
      } catch (NumberFormatException e ) {}
      return;
    } else {
      CgiArguments cgiArgs = new CgiArguments(uriQuery);
      if(cgiArgs._query.isEmpty() && !uriPath.equalsIgnoreCase("/generateretrievalresults") && !uriPath.equalsIgnoreCase("/prfgenerateretrievalresults")) {
        respondWithMsg(exchange, "No query is given!");
      }

      // Create the ranker.
      _ranker = Ranker.Factory.getRankerByArguments(cgiArgs, SearchEngine.OPTIONS, _indexer);
      if(_ranker == null) {
        respondWithMsg(exchange, "Ranker " + cgiArgs._rankerType.toString() + " is not valid!");
      }

      if(uriPath.equalsIgnoreCase("/search")) {
        System.out.println("Query: " + uriQuery);
        long startTime = Calendar.getInstance().getTimeInMillis();
        Vector<ScoredDocument> scoredDocs = searchQuery(exchange, cgiArgs);
        long endTime = Calendar.getInstance().getTimeInMillis();

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
          case JSON:
        	try
        	{
        		JSONObject returnObj = new JSONObject();
        	  JSONArray docsArray = new JSONArray();
              if(scoredDocs != null) {
    	          for(ScoredDocument doc : scoredDocs) {	
    	        	  JSONObject tempObj = new JSONObject();
    	        	  tempObj.put("id", doc.getDocId());
    	        	  tempObj.put("title", doc.getDocTitle());
    	        	  tempObj.put("url", doc.getUrl());
    	        	  tempObj.put("query", cgiArgs._query);
    	        	  docsArray.put(tempObj);
    	          }
              }
                       
              returnObj.put("time", endTime-startTime);
              returnObj.put("results", docsArray);
              response.append(returnObj.toString());
            } catch (JSONException e) {}
            
        	respondWithJSON(exchange, response.toString());
          default:
            // nothing
        }
        _autocompleter.recordQuery(cgiArgs._query);
        System.out.println("Finished query: " + cgiArgs._query);

      } else if(uriPath.equalsIgnoreCase("/prf")){
        System.out.println("Query: " + uriQuery);
        Vector<ScoredDocument> scoredDocs = searchQuery(exchange, cgiArgs);

        PseudoRelevanceFeedbackProvider prf = new PseudoRelevanceFeedbackProvider((IndexerInverted)_indexer);
        List<Entry<String, Double>> topWords = prf.getExpansionTermsForDocuments(scoredDocs, cgiArgs._numTerms);

        StringBuffer response = new StringBuffer();
        for (Entry<String, Double> entry : topWords) {
          response.append(entry.getKey()).append("\t").append(entry.getValue()).append("\n");
        }

        respondWithMsg(exchange, response.toString());
        System.out.println("Finished query: " + cgiArgs._query);

      } else if(uriPath.equalsIgnoreCase("/prfsearch")){
        System.out.println("Query: " + uriQuery);
        long startTime = Calendar.getInstance().getTimeInMillis();
        Vector<ScoredDocument> scoredDocs = searchQueryPRF(exchange, cgiArgs);
        long endTime = Calendar.getInstance().getTimeInMillis();

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
        _autocompleter.recordQuery(cgiArgs._query);
        System.out.println("Finished query: " + cgiArgs._query);

      } else if(uriPath.equalsIgnoreCase("/evaluation")) {
        System.out.println("Query: " + uriQuery);

        Vector<ScoredDocument> scoredDocs = searchQuery(exchange, cgiArgs);
        if(scoredDocs.size() < 10){
          System.out.println("Unable to evaluate in case that less than 10 documents are returned.");
          return;
        }

        Evaluator evaluator = new Evaluator(cgiArgs._query);

        Vector<String> retrievalResults = new Vector<String>();
        for(ScoredDocument scoredDoc : scoredDocs){
          String retrievalResult = cgiArgs._query + "\t" + scoredDoc.getDocId();
          retrievalResults.add(retrievalResult);
        }

        String evaluationResult = evaluator.getAllEvaluationAsString(cgiArgs._query, retrievalResults, evaluator.judgements);

        respondWithMsg(exchange, evaluationResult);

        System.out.println("Finished query: " + cgiArgs._query);

      } else if(uriPath.equalsIgnoreCase("/instant")) {
        System.out.println("Query: " + uriQuery);
        

        List<String> suggestions = null;
        try
        {
         suggestions = _autocompleter.topAutoCompleteSuggestions(cgiArgs._query, NUM_SUGGESTED_QUERIES);
        }
        catch(Exception e){}

        if(suggestions != null)
        	cgiArgs._query = cgiArgs._query + suggestions.get(0);
        
        long startTime = Calendar.getInstance().getTimeInMillis();
        Vector<ScoredDocument> scoredDocs = searchQuery(exchange, cgiArgs);
        long endTime = Calendar.getInstance().getTimeInMillis();

        StringBuffer response = new StringBuffer();

        try {
        JSONObject returnObj = new JSONObject();
        JSONArray suggestionsArray = new JSONArray();
        JSONArray docsArray = new JSONArray();
                 
	      if(suggestions != null){
	          for (String suggestion : suggestions) {
	        	  suggestionsArray.put(suggestion);
	          }
	      }
          
          if(scoredDocs != null) {
	          for(ScoredDocument doc : scoredDocs) {	
	        	  JSONObject tempObj = new JSONObject();
	        	  tempObj.put("id", doc.getDocId());
	        	  tempObj.put("title", doc.getDocTitle());
	        	  tempObj.put("url", doc.getUrl());
	        	  tempObj.put("query", cgiArgs._query);
	        	  docsArray.put(tempObj);
	          }
          }
          
          returnObj.put("suggestions", suggestionsArray);
          returnObj.put("results", docsArray);
          returnObj.put("time", endTime-startTime);
          response.append(returnObj.toString());
        } catch (JSONException e) {}

        if(cgiArgs._outputFormat.equals(CgiArguments.OutputFormat.JSON)) {
          respondWithJSON(exchange, response.toString());
        }
        System.out.println("Finished query: " + cgiArgs._query);
      } else if(uriPath.equalsIgnoreCase("/prfevaluation")) {
        System.out.println("Query: " + uriQuery);

        Vector<ScoredDocument> scoredDocs = searchQueryPRF(exchange, cgiArgs);
        if(scoredDocs.size() < 10){
          System.out.println("Unable to evaluate in case that less than 10 documents are returned.");
          return;
        }

        Evaluator evaluator = new Evaluator(cgiArgs._query);

        Vector<String> retrievalResults = new Vector<String>();
        for(ScoredDocument scoredDoc : scoredDocs){
          String retrievalResult = cgiArgs._query + "\t" + scoredDoc.getDocId();
          retrievalResults.add(retrievalResult);
        }

        String evaluationResult = evaluator.getAllEvaluationAsString(cgiArgs._query, retrievalResults, evaluator.judgements);
        respondWithMsg(exchange, evaluationResult);

        System.out.println("Finished query: " + cgiArgs._query);
      } else if(uriPath.equalsIgnoreCase("/generateretrievalresults")) {
      	
      	ArrayList<String> queries = new ArrayList<String>();
      	BufferedReader reader = new BufferedReader(new FileReader(Evaluator.queriesFilePath));
      	String query = "";
      	while((query = reader.readLine()) != null){
      		queries.add(query);
      	}
      	reader.close();
      	
      	long averageRetrievalTime = 0;
      	int numSuccessfulQuery = 0;
      	
      	for(String q : queries){
      		System.out.println("Retrieving results for query: " +  q);
      		_processedQuery = new Query(q);
      		_processedQuery.processQuery();
      		long startTime = Calendar.getInstance().getTimeInMillis();
          Vector<ScoredDocument> scoredDocs = _ranker.runQuery(_processedQuery, cgiArgs._numResults);
          long endTime = Calendar.getInstance().getTimeInMillis();
          if(scoredDocs == null){
          	System.out.println("The query " + q +" failed.");
          	continue;
          }
          
          averageRetrievalTime += endTime - startTime;
          numSuccessfulQuery++;
          
          PrintWriter writer = new PrintWriter(Evaluator.retrievalResultsFolderPath + q + ".txt");
          for(ScoredDocument scoredDoc : scoredDocs){
            String retrievalResult = q + "\t" + scoredDoc.getDocId();
            writer.println(retrievalResult);
          }
          writer.close();
      	}
      	
      	averageRetrievalTime /= numSuccessfulQuery;
      	
      	System.out.println("Average retrieval time: " + averageRetrievalTime + " ms");
      	System.out.println("Done generating the retrieval result file!");
      } else if(uriPath.equalsIgnoreCase("/prfgenerateretrievalresults")) {

      	ArrayList<String> queries = new ArrayList<String>();
      	BufferedReader reader = new BufferedReader(new FileReader(Evaluator.queriesFilePath));
      	String query = "";
      	while((query = reader.readLine()) != null){
      		queries.add(query);
      	}
      	reader.close();
      	
      	long averageRetrievalTime = 0;
      	int numSuccessfulQuery = 0;
      	
      	for(String q : queries){
      		System.out.println("Retrieving results for query: " +  q);
      		_processedQuery = new Query(q);
      		_processedQuery.processQuery();
      		long startTime = Calendar.getInstance().getTimeInMillis();
          Vector<ScoredDocument> scoredDocs = _ranker.runQuery(_processedQuery, cgiArgs._numResults);
          
          PseudoRelevanceFeedbackProvider prf = new PseudoRelevanceFeedbackProvider((IndexerInverted)_indexer);
          List<Entry<String, Double>> topWords = prf.getExpansionTermsForDocuments(scoredDocs, cgiArgs._numTerms);
          // add top words into the original query
          _processedQuery = new Query(q);
          int numTopWordsAdded = 0;
          for (int i = 0; i < topWords.size(); i++) {
          	if(topWords.get(i).getValue() >= TOP_WORD_THRESHOLD){
	            String topWord = topWords.get(i).getKey();
	            if(!_processedQuery._query.toLowerCase().contains(topWord.toLowerCase())){
	            	_processedQuery._query += " " + topWord;
	            	numTopWordsAdded++;
	            }         		
          	}
          }
          _processedQuery.processQuery();
          System.out.println("New query: "+_processedQuery._query);
      		scoredDocs = _ranker.runQuery(_processedQuery, cgiArgs._numResults);
      		long endTime = Calendar.getInstance().getTimeInMillis();
          if(scoredDocs == null){
          	System.out.println("The query " + q +" failed.");
          	continue;
          }
          
          averageRetrievalTime += endTime - startTime;
          numSuccessfulQuery++;

          PrintWriter writer = new PrintWriter(Evaluator.retrievalResultsFolderPath + q + ".txt");
          for(ScoredDocument scoredDoc : scoredDocs){
            String retrievalResult = q + "\t" + scoredDoc.getDocId();
            writer.println(retrievalResult);
          }
          writer.close();
      	}
      	
      	averageRetrievalTime /= numSuccessfulQuery;
      	
      	System.out.println("Average retrieval time: " + averageRetrievalTime + " ms");
      	System.out.println("Done generating the retrieval result file!");
      }
    }
  }
}