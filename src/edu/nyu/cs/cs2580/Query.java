package edu.nyu.cs.cs2580;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Scanner;
import java.util.Vector;

/**
 * Representation of a user query.
 * 
 * In HW1: instructors provide this simple implementation.
 * 
 * In HW2: students must implement {@link QueryPhrase} to handle phrases.
 * 
 * @author congyu
 * @auhtor fdiaz
 */
public class Query {
  public String _query = null;
  public Vector<String> _tokens = new Vector<String>();

  public Query(String query) {
    // TODO Do not process on creation. (need more processing too)
    try {
      _query = URLDecoder.decode(query, "UTF-8");
      _query = performStemming(_query);
    } catch (UnsupportedEncodingException e) {
      SearchEngine.Check(false, "Query is not in UTF-8 encoding.");
    }
  }

  public void processQuery() {
    if (_query == null) {
      return;
    }
    if(_tokens.size() > 0){
      return;
    }
    Scanner s = new Scanner(_query);
    while (s.hasNext()) {
      String token = s.next();
      _tokens.add(token);
    }
    s.close();
  }
  
  public String performStemming(String text){
	    Stemmer stemmer = new Stemmer();
	    stemmer.add(text.toCharArray(), text.length());
	    stemmer.stem();
		String result = stemmer.toString();
    // TODO This is a bug. Return result, not text.
	    return text;
	  }
}
