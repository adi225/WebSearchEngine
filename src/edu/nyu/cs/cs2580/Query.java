package edu.nyu.cs.cs2580;

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
    _query = performStemming(query);
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
      if(!IndexerInverted._stoppingWords.contains(token)){  // if token is not a stopping word, add it to _tokens
        _tokens.add(token);
      }
    }
    s.close();
  }
  
  public String performStemming(String text){
	    Stemmer stemmer = new Stemmer();
	    stemmer.add(text.toCharArray(), text.length());
	    stemmer.stem();
		String result = stemmer.toString();
	    return text;
	  }
}
