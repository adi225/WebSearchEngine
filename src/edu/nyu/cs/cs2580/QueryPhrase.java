package edu.nyu.cs.cs2580;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Vector;


/**
 * @CS2580: implement this class for HW2 to handle phrase. If the raw query is
 * ["new york city"], the presence of the phrase "new york city" must be
 * recorded here and be used in indexing and ranking.
 */
public class QueryPhrase extends Query {

  public Map<String, List<String>> _phrases = new HashMap<String, List<String>>();
	
  public QueryPhrase(String query) {
    super(query);
  }

  @Override
  public void processQuery() {
    if(_query == null) {
      return;
    }
    if(!_phrases.isEmpty() || !_tokens.isEmpty()) {
      return;
    }

    // Count how many quotations we have.
    int count = 0;
    for (int i = 0; i < _query.length(); i++) {
      if (_query.charAt(i) == '"') {
        count++;
      }
    }

    if(count == 0) {
      /* How did we get here? this is a regular query */
      return;
    } else if(count % 2 == 0) {
      /* We have an even number of quotations */
      boolean inQuotes = false;
      // TODO Maybe use StringBuilder instead for mutability.
      String inQuotesString = "";
      for(int i = 0; i < _query.length(); i++) {
        char c = _query.charAt(i);
        if(c == '"' && inQuotes) {
          Scanner s = new Scanner(inQuotesString);
          List<String> phraseTokens = new ArrayList<String>();
          while (s.hasNext()) {
            //if(!IndexerInverted._stoppingWords.contains(s.next()))
            phraseTokens.add(TextUtils.performStemming(s.next()));
          }
          s.close();

          _phrases.put(inQuotesString, phraseTokens);

          inQuotes = false;
          inQuotesString = "";
        } else if (c == '"' && !inQuotes) {
          /* just starting a new string */
          if(!inQuotesString.isEmpty()) {
            Scanner s = new Scanner(inQuotesString);
            // TODO Maybe use super()?
            while (s.hasNext()) {
              //if(!IndexerInverted._stoppingWords.contains(s.next()))
              _tokens.add(TextUtils.performStemming(s.next()));
            }
            s.close();
          }

          inQuotes = true;
          inQuotesString = "";
        } else {
          /* Just a regular character */
          inQuotesString += c;
        }
      }

      Scanner s = new Scanner(inQuotesString);
      while (s.hasNext()) {
        //if(!IndexerInverted._stoppingWords.contains(s.next()))
        _tokens.add(TextUtils.performStemming(s.next()));
      }
      s.close();
    } else {
      /* Pretend that the user forgot to close the " and count towards the end */
      int index = _query.indexOf('"');
      String querySubString = _query.substring(index + 1);
      Scanner s = new Scanner(querySubString);
      Vector<String> phraseTokens = new Vector<String>();
      while (s.hasNext()) {
        //if(!IndexerInverted._stoppingWords.contains(s.next()))
        phraseTokens.add(TextUtils.performStemming(s.next()));
      }
      s.close();

      _phrases.put(querySubString, phraseTokens);

      if(index != 0) {
        s = new Scanner(_query.substring(0, index - 1));
        while (s.hasNext()) {
          //if(!IndexerInverted._stoppingWords.contains(s.next()))
          _tokens.add(TextUtils.performStemming(s.next()));
        }
        s.close();
      }
    }
  }
}
