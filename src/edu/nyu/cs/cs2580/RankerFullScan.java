package edu.nyu.cs.cs2580;

import java.util.Collections;
import java.util.Vector;

import edu.nyu.cs.cs2580.QueryHandler.CgiArguments;
import edu.nyu.cs.cs2580.SearchEngine.Options;

/**
 * This Ranker makes a full scan over all the documents in the index. It is the
 * instructors' implementation of the Ranker in HW1.
 * 
 * @author fdiaz
 * @author congyu
 */
class RankerFullScan extends Ranker {

  public RankerFullScan(Options options,
      CgiArguments arguments, Indexer indexer) {
    super(options, arguments, indexer);
    System.out.println("Using Ranker: " + this.getClass().getSimpleName());
  }

  @Override
  public Vector<ScoredDocument> runQuery(Query query, int numResults) {    
    Vector<ScoredDocument> all = new Vector<ScoredDocument>();
    for (int i = 0; i < _indexer.numDocs(); ++i) {
      all.add(scoreDocument(query, i));
    }
    Collections.sort(all, Collections.reverseOrder());
    Vector<ScoredDocument> results = new Vector<ScoredDocument>();
    for (int i = 0; i < all.size() && i < numResults; ++i) {
      results.add(all.get(i));
    }
    return results;
  }

  private ScoredDocument scoreDocument(Query query, int did) {
    // Process the raw query into tokens.
    query.processQuery();

    // Get the document tokens.
    Document doc = _indexer.getDoc(did);
    Vector<String> docTokens = ((DocumentFull) doc).getConvertedTitleTokens();

    // Score the document. Here we have provided a very simple ranking model,
    // where a document is scored 1.0 if it gets hit by at least one query term.
    double score = 0.0;
    for (String docToken : docTokens) {
      for (String queryToken : query._tokens) {
        if (docToken.equals(queryToken)) {
          score = 1.0;
          break;
        }
      }
      if (score > 0.0) {
        break;
      }
    }
    return new ScoredDocument(doc, score);
  }
}
