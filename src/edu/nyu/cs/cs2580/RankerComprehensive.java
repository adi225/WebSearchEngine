package edu.nyu.cs.cs2580;

import java.util.Collections;
import java.util.PriorityQueue;
import java.util.Vector;

import edu.nyu.cs.cs2580.QueryHandler.CgiArguments;
import edu.nyu.cs.cs2580.SearchEngine.Options;

/**
 * @CS2580: Implement this class for HW3 based on your {@code RankerFavorite}
 * from HW2. The new Ranker should now combine both term features and the
 * document-level features including the PageRank and the NumViews. 
 */
public class RankerComprehensive extends Ranker {

	// TODO: Adjust the weighing parameters below by experimentation.
  public static final double BETA_COS = 1.0/0.8;
  public static final double BETA_PAGERANK = 1.0/0.8;
  public static final double BETA_NUMVIEWS = 1.0/20000.0;

  RankerCosine _cosineRanker;
	
  public RankerComprehensive(Options options,
      CgiArguments arguments, Indexer indexer) {
    super(options, arguments, indexer);
    _cosineRanker = new RankerCosine(options, arguments, indexer);
    System.out.println("Using Ranker: " + this.getClass().getSimpleName());
  }

  @Override
  public Vector<ScoredDocument> runQuery(Query query, int numResults) {
    Vector<ScoredDocument> results = new Vector<ScoredDocument>();
    
    PriorityQueue<ScoredDocument> scoredDocuments = new PriorityQueue<ScoredDocument>();
    Document doc = _indexer.nextDoc(query, -1);
    
    while(doc!=null){
    	Double score = scoreDocument(query,doc._docid);
    	scoredDocuments.add(new ScoredDocument(doc,score));
    	if(scoredDocuments.size()>numResults){
    		scoredDocuments.poll();
    	}
    	doc = _indexer.nextDoc(query, doc._docid);
    }
    
    while(!scoredDocuments.isEmpty()){
    	results.add(scoredDocuments.poll());
    }
    Collections.sort(results, Collections.reverseOrder());
    return results;
  }
  
  private double scoreDocument(Query query, int did) {

    double cosineScore = _cosineRanker.scoreDocument(query, did);
    double pageRank		 = _indexer.getDoc(did).getPageRank(); 
    double numViews    = _indexer.getDoc(did).getNumViews();

    double combinedScore = BETA_COS      * cosineScore +
    		               BETA_PAGERANK       * pageRank +
    		               BETA_NUMVIEWS * numViews;

    return combinedScore;
  }
}
