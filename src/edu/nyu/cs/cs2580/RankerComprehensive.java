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
  public static final double alpha = 0.75;
  public static final double beta = 0.000001;

  public static final double maxPageRank = 0.01712521;
  public static final double minPageRank = 1.0040161E-5;
  
  public static final double maxNumViews = 420930;
  public static final double minNumViews = 0;
  
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

    // Normalization of PageRank and NumViews so that the value is between [0,1]
    pageRank = (pageRank - minPageRank) / (maxPageRank - minPageRank);
    numViews = (numViews - minNumViews) / (maxNumViews - minNumViews);
    
    double visitationScore = beta * pageRank + (1 - beta) * numViews;
    
    double combinedScore = alpha * cosineScore + (1 - alpha) * visitationScore;

    return combinedScore;
  }
}
