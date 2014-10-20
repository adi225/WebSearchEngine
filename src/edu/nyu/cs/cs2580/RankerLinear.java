package edu.nyu.cs.cs2580;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Vector;

/**
 * Created by andreidinuionita on 10/8/14.
 */
public class RankerLinear extends Ranker {

    public static final double BETA_COS = 1.0/0.8;
    public static final double BETA_QL = 1.0/9.0;
    public static final double BETA_PHRASE = 1.0/300.0;
    public static final double BETA_NUMVIEWS = 1.0/20000.0;

    RankerCosine _cosineRanker;
    RankerQL _qlRanker;

    public RankerLinear(SearchEngine.Options options,
                        QueryHandler.CgiArguments arguments,
                        Indexer indexer) {
        super(options, arguments, indexer);
        _cosineRanker = new RankerCosine(options, arguments, indexer);
        _qlRanker = new RankerQL(options, arguments, indexer);
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
        double qlScore     = _qlRanker.scoreDocument(query, did);
        double numViews    = _indexer.getDoc(did).getNumViews();

        double combinedScore = BETA_COS      * cosineScore +
        		               BETA_QL       * qlScore +
        		               BETA_NUMVIEWS * numViews;

        return combinedScore;
    }
}