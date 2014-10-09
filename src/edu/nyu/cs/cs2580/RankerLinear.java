package edu.nyu.cs.cs2580;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
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
        Vector<ScoredDocument> all = new Vector<ScoredDocument>();
        for (int i = 0; i < _indexer.numDocs(); ++i) {
            all.add(new ScoredDocument(_indexer.getDoc(i), scoreDocument(query, i)));
        }
        Collections.sort(all, Collections.reverseOrder());
        Vector<ScoredDocument> results = new Vector<ScoredDocument>();
        for (int i = 0; i < all.size() && i < numResults; ++i) {
            results.add(all.get(i));
        }
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