package edu.nyu.cs.cs2580;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Vector;

/**
 * Created by andreidinuionita on 10/8/14.
 */
public class RankerQL extends Ranker {

    public RankerQL(SearchEngine.Options options,
                    QueryHandler.CgiArguments arguments,
                    Indexer indexer) {
        super(options, arguments, indexer);
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

    protected double scoreDocument(Query query, int did) {

        int documentSize = ((DocumentIndexed)_indexer.getDoc(did)).getDocumentSize();  // get value from the Document
        long totalWordsInCorpus = _indexer.totalTermFrequency();
        double lambda = 0.5;
        double score = 0;

        for(String word : query._tokens) {
            int wordFrequencyInDocument = _indexer.documentTermFrequency(word, did);
            int wordFrequencyInCorpus = _indexer.corpusTermFrequency(word);

            // This formula calculates the value of log( P(Q|D) ), which is given by
            // the summation of log( ((1-lambda) * wordFrequencyInDocument/documentSize) + wordFrequencyInCorpus/totalWordsInCorpus )
            // Usage of the log is to overcome the problem of multiplying many small numbers together, which might
            // lead to accuracy problem.
            score += Math.log(((1 - lambda) * ((double) wordFrequencyInDocument) / documentSize) +
                    lambda * ((double) wordFrequencyInCorpus) / totalWordsInCorpus);
        }

        return score;
    }
}

