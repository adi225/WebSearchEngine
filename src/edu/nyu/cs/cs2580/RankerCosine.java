package edu.nyu.cs.cs2580;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Vector;

/**
 * Created by andreidinuionita on 10/8/14.
 */
public class RankerCosine extends Ranker {

    public RankerCosine(SearchEngine.Options options,
                        QueryHandler.CgiArguments arguments,
                        Indexer indexer) {
        super(options, arguments, indexer);
        System.out.println("Using Ranker: " + this.getClass().getSimpleName());
    }

    // SLIDE'S IMPLEMENTATION
    // This should be done according to the lecture slide 3, page 52
    // (Document-at-a-time Query Processing), or page 54
    // (Term-at-a-time Query Processing). The former is prefered in practice.
    @Override
    public Vector<ScoredDocument> runQuery(Query query, int numResults) {
//	Commented below is the previous implementation of query processing, which needed to be changed.

//    	 Vector<ScoredDocument> all = new Vector<ScoredDocument>();
//    	 for (int i = 0; i < _indexer.numDocs(); ++i) {
//    		 all.add(new ScoredDocument(_indexer.getDoc(i), scoreDocument(query, i)));
//    	 }
//    	 Collections.sort(all, Collections.reverseOrder());
//    	 Vector<ScoredDocument> results = new Vector<ScoredDocument>();
//    	 for (int i = 0; i < all.size() && i < numResults; ++i) {
//    		 results.add(all.get(i));
//    	 }
//    	 return results;
    	
    	
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
        
        return results;
    }

    protected double scoreDocument(Query query, int did) {
        // TODO Check that double processing is ok. (Query Handler also processes it).
        // Process the raw query into tokens.
        query.processQuery();

        // Get the document tokens.
        Document doc = _indexer.getDoc(did);
        Vector<String> docTokens = ((DocumentFull) doc).getConvertedBodyTokens();

        double score = 0, q_sqr = 0, d_sqr = 0;
        int n = _indexer.numDocs();
        double idf, tf_q, tf_d, tfidf_q, tfidf_d;

        // builds map of query word frequency
        Map<String, Integer> queryMap = new HashMap<String, Integer>();
        for(String word : query._tokens) {
            if(queryMap.containsKey(word)) {
                queryMap.put(word, queryMap.get(word) + 1);
            } else {
                queryMap.put(word, 1);
            }
        }

        // builds map of document word frequency
        Map<String, Integer> documentMap = new HashMap<String, Integer>();
        for(String word : docTokens) {
            if(documentMap.containsKey(word)) {
                documentMap.put(word, documentMap.get(word) + 1);
            } else {
                documentMap.put(word, 1);
            }
        }

        // iterates over all words in query
        for(String word : queryMap.keySet()) {
            idf = 1 + Math.log(n / _indexer.corpusDocFrequencyByTerm(word)) / Math.log(2);
            tf_q = queryMap.get(word); // count of term in query
            tf_d = documentMap.containsKey(word) ? documentMap.get(word) : 0; // count of term in document
            tfidf_q = tf_q * idf;  // tfidf of term in query
            tfidf_d = tf_d * idf;  // tfidf of term in document
            q_sqr += tfidf_q * tfidf_q;  // computing sum(x^2) term of cosine similarity
            score += tfidf_q * tfidf_d;  // computing sum(x*y) term of cosine similarity
        }

        // we count sum(y^2) separately so that we include all words in document
        for(String word : documentMap.keySet()) {
            idf = 1 + Math.log(n / _indexer.corpusDocFrequencyByTerm(word)) / Math.log(2);
            tf_d = documentMap.containsKey(word) ? documentMap.get(word) : 0; // count of term in document
            tfidf_d = tf_d * idf;  // tfidf of term in document
            d_sqr += tfidf_d * tfidf_d; // computing sum(y^2) term of cosine similarity
        }

        if (q_sqr * d_sqr == 0)
            score = 0;
        else
            score /= Math.sqrt(q_sqr * d_sqr); // computing cosine similarity

        return score;
    }

}
