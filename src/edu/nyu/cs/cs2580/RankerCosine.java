package edu.nyu.cs.cs2580;

import com.google.common.collect.Lists;

import java.util.*;

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
        while(doc != null) {
          double score = scoreDocument(query, doc._docid);
          scoredDocuments.add(new ScoredDocument(doc, score));
          if(scoredDocuments.size()>numResults){
              scoredDocuments.poll();
          }
          doc = _indexer.nextDoc(query, doc._docid);      // nextDoc() is conjunctive.
        }
        
        while(!scoredDocuments.isEmpty()){
          results.add(scoredDocuments.poll());
        }
        Collections.sort(results, Collections.reverseOrder());
        return results;
    }

    protected double scoreDocument(Query query, int did) {
    	// If the inverted-doconly is used, just return 1.0 as a score.
    	// This is because the index has only the docID, which is
    	// insufficient to calculate the score.
    	if(_indexer instanceof IndexerInvertedDoconly){
            System.out.println("Ranker does not support this indexer type.");
    		return 1.0;
    	}

        DocumentIndexed doc = ((DocumentIndexed)_indexer.getDoc(did));

        double score = 0, q_sqr = 0;
        double n = _indexer.numDocs();
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

        if(query instanceof QueryPhrase) {
          QueryPhrase queryPhrase = (QueryPhrase)query;
          List<List<String>> phrases = new ArrayList<List<String>>(queryPhrase._phrases.values());
          for (List<String> phrase : phrases) {
            for(String word : phrase) {
              if (queryMap.containsKey(word)) {
                queryMap.put(word, queryMap.get(word) + 1);
              } else {
                queryMap.put(word, 1);
              }
            }
          }
        }


        // iterates over all words in query
        for(String word : queryMap.keySet()) {
            idf = 1 + Math.log(n / _indexer.corpusDocFrequencyByTerm(word)) / Math.log(2);
            tf_q = queryMap.get(word); // count of term in query
            tf_d = _indexer.documentTermFrequency(word, doc._docid); // count of term in document
            tfidf_q = tf_q * idf;  // tfidf of term in query
            tfidf_d = tf_d * idf;  // tfidf of term in document
            q_sqr += tfidf_q * tfidf_q;  // computing sum(x^2) term of cosine similarity
            score += tfidf_q * tfidf_d;  // computing sum(x*y) term of cosine similarity
        }

        double d_sqr = doc.getTfidfSumSquared();

        if (q_sqr * d_sqr == 0)
            score = 0;
        else
            score /= Math.sqrt(q_sqr * d_sqr); // computing cosine similarity

        return score;
    }
}
