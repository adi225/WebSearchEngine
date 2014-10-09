package edu.nyu.cs.cs2580;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
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

    protected double scoreDocument(Query query, int did) {
        // TODO Check that double processing is ok. (Query Handler also processes it).
        // Process the raw query into tokens.
        query.processQuery();

        // Get the document tokens.
        Document doc = _indexer.getDoc(did);
        Vector<String> docTokens = ((DocumentFull) doc).getConvertedBodyTokens();

        int documentSize = docTokens.size();
        long totalWordsInCorpus = _indexer.totalTermFrequency();
        double lambda = 0.5;
        double score = 0;

        // builds map of document word frequency
        Map<String, Integer> documentMap = new HashMap<String, Integer>();
        for(String word : docTokens) {
            if(documentMap.containsKey(word)) {
                documentMap.put(word, documentMap.get(word) + 1);
            } else {
                documentMap.put(word, 1);
            }
        }

        for(String word : docTokens) {
            int wordFrequencyInDocument = documentMap.containsKey(word) ? documentMap.get(word) : 0;
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

