package edu.nyu.cs.cs2580;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Vector;

/**
 * Created by andreidinuionita on 11/24/14.
 */
public class PseudoRelevanceFeedbackProvider {

  private IndexerInverted _indexer;

  public PseudoRelevanceFeedbackProvider(IndexerInverted index) {
    _indexer = index;
  }

  public List<Map.Entry<String, Double>> getExpansionTermsForDocuments(Vector<ScoredDocument> scoredDocs, int numTerms) throws IOException {
    /* get most frequent terms for all documents
     * get total number of words in all documents: total
     * for each term:
     * 	compute how many times it appears in each document: freq
     * 	compute freq/total -> Prob
     * When the loop is done, normalize and output
     */

    Map<String, Integer> allTerms = Maps.newTreeMap();
    int allWordCounts = 0;

    //Loop over documents to get all the terms and count total occurence of the term in all the documents
    for(ScoredDocument singleDoc : scoredDocs) {
      DocumentIndexed docIndexed = (DocumentIndexed)(_indexer.getDoc(singleDoc.getDocId()));
      Map<String, Integer> docTerms = _indexer.wordListWithoutStopwordsForDoc(docIndexed._docid);

      //As we're looping, we can compute the total words of all the documents
      //This is the denominator of the probability
      allWordCounts += docIndexed.getDocumentSize();

      for(String word : docTerms.keySet()) {
        int totalCount = allTerms.containsKey(word) ? allTerms.get(word) : 0;
        totalCount += docTerms.get(word);
        allTerms.put(word, totalCount);
      }
    }

    List<Map.Entry<String, Integer>> allTermsList = Utils.sortByValues(allTerms, true);

    Map<String, Double> probabilities = Maps.newHashMap();
    double total = 0.0;

    // Loop over the top m terms based on the numTerms param specified in the url
    int termsToLoopOver = allTermsList.size() > numTerms ? numTerms : allTermsList.size();
    for(int i = 0; i < termsToLoopOver; i++) {
      String word = allTermsList.get(i).getKey();
      double count = allTermsList.get(i).getValue();

      // probability = total count of term in all docs / total count of all words in all docs
      double probability = count / allWordCounts;

      // Add value to total to use for normalization
      total += probability;

      // Add value to probabilities list
      probabilities.put(word, probability);
    }

    List<Map.Entry<String, Double>> probabilitiesList = Utils.sortByValues(probabilities, true);
    List<Map.Entry<String, Double>> normalizedProbabilities = Lists.newArrayList();

    // Loop over each value in the list and output formatted normalized result
    for(Map.Entry<String, Double> entry : probabilitiesList) {
      double normalizedProbability = entry.getValue() / total;
      entry.setValue(normalizedProbability);
      normalizedProbabilities.add(entry);
    }

    return normalizedProbabilities;
  }
}
