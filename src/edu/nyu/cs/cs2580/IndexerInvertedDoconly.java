package edu.nyu.cs.cs2580;

import de.l3s.boilerpipe.BoilerpipeProcessingException;

import java.io.IOException;
import java.util.*;

/**
 * Created by andreidinuionita on 10/17/14.
 */
public class IndexerInvertedDoconly extends IndexerInverted {

  private Map<Integer, Integer> _utilityCorpusDocFrequencyByTerm = new HashMap<Integer, Integer>();

  public IndexerInvertedDoconly(SearchEngine.Options options) {
    super(options);
  }

  protected void updatePostingsLists(int docId, Vector<Integer> docTokensAsIntegers) throws IOException {
    Set<Integer> uniqueTokens = new HashSet<Integer>();  // unique term ID
    uniqueTokens.addAll(docTokensAsIntegers);

    // Indexing
    for(Integer term : uniqueTokens) {
      if(!_utilityIndex.containsKey(term)) {
        _utilityIndex.put(term, new LinkedList<Integer>());
      }
      incrementCorpusDocFrequencyForTerm(term);
      _utilityIndex.get(term).add(docId);
      _utilityIndexFlatSize++;

      if(_utilityIndexFlatSize > UTILITY_INDEX_FLAT_SIZE_THRESHOLD) {
        String filePath = _options._indexPrefix + WORDS_DIR + "/" + _utilityPartialIndexCounter++;
        dumpUtilityIndexToFileAndClearFromMemory(filePath);
      }
    }
  }

  @Override
  protected int nextPhrase(List<String> phraseTokens, int docid) throws IOException {
    if(phraseTokens.size() == 1) {
      return next(phraseTokens.get(0), docid);
    }
    System.out.println("This indexer does not support query phrases.");

    List<Integer> docIDs = new ArrayList<Integer>();  // a list containing doc ID for each term in the phrase
    for(String token : phraseTokens) {
      int nextDocID = next(token, docid);
      if(nextDocID == -1) return -1;
      docIDs.add(nextDocID);
    }

    boolean found = false;

    while(!found) {
      // Get maximum docId for all tokens.
      int maxDocId = Integer.MIN_VALUE;
      int maxDocIdIndex = -1;
      for(int pos = 0; pos < docIDs.size(); pos++) {
        if(docIDs.get(pos) > maxDocId) {
          maxDocId = docIDs.get(pos);
          maxDocIdIndex = pos;
        }
      }

      for(int pos = 0; pos < docIDs.size(); pos++) {
        if(docIDs.get(pos) < maxDocId) {
          // Get next docId after or equal to the max general docId.
          int docIdNew = next(phraseTokens.get(pos), maxDocId - 1);
          if (docIdNew < 0) return -1;

          // Set this to new docId for that token.
          docIDs.set(pos, docIdNew);
        }
      }

      // Check if the docIds are all equal.
      found = true;
      for(int pos = 1; pos < docIDs.size(); pos++) {
        if(!docIDs.get(pos - 1).equals(docIDs.get(pos))) {  // careful with Integer unboxing
          found = false;
          break;
        }
      }
    }
    return docIDs.get(0);
  }

  // Just like in the lecture slide 3, page 14, this helper method returns the next document id
  // after the given docid. It returns -1 if not found.
  @Override
  protected int next(String term, int docid) throws IOException {
    if(!_dictionary.containsKey(term)) {
      return -1;
    }

    int termInt = _dictionary.get(term);  // an integer representation of a term
    List<Integer> postingList = postingsListForWord(termInt);

    for(int i=0; i < postingList.size(); i++){
      if(postingList.get(i) > docid){
        return postingList.get(i);
      }
    }

    return -1;
  }

  @Override
  public int corpusDocFrequencyByTerm(String term) {
    try {
      if (_dictionary.containsKey(term)) {
        int termInt = _dictionary.get(term);  // an integer representation of a term
        if(_utilityCorpusDocFrequencyByTerm.containsKey(termInt)) {
          // during construction phase, the posting list is not available.
          return _utilityCorpusDocFrequencyByTerm.get(termInt);
        }
        List<Integer> postingList = postingsListForWord(termInt);
        return postingList.size();
      }
    } catch (IOException e) {}
    return 0;
  }

  private void incrementCorpusDocFrequencyForTerm(int word) {
    if(_utilityCorpusDocFrequencyByTerm.containsKey(word)) {
      _utilityCorpusDocFrequencyByTerm.put(word, _utilityCorpusDocFrequencyByTerm.get(word) + 1);
    } else {
      _utilityCorpusDocFrequencyByTerm.put(word, 1);
    }
  }

  @Override
  public int corpusTermFrequency(String term) {
    if(_dictionary.containsKey(term)) {
      int termInt = _dictionary.get(term);  // an integer representation of a term
      return _termCorpusFrequency.get(termInt);
    }
    return 0;
  }

  // Need not be implemented because the information is not available in the index.
  @Override
  public int documentTermFrequency(String term, String url) {
    SearchEngine.Check(false, "Not implemented!");
    return 0;
  }
}
