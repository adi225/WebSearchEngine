package edu.nyu.cs.cs2580;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

import com.google.common.collect.BiMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import edu.nyu.cs.cs2580.SearchEngine.Options;

/**
 * @CS2580: Implement this class for HW2.
 */
public class IndexerInvertedOccurrence extends IndexerInverted implements Serializable {

  private static final long serialVersionUID = 1077111905740085030L;

  protected Map<Integer, Integer> _corpusDocFrequencyByTerm = new HashMap<Integer, Integer>();
	
  public IndexerInvertedOccurrence(Options options) {
    super(options);
  }

  @Override
  public void constructIndex() throws IOException {
    _corpusDocFrequencyByTerm = new HashMap<Integer, Integer>();
    super.constructIndex();
  }

  @Override
  protected List<Object> selectMetadataToStore() {
    List<Object> indexMetadata = super.selectMetadataToStore();
    indexMetadata.add(_corpusDocFrequencyByTerm);
    return indexMetadata;
  }

  @Override
  protected void setLoadedMetadata(List<Object> indexMetadata) {
    super.setLoadedMetadata(indexMetadata);
    _corpusDocFrequencyByTerm = (Map<Integer, Integer>)indexMetadata.get(indexMetadata.size() - 1);
  }

  protected void incrementCorpusDocFrequencyForTerm(int word) {
    if(_corpusDocFrequencyByTerm.containsKey(word)) {
      _corpusDocFrequencyByTerm.put(word, _corpusDocFrequencyByTerm.get(word) + 1);
    } else {
      _corpusDocFrequencyByTerm.put(word, 1);
    }
  }

  protected void updatePostingsLists(int docId, Vector<Integer> docTokensAsIntegers) throws IOException {
    // Indexing
    Map<Integer, List<Integer>> occurences = new HashMap<Integer,List<Integer>>();

    for(int position = 0; position < docTokensAsIntegers.size(); position++) {
      int word = docTokensAsIntegers.get(position);
      if (!occurences.containsKey(word)) {
        occurences.put(word, new LinkedList<Integer>());
      }
      List<Integer> occurancesList = occurences.get(word);
      occurancesList.add(position);
    }

    for(int word : occurences.keySet()) {
      if(!_utilityIndex.containsKey(word)) {
        _utilityIndex.put(word, new LinkedList<Integer>());
      }
      List<Integer> postingList = _utilityIndex.get(word);
      List<Integer> occurancesList = occurences.get(word);

      postingList.add(docId);
      incrementCorpusDocFrequencyForTerm(word);
      postingList.add(occurancesList.size());
      postingList.addAll(occurancesList);

      _utilityIndexFlatSize += occurancesList.size() + 2;
      occurences.put(word, null);

      if(_utilityIndexFlatSize > UTILITY_INDEX_FLAT_SIZE_THRESHOLD) {
        String filePath = _options._indexPrefix + WORDS_DIR + "/" + _utilityPartialIndexCounter++;
        dumpUtilityIndexToFileAndClearFromMemory(filePath);
      }
    }
  }

  /**
   * This helper method returns the next docid after the given docid in the term's postings list.
   * It returns -1 if not found.
   * Works just like in the lecture slide 3, page 14.
   */
  protected int next(String term, int docid) throws IOException {
    if(!_dictionary.containsKey(term)) {
      return -1;
    }

    int termInt = _dictionary.get(term);  // an integer representation of a term
    List<Integer> postingList = postingsListForWord(termInt);

    int occurrenceIndex = 1;  // the first index of occurrence position in the list
    while(occurrenceIndex < postingList.size()) {
      int docIndex = occurrenceIndex - 1;
      if(postingList.get(docIndex) > docid) {
        return postingList.get(docIndex);
      }
      int occurrence = postingList.get(occurrenceIndex);
      occurrenceIndex += occurrence + 2;  // jump to the next occurrence position
    }
    return -1;
  }

  /**
   *  This helper method returns the next docid after the given docid for this phrase (series of consecutive terms)
   */
  protected int nextPhrase(List<String> phraseTokens, int docid) throws IOException {
    if(phraseTokens.size() == 1) {
      return next(phraseTokens.get(0), docid);
    }

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

      if(found) {
        // Check that occurrences are consecutive.
        boolean areConsecutive = false;

        // Use a cache so we don't have to reload the occurances set every time.
        Map<Integer, Set<Integer>> occurancesCache = new HashMap<Integer, Set<Integer>>();

        // Go through all elem of first list and check if the other lists have the required consecutive elems.
        Set<Integer> firstList = getOccurancesForDoc(phraseTokens.get(0), docIDs.get(0));
        for(int firstListOccurance : firstList) {
          int listIndex;
          for(listIndex = 1; listIndex < phraseTokens.size(); listIndex++) {
            Set<Integer> otherList;
            if(!occurancesCache.containsKey(listIndex)) {
              otherList = getOccurancesForDoc(phraseTokens.get(listIndex), docIDs.get(0));
              occurancesCache.put(listIndex, otherList);
            } else {
              otherList = occurancesCache.get(listIndex);
            }

            if(!otherList.contains(firstListOccurance + listIndex)) {
              break;
            }
          }
          if(listIndex == phraseTokens.size()) {
            areConsecutive = true;
            break;
          }
        }
        if(!areConsecutive) {
          found = false;
          int commonButNonConsecutiveDocId = docIDs.get(0);
          docIDs.clear();
          for(String token : phraseTokens) {
            int nextDocID = next(token, commonButNonConsecutiveDocId);
            if(nextDocID == -1) return -1;
            docIDs.add(nextDocID);
          }
        }
      }
    }
    return docIDs.get(0);
  }
  
  // This method returns the next occurrence of the term in docid after pos
  protected int nextPosition(String term, int docid, int pos) {
    if(!_dictionary.containsKey(term)) {
      return -1;
    }
    try {
      int termInt = _dictionary.get(term);  // an integer representation of a term
      List<Integer> postingList = postingsListForWord(termInt);
      int occurrenceIndex = 1;  // the first index of occurrence position in the list
      while (occurrenceIndex < postingList.size()) {
        int docIndex = occurrenceIndex - 1;
        if (postingList.get(docIndex) > docid) {
          return -1;
        }
        int occurrence = postingList.get(occurrenceIndex);
        if (postingList.get(docIndex) == docid) {  // found the docid
          // iterating through the positions of docid
          for (int posIndex = occurrenceIndex + 1; posIndex < occurrenceIndex + 1 + occurrence; posIndex++) {
            if (postingList.get(posIndex) > pos) {
              return postingList.get(posIndex);
            }
          }
          return -1;
        }
        occurrenceIndex += occurrence + 2;  // jump to the next occurrence position
      }
    } catch (IOException e) {}
    return -1; // not found
  }

  protected Set<Integer> getOccurancesForDoc(String term, int docId) throws IOException {
    if(!_dictionary.containsKey(term)) {
      return null;
    }

    int termInt = _dictionary.get(term);  // an integer representation of a term
    List<Integer> postingList = postingsListForWord(termInt);

    int occurrenceIndex = 1;  // the first index of occurrence position in the list
    while(occurrenceIndex < postingList.size()) {
      int docIndex = occurrenceIndex - 1;
      if(postingList.get(docIndex) == docId) {
        Set<Integer> result = Sets.newHashSet();
        int occurrence = postingList.get(occurrenceIndex);
        for(int i = occurrenceIndex + 1; i <= occurrenceIndex + occurrence; i++) {
          result.add(postingList.get(i));
        }
        return result;
      } else if(postingList.get(docIndex) > docId) {
        return null;
      }
      int occurrence = postingList.get(occurrenceIndex);
      occurrenceIndex += occurrence + 2;  // jump to the next occurrence position
    }
    return null;
  }

  @Override
  public int corpusDocFrequencyByTerm(String term) {
    if(_dictionary.containsKey(term)) {
      int word = _dictionary.get(term);
      return _corpusDocFrequencyByTerm.get(word);
    }
    return 0;
  }

  @Override
  public int corpusTermFrequency(String term) {
    if(_dictionary.containsKey(term)) {
      int word = _dictionary.get(term);  // an integer representation of a term
      return _termCorpusFrequency.get(word);
    }
    return 0;
  }

  @Override
  public int documentTermFrequency(String term, String url) {
    // TODO get docid from url here, should we have a mapping from url to docid?
    int docId = mapUrlToDocId(url);
    if(docId < 0 || !_dictionary.containsKey(term)) return 0;
    int termId = _dictionary.get(term);
    
    try {
      List<Integer> postingList = postingsListForWord(termId);
      int termFrequency = 0;
      int occurrenceIndex = 1;  // the first index of occurrence position in the list
      while(occurrenceIndex < postingList.size()){
        int docIndex = occurrenceIndex - 1;
        if(postingList.get(docIndex) > docId){
          return 0;
        }
        int occurrence = postingList.get(occurrenceIndex);
        if(postingList.get(docIndex) == docId){
          return occurrence;
        }
        occurrenceIndex += occurrence + 2;  // jump to the next occurrence position
      }
	} catch (IOException e) {}
    return 0;
  }
  
  // This method returns the corresponding docid of the given url.
  public int mapUrlToDocId(String url) {
    for(Document doc : _documents){
      if(doc.getUrl().equals(url)){
        return doc._docid;
      }
    }
    return -1;
  }
}
