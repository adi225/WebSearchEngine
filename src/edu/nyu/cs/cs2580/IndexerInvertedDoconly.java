package edu.nyu.cs.cs2580;

import de.l3s.boilerpipe.BoilerpipeProcessingException;

import java.io.IOException;
import java.util.*;

/**
 * Created by andreidinuionita on 10/17/14.
 */
public class IndexerInvertedDoconly extends IndexerInverted {

  public IndexerInvertedDoconly(SearchEngine.Options options) {
    super(options);
    System.out.println("Using Indexer: " + this.getClass().getSimpleName());
  }

  // The input of this method (String text) is the raw context of the document.
  @Override
  public void processDocument(int docId, String text) throws IOException, BoilerpipeProcessingException {
    text = removeNonVisibleContext(text);  // step 1 of document processing
    text = removePunctuation(text).toLowerCase();
    text = performStemming(text);  // step 2 of document processing

    Vector<Integer> docTokensAsIntegers = readTermVector(text);

    Set<Integer> uniqueTokens = new HashSet<Integer>();  // unique term ID
    uniqueTokens.addAll(docTokensAsIntegers);
    //_documents.get(docId).setUniqueBodyTokens(uniqueTokens);  // setting the unique tokens for a document

    // Indexing
    for(Integer term : uniqueTokens) {
      if(!_utilityIndex.containsKey(term)) {
        _utilityIndex.put(term, new LinkedList<Integer>());
      }
      _utilityIndex.get(term).add(docId);
      _utilityIndexFlatSize++;

      if(_utilityIndexFlatSize > UTILITY_INDEX_FLAT_SIZE_THRESHOLD) {
        String filePath = _options._indexPrefix + WORDS_DIR + "/" + _utilityPartialIndexCounter++;
        dumpUtilityIndexToFileAndClearFromMemory(filePath);
      }
    }
    System.out.println("Finished indexing document id: " + docId);
  }

  /**
   * In HW2, you should be using {@link DocumentIndexed}
   * @throws IOException
   */
  // This implementation follows that in the lecture 3 slide, page 13.
  @Override
  public Document nextDoc(Query query, int docid) {
    // Assuming that the query has already been processed.
    // query.processQuery();
    try {
      List<Integer> docIDs = new ArrayList<Integer>();  // a list containing doc ID for each term in the query
      for(String token : query._tokens){
        int docID = next(token,docid);
        if(docID == -1){
          return null;
        }
        docIDs.add(docID);
      }

      boolean foundDocID = true;
      int docIDFixed = docIDs.get(0);
      int docIDNew = Integer.MIN_VALUE;

      for(Integer docID : docIDs){  // check if all the doc IDs are equal
        if(docID != docIDFixed){
          foundDocID = false;
        }
        if(docID > docIDNew){
          docIDNew = docID;
        }
      }

      if(foundDocID){
        return _documents.get(docIDFixed);
      }

      return nextDoc(query,docIDNew-1);
    } catch (IOException e) {
      return null;
    }
  }

  // Just like in the lecture slide 3, page 14, this helper method returns the next document id
  // after the given docid. It returns -1 if not found.
  @Override
  public int next(String term, int docid) throws IOException {
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
    try{
      int termInt = _dictionary.get(term);  // an integer representation of a term

      List<Integer> postingList = postingsListForWord(termInt);

      return postingList.size();
    }
    catch(Exception e){
      return 0;
    }
  }

  @Override
  public int corpusTermFrequency(String term) {
    try{
      int termInt = _dictionary.get(term);  // an integer representation of a term
      return _termCorpusFrequency.get(termInt);
    }
    catch(NullPointerException e){
      return 0;
    }
  }

  // Need not be implemented because the information is not available in the index.
  @Override
  public int documentTermFrequency(String term, String url) {
    SearchEngine.Check(false, "Not implemented!");
    return 0;
  }

  // This method may be deprecated in later versions. Use with caution!
  @Override
  protected List<Integer> postingsListForWord(int word) throws IOException {
    List<Integer> postingsList = new LinkedList<Integer>();
    FileUtils.FileRange fileRange = _index.get(word);
    _indexRAF.seek(_indexOffset + fileRange.offset);
    for(int i = 0; i < fileRange.length / 4; i++) {
      postingsList.add(_indexRAF.readInt());
    }
    return postingsList;
  }

}
