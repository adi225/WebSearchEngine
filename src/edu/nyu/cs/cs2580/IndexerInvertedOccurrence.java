package edu.nyu.cs.cs2580;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.Vector;

import de.l3s.boilerpipe.BoilerpipeProcessingException;
import de.l3s.boilerpipe.extractors.ArticleExtractor;

import edu.nyu.cs.cs2580.FileUtils.FileRange;
import edu.nyu.cs.cs2580.SearchEngine.Options;

/**
 * @CS2580: Implement this class for HW2.
 */
public class IndexerInvertedOccurrence extends Indexer implements Serializable{

  private static final long serialVersionUID = 1077111905740085030L;
  private static final String WORDS_DIR = "/partials";
  private static final long UTILITY_INDEX_FLAT_SIZE_THRESHOLD = 1000000;

  private RandomAccessFile _indexRAF;

  // Utility index is only used during index construction.
  private Map<Integer, List<Integer>> _utilityIndex = new HashMap<Integer, List<Integer>>();
  private long _utilityIndexFlatSize = 0;
  private long _utilityPartialIndexCounter = 0;

  // An index, which is a mapping between an integer representation of a term
  // and a byte range in the file where the postings list for the term is located.
  private Map<Integer, FileRange> _index = new HashMap<Integer, FileRange>();

  // An offset in the file where the postings lists begin (after all metadata).
  private long _indexOffset = 0;
	
  // Metadata of documents.
  private Vector<DocumentIndexed> _documents = new Vector<DocumentIndexed>();
  
  // Maps each term to its integer representation
  private Map<String, Integer> _dictionary = new HashMap<String, Integer>();

  // All unique terms appeared in corpus. Offsets are integer representations.
  private Vector<String> _terms = new Vector<String>();

  // Term frequency, key is the integer representation of the term and value is
  // the number of times the term appears in the corpus.
  private Map<Integer, Integer> _termCorpusFrequency = new HashMap<Integer, Integer>();
	
  public IndexerInvertedOccurrence(Options options) {
    super(options);
    System.out.println("Using Indexer: " + this.getClass().getSimpleName());
  }

  @Override
  public void constructIndex() throws IOException {
  }

  //   TODO No stop word is removed, you need to dynamically determine whether to drop the processing of a certain inverted list.
  
  // The input of this method (String text) is the raw context of the document.
  public void processDocument(int docId, String text) throws IOException, BoilerpipeProcessingException {
    text = removeNonVisibleContext(text);  // step 1 of document processing
    text = removePunctuation(text).toLowerCase();
    text = performStemming(text);  // step 2 of document processing

    Vector<Integer> docTokensAsIntegers = readTermVector(text);

    Set<Integer> uniqueTokens = new HashSet<Integer>();  // unique term ID
    uniqueTokens.addAll(docTokensAsIntegers);
    _documents.get(docId).setUniqueBodyTokens(uniqueTokens);  // setting the unique tokens for a document

    // indexing
    Map<Integer,Integer> existingTokens = new HashMap<Integer,Integer>();  // this set is to keep track of the existing term, key is term id and value is the number of occurrence
    
    for(int position=0; position < docTokensAsIntegers.size(); position++){
    	int termId = docTokensAsIntegers.get(position);
    	if(!existingTokens.containsKey(termId)){
    	  if(!_utilityIndex.containsKey(termId)){
    	    _utilityIndex.put(termId, new LinkedList<Integer>());    		  
    	  }
    	  List<Integer> postingList = _utilityIndex.get(termId);
    	  postingList.add(docId);  // add docId to the end of the list followed by the occurrence and the position
    	  postingList.add(1);  // first time that this termId appears in the current docId, so the occurrence is 1
    	  postingList.add(position);  // appending position to the posting list
    	  existingTokens.put(termId, 1);
    	}
    	else{ // the posting list already contains the termId
    	  List<Integer> postingList = _utilityIndex.get(termId);
    	  int occurrence = existingTokens.get(termId);
    	  int occurrenceIndex = postingList.size()-occurrence-1; // the index that needs to get updated
    	  postingList.set(occurrenceIndex, postingList.get(occurrenceIndex)+1);  // updating the occurrence (+1)
    	  postingList.add(position);  // appending position to the posting list
    	  existingTokens.put(termId, existingTokens.get(termId)+1);
    	}
    	
        _utilityIndexFlatSize++;

        if(_utilityIndexFlatSize > UTILITY_INDEX_FLAT_SIZE_THRESHOLD) {
          String filePath = _options._indexPrefix + WORDS_DIR + "/" + _utilityPartialIndexCounter++;
          dumpUtilityIndexToFileAndClearFromMemory(filePath);
        }
    }
    
    System.out.println("Finished indexing document id: " + docId);
  }
  
  // Non-visible page content is removed, e.g., those inside <script> tags.
  // Right now, the 3rd party library "BoilerPiper" is used to perform the task.
  public String removeNonVisibleContext(String text) throws BoilerpipeProcessingException {
	return ArticleExtractor.INSTANCE.getText(text);
  }

  public String removePunctuation(String text) {
    return text.replaceAll("[^a-zA-Z0-9\n]", " ");
    // TODO Treat abbreviation specially (I.B.M.)
    // TODO Think about how to treat hyphen. Ex: peer-to-peer, live-action, 978-0-06-192691-4, 1998-2002
    // TODO Think about accented characters.
  }
  
  // Tokens are stemmed with Step 1 of the Porter's algorithm.
  public String performStemming(String text){
	  return text;
  }
  
  /**
   * Tokenize {@code content} into terms, translate terms into their integer
   * representation, store the integers in {@code tokens}.
   * @param content
   */
  private Vector<Integer> readTermVector(String content) {
    Vector<Integer> tokens = new Vector<Integer>();
    Scanner s = new Scanner(content);  // Uses white space by default.
    while (s.hasNext()) {
      String token = s.next();
      int idx = -1;
      if (_dictionary.containsKey(token)) {
        idx = _dictionary.get(token);
        _termCorpusFrequency.put(idx, _termCorpusFrequency.get(idx) + 1);
      } else {
        idx = _terms.size();  // offsets are the integer representations
        idx = _dictionary.keySet().size();
        // TODO Do we need _terms? Isn't it equal to the set of keys in _dictionary?
        // Need _terms in order to convert the integer representation back to String
        _terms.add(token);
        _dictionary.put(token, idx);
        _termCorpusFrequency.put(idx, 1);
      }
      tokens.add(idx);
    }
    _totalTermFrequency += tokens.size();
    return tokens;
  }
  
  @Override
  public void loadIndex() throws IOException, ClassNotFoundException {
  }

  @Override
  public Document getDoc(int docid) {
	  return (docid >= _documents.size() || docid < 0) ? null : _documents.get(docid);
  }

  /**
   * In HW2, you should be using {@link DocumentIndexed}.
   */
  @Override
  public Document nextDoc(Query query, int docid) {
	// 3 cases to handle:
	// 1.) query containing only conjunctive terms (easily dealt with nextDocConjunctive)
	// 2.) query containing only the phrase portion
	// 3.) query containing both conjunctive and phrase parts
    return null;
  }
  
  // This method returns the next docid after the given docid that contains all terms in the query conjunctive.
  // This is equivalent to the nextDoc method in the IndexerInvertedDoconly class.
  public Document nextDocConjunctive(Query query, int docid){
	// query is already processed before getting passed into this method	
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
  public int next(String term, int docid) throws IOException {
	  if(!_dictionary.containsKey(term)) {
		  return -1;
	  }
	    
	  int termInt = _dictionary.get(term);  // an integer representation of a term
	  List<Integer> postingList = postingsListForWord(termInt);
	  
	  int occurrenceIndex = 1;  // the first index of occurrence position in the list
	  while(occurrenceIndex < postingList.size()){
		  int docIndex = occurrenceIndex - 1;
		  if(postingList.get(docIndex) > docid){
			  return postingList.get(docIndex);
		  }
		  int occurrence = postingList.get(occurrenceIndex);
		  occurrenceIndex += occurrence + 2;  // jump to the next occurrence position
	  }
  
	  return -1;
  }
  
  // Lecture 3 slide, page 23
  // This method returns the next position of the phrase after pos within the docid.
  public int nextPhrase(QueryPhrase queryPhrase, int docid, int pos){
	  // need to pass the phrase portion into the query
	  Query query = new Query(""); // put the phrase information here
	  query.processQuery();
	  Document docVerify = nextDocConjunctive(query, docid-1);
	  if(docVerify == null){
		  return -1;  // if the document does not contain all the terms, then there is certainly no phrase
	  }
	  
	  List<Integer> positions = new ArrayList<Integer>();
	  int maxPosition = Integer.MIN_VALUE;
	  for(int i=0; i<query._tokens.size(); i++){
		  int termPosition = nextPosition(query._tokens.get(i), docid, pos);
		  if(termPosition == -1){
			  return -1;
		  }
		  maxPosition = Math.max(maxPosition, termPosition);
		  positions.add(termPosition);
	  }
	  
	  boolean foundPhrase = true;
	  for(int i=1; i<positions.size(); i++){
		  if(positions.get(i) != positions.get(i-1) + 1){
			  foundPhrase = false;
			  break;
		  }
	  }
	  
	  if(foundPhrase){
		  positions.get(0);
	  }
	  return nextPhrase(queryPhrase, docid, maxPosition);
  }
  
  // This method returns the next occurrence of the term in docid after pos
  public int nextPosition(String term, int docid, int pos){
	  try{
		  int termInt = _dictionary.get(term);  // an integer representation of a term
		  
		  List<Integer> postingList = postingsListForWord(termInt);
		  int occurrenceIndex = 1;  // the first index of occurrence position in the list
		  while(occurrenceIndex < postingList.size()){
			  int docIndex = occurrenceIndex - 1;
			  if(postingList.get(docIndex) > docid){
				  return -1;
			  }
			  int occurrence = postingList.get(occurrenceIndex);
			  if(postingList.get(docIndex) == docid){  // found the docid
				// iterating through the positions of docid
				for(int posIndex=occurrenceIndex+1; posIndex < occurrenceIndex+1+occurrence; posIndex++){
					if(postingList.get(posIndex) > pos){
						return postingList.get(posIndex);
					}
				}
				return -1;
			  }
			  occurrenceIndex += occurrence + 2;  // jump to the next occurrence position
		  }
		  
		  return -1; // not found
	  }
	  catch(Exception e){
		  return -1;
	  }
  }

  @Override
  public int corpusDocFrequencyByTerm(String term) {
	  try{
		  int termInt = _dictionary.get(term);  // an integer representation of a term
		  
		  List<Integer> postingList = postingsListForWord(termInt);
		  int docCount = 0;
		  int occurrenceIndex = 1;  // the first index of occurrence position in the list
		  while(occurrenceIndex < postingList.size()){
			  int occurrence = postingList.get(occurrenceIndex);
			  docCount++;
			  occurrenceIndex += occurrence + 2;  // jump to the next occurrence position
		  }
		  
		  return docCount;
	  }
	  catch(Exception e){
		  return 0;
	  }
  }

  @Override
  public int corpusTermFrequency(String term) {
	  try{
		  int termInt = _dictionary.get(term);  // an integer representation of a term
		  
		  List<Integer> postingList = postingsListForWord(termInt);
		  int termFrequency = 0;
		  int occurrenceIndex = 1;  // the first index of occurrence position in the list
		  while(occurrenceIndex < postingList.size()){
			  int occurrence = postingList.get(occurrenceIndex);
			  termFrequency += occurrence;
			  occurrenceIndex += occurrence + 2;  // jump to the next occurrence position
		  }
		  
		  return termFrequency;
	  }
	  catch(Exception e){
		  return 0;
	  }
  }

  @Override
  public int documentTermFrequency(String term, String url) {
    //SearchEngine.Check(_dictionary.containsKey(term), "The term "+term+" does not exist in corpus.");
    
    int docId = 0;  // get docid from url here, should we have a mapping from url to docid?
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
		
	} catch (Exception e) {
		return 0;
	}
    
    return 0;
  }
  
  // This method may be deprecated in later versions. Use with caution!
  private List<Integer> postingsListForWord(int word) throws IOException {
    List<Integer> postingsList = new LinkedList<Integer>();
    FileRange fileRange = _index.get(word);
    _indexRAF.seek(_indexOffset + fileRange.offset);
    for(int i = 0; i < fileRange.length; i++) {
      postingsList.add(_indexRAF.readInt());
    }
    return postingsList;
  }

  private void dumpUtilityIndexToFileAndClearFromMemory(String filePath) throws IOException {
    FileUtils.dumpIndexToFile(_utilityIndex, new File(filePath));
    _utilityIndex = new HashMap<Integer, List<Integer>>();
    _utilityIndexFlatSize = 0;
    System.gc();
  }
}
