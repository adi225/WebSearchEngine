package edu.nyu.cs.cs2580;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.Serializable;
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
    return null;
  }

  /**
   * In HW2, you should be using {@link DocumentIndexed}.
   */
  @Override
  public Document nextDoc(Query query, int docid) {
    return null;
  }

  @Override
  public int corpusDocFrequencyByTerm(String term) {
    return 0;
  }

  @Override
  public int corpusTermFrequency(String term) {
    return 0;
  }

  @Override
  public int documentTermFrequency(String term, String url) {
    SearchEngine.Check(false, "Not implemented!");
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
