package edu.nyu.cs.cs2580;

import java.io.*;
import java.util.*;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import de.l3s.boilerpipe.BoilerpipeProcessingException;
import de.l3s.boilerpipe.extractors.ArticleExtractor;

import edu.nyu.cs.cs2580.SearchEngine.Options;
import edu.nyu.cs.cs2580.FileUtils.FileRange;

/**
 * @CS2580: Implement this class for HW2.
 */
public class IndexerInvertedDoconly extends Indexer implements Serializable{

  private static final long serialVersionUID = 1077111905740085030L;
  private static final String WORDS_DIR = "/.partials";
  private static final long UTILITY_INDEX_FLAT_SIZE_THRESHOLD = 1000000;

  private RandomAccessFile _indexRAF;
  private final String indexFilePath = _options._indexPrefix + "/index.idx";

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
  private BiMap<String, Integer> _dictionary = HashBiMap.create();

  // Term frequency, key is the integer representation of the term and value is
  // the number of times the term appears in the corpus.
  private Map<Integer, Integer> _termCorpusFrequency = new HashMap<Integer, Integer>();

  public IndexerInvertedDoconly(Options options) {
    super(options);
    System.out.println("Using Indexer: " + this.getClass().getSimpleName());
  }

  @Override
  public void constructIndex() throws IOException {
	System.out.println("Construct index from: " + _options._corpusPrefix);
    File indexFile = new File(indexFilePath);
    File indexAuxFile = new File(indexFile.getAbsolutePath() + "_aux");
    if(indexFile.exists()) {
      indexFile.delete();
      indexFile = new File(indexFilePath);
    }
    if(indexAuxFile.exists()) {
      indexAuxFile.delete();
      indexAuxFile = new File(indexFile.getAbsolutePath() + "_aux");
    }
    new File(_options._indexPrefix + WORDS_DIR).mkdir();
    File dir = new File(_options._corpusPrefix);
    File[] directoryListing = dir.listFiles();

    if (directoryListing != null) {
      for (File docFile : directoryListing) {
        StringBuffer text = new StringBuffer();  // the original text of the document

        // getting the original text of the document
        BufferedReader reader = new BufferedReader(new FileReader(docFile));
        String line = null;
        while ((line = reader.readLine()) != null) {
          text.append(line + "\n");
        }
        reader.close();

        // adding an indexed document
        int docId = _numDocs++;     // the current number of doc is ID for the current document
        DocumentIndexed docIndexed = new DocumentIndexed(docId);
        docIndexed.setTitle(docFile.getName());
        docIndexed.setUrl(docFile.getAbsolutePath());
        _documents.add(docIndexed);

        try {
          processDocument(docId, text.toString());  // process the raw context of the document
        } catch (BoilerpipeProcessingException e) {
          throw new IOException("File format could not be processed by Boilerplate.");
        }
      }

      // dump any leftover partial index
      if(_utilityIndexFlatSize > 0) {
        dumpUtilityIndexToFileAndClearFromMemory(
                _options._indexPrefix + WORDS_DIR + "/" + _utilityPartialIndexCounter++);
      }
    } else {
        throw new IOException("Invalid directory.");
    }

    // Merge all partial indexes.
    System.out.println("Generated " + _utilityPartialIndexCounter + " partial indexes.");
    String filePathBase = _options._indexPrefix + WORDS_DIR + "/";
    String filePath1 =  filePathBase + 0;
    for(int i = 1; i < _utilityPartialIndexCounter; i++) {
      String filePath2 = filePathBase + i;
      Map<Integer, FileRange> tempIndex = new HashMap<Integer, FileRange>();
      String resFilePath = filePath1 + i;
      System.gc();
      System.out.println("Merging file #" + i);
      long offset = FileUtils.mergeFilesIntoIndexAndFile(filePath1, filePath2, tempIndex, resFilePath);
      filePath1 = resFilePath;
      if(i == _utilityPartialIndexCounter - 1) {
        _index = tempIndex;
        _indexOffset = offset;
        new File(resFilePath).renameTo(indexAuxFile);
        new File(_options._indexPrefix + WORDS_DIR).delete();
      }
    }
    System.out.println("Done merging files.");

    // Add metadata to file.
    List<Object> indexMetadata = new ArrayList<Object>();
    indexMetadata.add(_documents);
    indexMetadata.add(_dictionary);
    indexMetadata.add(_termCorpusFrequency);

    FileUtils.writeObjectsToFile(indexMetadata, indexFile);
    FileUtils.appendFileToFile(indexAuxFile, indexFile);

    System.out.println(
            "Indexed " + Integer.toString(_numDocs) + " docs with " +
                    Long.toString(_totalTermFrequency) + " terms.");
  }

  @Override
  public void loadIndex() throws IOException, ClassNotFoundException {
    File indexFile = new File(indexFilePath);
    List<Object> indexMetadata = new ArrayList<Object>();
    DataInputStream indexFileDIS = new DataInputStream(new FileInputStream(indexFile));
    long bytesRead = FileUtils.readObjectsFromFileIntoList(indexFileDIS, indexMetadata);
    _documents           = (Vector<DocumentIndexed>)indexMetadata.get(0);
    _dictionary          = (BiMap<String, Integer>)indexMetadata.get(1);
    _termCorpusFrequency = (Map<Integer, Integer>)indexMetadata.get(2);
    bytesRead += FileUtils.loadFromFileIntoIndex(indexFileDIS, _index);
    indexFileDIS.close();
    _indexOffset = bytesRead;
    _indexRAF = new RandomAccessFile(indexFile, "r");
    _indexRAF.seek(_indexOffset);
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
        idx = _dictionary.keySet().size();
        _dictionary.put(token, idx);
        _termCorpusFrequency.put(idx, 1);
      }
      tokens.add(idx);
    }
    _totalTermFrequency += tokens.size();
    return tokens;
  }

  @Override
  public DocumentIndexed getDoc(int docid) {
    return (docid >= _documents.size() || docid < 0) ? null : _documents.get(docid);
  }

  /**
   * In HW2, you should be using {@link DocumentIndexed}
 * @throws IOException 
   */
  @Override
  // This implementation follows that in the lecture 3 slide, page 13.
  public DocumentIndexed nextDoc(Query query, int docid) {
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
  private List<Integer> postingsListForWord(int word) throws IOException {
    List<Integer> postingsList = new LinkedList<Integer>();
    FileRange fileRange = _index.get(word);
    _indexRAF.seek(_indexOffset + fileRange.offset);
    for(int i = 0; i < fileRange.length / 4; i++) {
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