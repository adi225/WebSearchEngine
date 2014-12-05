package edu.nyu.cs.cs2580;

import java.io.*;
import java.util.*;

import com.google.common.collect.*;
import de.l3s.boilerpipe.BoilerpipeProcessingException;
import edu.nyu.cs.cs2580.FileUtils.FileRange;
import org.xml.sax.SAXException;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * @CS2580: Implement this class for HW2.
 */
public abstract class IndexerInverted extends Indexer implements Serializable {
  private static final long serialVersionUID = 1077111905740085030L;
  protected static final String WORDS_DIR = "/.partials";
  protected static long UTILITY_INDEX_FLAT_SIZE_THRESHOLD = 1000000;
  protected static long UTILITY_DOC_WORDS_FLAT_SIZE_THRESHOLD = 1000000;
  protected static long INDEX_CACHE_THRESHOLD = 500000;
  protected static int TOP_WORDS_TO_STORE = Integer.MAX_VALUE;
  protected static int MAX_DOCS = Integer.MAX_VALUE;
  protected static int STOPWORDS_NUM = 70;

  protected RandomAccessFile _indexRAF;
  protected final String indexFilePath = _options._indexPrefix + "/index.idx";

  //-----------------------------INVERTED INDEX------------------------------------//
  // Utility index is only used during index construction.
  protected Map<Integer, List<Integer>> _utilityIndex = Maps.newTreeMap();
  protected long _utilityIndexFlatSize = 0;
  protected int _utilityPartialIndexCounter = 0;

  // An index, which is a mapping between an integer representation of a term
  // and a byte range in the file where the postings list for the term is located.
  protected Map<Integer, FileRange> _index = Maps.newTreeMap();

  // A cache for postings lists to contain some of the lists in memory.
  protected Map<Integer, List<Integer>> _indexCache = Maps.newTreeMap();
  protected long _indexCacheFlatSize = 0;
  protected Map<Integer, Integer> _cachedDocId = Maps.newTreeMap();
  protected Map<Integer, Integer> _cachedOffset = Maps.newTreeMap();

  // An offset in the file where the postings lists begin (after all metadata).
  protected long _indexOffset = 0;
  protected long _indexByteSize = 0;

  //-----------------------------DOCUMENT WORDS-------------------------------------//
  // Utility map is only used during index construction.
  protected Map<Integer, List<Integer>> _utilityDocWords = Maps.newTreeMap();
  protected long _utilityDocWordsFlatSize = 0;
  protected int _utilityPartialDocWordsCounter = 0;

  // An index, which is a mapping between an integer representation of a term
  // and a byte range in the file where the postings list for the term is located.
  protected Map<Integer, FileRange> _docWords = Maps.newTreeMap();

  // An offset in the file where the postings lists begin (after all metadata).
  protected long _docWordsOffset = 0;

  //--------------------------------METADATA----------------------------------------//
  // Metadata of documents.
  protected Vector<DocumentIndexed> _documents = new Vector<DocumentIndexed>();
  
  // Maps each term to its integer representation
  protected BiMap<String, Integer> _dictionary = HashBiMap.create();

  // Term frequency, key is the integer representation of the term and value is
  // the number of times the term appears in the corpus.
  protected Map<Integer, Integer> _termCorpusFrequency = Maps.newTreeMap();;

  // The set contains stopping words, corresponding to the top 50 most frequent words in a corpus.
  protected Set<String> _stoppingWords = Sets.newTreeSet();
  
  // Provided for serialization.
  public IndexerInverted() { }

  // The real constructor
  public IndexerInverted(SearchEngine.Options options) {
    super(options);
  }

  @Override
  public void constructIndex() throws IOException {
	System.out.println("Construct index from: " + _options._corpusPrefix);
    long startTime = System.currentTimeMillis();
    File indexFile = new File(indexFilePath);
    File indexAuxFile = new File(indexFile.getAbsolutePath() + "_aux");
    File docWordsAuxFile = new File(indexFile.getAbsolutePath() + "_words_aux");
    if(indexFile.exists()) {
      indexFile.delete();
      indexFile = new File(indexFilePath);
    }
    if(indexAuxFile.exists()) {
      indexAuxFile.delete();
      indexAuxFile = new File(indexFile.getAbsolutePath() + "_aux");
    }
    if(docWordsAuxFile.exists()) {
      docWordsAuxFile.delete();
      docWordsAuxFile = new File(indexFile.getAbsolutePath() + "_words_aux");
    }
    new File(_options._indexPrefix + WORDS_DIR).mkdir();
    File[] directoryListing = checkNotNull(new File(_options._corpusPrefix)).listFiles();

    // ensure that documents are processed in an ascending order of docid
    Arrays.sort(directoryListing, new Comparator(){
      @Override
      public int compare(Object f1, Object f2) {
      	String f1Name = ((File) f1).getName();
      	String f2Name = ((File) f2).getName();
      	try{
         	int f1ID = Integer.parseInt(f1Name);
        	int f2ID = Integer.parseInt(f2Name);       		
        	return f1ID - f2ID;	        	
      	}
      	catch(Exception e){
      		return f1Name.compareTo(f2Name);
      	}

      }
    });
    
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
      docIndexed.setUrl(docFile.getName());
      _documents.add(docIndexed);

      try {
        // process the raw content of the document and build maps
        Vector<Integer> processedBody = processDocument(docIndexed, text.toString());
        updatePostingsLists(docId, processedBody);
        updateDocWords(docId, processedBody); // destructive! processedBody is cleared from memory.
      } catch (BoilerpipeProcessingException e) {
        throw new IOException("File format could not be processed by Boilerplate.");
      } catch (SAXException e) {
        throw new IOException("File format could not be processed by Boilerplate.");
      }
      System.out.println("Finished indexing document id: " + docId);

      if(_numDocs > MAX_DOCS) break;
    }

    // dump any leftover partial index
    if(_utilityIndexFlatSize > 0) {
      String filePath = _options._indexPrefix + WORDS_DIR + "/" + _utilityPartialIndexCounter++;
      dumpUtilityIndexToFileAndClearFromMemory(filePath);
    }
    if(_utilityDocWordsFlatSize > 0) {
      String filePath = _options._indexPrefix + WORDS_DIR + "/words" + _utilityPartialDocWordsCounter++;
      dumpUtilityDocWordsToFileAndClearFromMemory(filePath);
    }

    // Merge all partial indexes.
    System.out.println("Generated " + _utilityPartialIndexCounter + " partial indexes.");
    String filePathBase = _options._indexPrefix + WORDS_DIR + "/";
    FileUtils.mergeFilesWithBasePathIntoIndexAndFile(filePathBase, _utilityPartialIndexCounter, _index, indexAuxFile);

    // Merge all partial words lists.
    System.out.println("Generated " + _utilityPartialDocWordsCounter + " document word lists.");
    filePathBase = _options._indexPrefix + WORDS_DIR + "/words";
    FileUtils.mergeFilesWithBasePathIntoIndexAndFile(filePathBase, _utilityPartialDocWordsCounter, _docWords, docWordsAuxFile);

    new File(_options._indexPrefix + WORDS_DIR).delete();
    System.out.println("Done merging files.");

    // Calculate some corpus-holistic metadata.
    loadPageRanks(_documents);
    loadNumViews(_documents);
    _stoppingWords = findStoppingWords();
    precomputeTfIdfSumSquared(_documents, docWordsAuxFile);

    // Add metadata to file.
    _indexByteSize = indexAuxFile.length();
    long metadataSize = FileUtils.writeObjectsToFile(selectMetadataToStore(), indexFile);
    FileUtils.appendFileToFile(indexAuxFile, indexFile);
    FileUtils.appendFileToFile(docWordsAuxFile, indexFile);

    System.out.println(
            "Indexed " + Integer.toString(_numDocs) + " docs with " +
                    Long.toString(_totalTermFrequency) + " terms.");

    long timeTaken = (System.currentTimeMillis() - startTime) / 60000;
    System.out.println("Total indexing time: " + timeTaken + " min");

    // Prepare autocomplete file.
    AutocompleteQueryLog.getInstance().prepareMainFile();
  }
  
  // This method finds the top most frequent words in the corpus.
  private Set<String> findStoppingWords(){
    Set<String> stoppingWords = Sets.newHashSet();
    List<Map.Entry<Integer, Integer>> sortedTermCorpusFrequency = Utils.sortByValues(_termCorpusFrequency, true);

    for(int i = 0; i < STOPWORDS_NUM && i < sortedTermCorpusFrequency.size(); i++) {  // extracting the top terms
      int termId = sortedTermCorpusFrequency.get(i).getKey();
      String term = _dictionary.inverse().get(termId);
      stoppingWords.add(term);
    }
    return stoppingWords;
  }

  private void loadPageRanks(Vector<DocumentIndexed> documents) throws IOException {
    CorpusAnalyzer corpusAnalyzer = CorpusAnalyzer.Factory.getCorpusAnalyzerByOption(_options);
    Map<String, Float> pageRanks = (Map<String, Float>)corpusAnalyzer.load();
    for(Document document : documents) {
      if(pageRanks.containsKey(document.getUrl())) {
        document.setPageRank(pageRanks.get(document.getUrl()));
      }
    }
  }

  private void loadNumViews(Vector<DocumentIndexed> documents) throws IOException {
    LogMiner logMiner = LogMinerNumviews.Factory.getLogMinerByOption(_options);
    Map<String, Integer> numViews = (Map<String, Integer>)logMiner.load();
    for(Document document : documents) {
      if(numViews.containsKey(document.getUrl())) {
        document.setNumViews(numViews.get(document.getUrl()));
      }
    }
  }

  private void updateDocWords(int docId, Vector<Integer> docBody) throws IOException {
    checkArgument(docId < _documents.size());
    DocumentIndexed doc = _documents.get(docId);

    doc.setDocumentSize(docBody.size());

    // Count word frequencies.
    Map<Integer, Integer> documentMap = Maps.newTreeMap();
    for(int word : docBody) {
      int frequency = documentMap.containsKey(word) ? documentMap.get(word) : 0;
      documentMap.put(word, frequency + 1);
    }
    docBody.clear(); // discard the words from memory.

    // Sort the words by most frequent ones first.
    List<Map.Entry<Integer, Integer>> sortedDocumentMap = Utils.sortByValues(documentMap, true);
    documentMap = null; // discard the map from memory.

    List<Integer> docWords = Lists.newArrayList();
    _utilityDocWords.put(docId, docWords);
    for(int i = 0; docWords.size() / 2 < TOP_WORDS_TO_STORE && i < sortedDocumentMap.size(); i++) {
      docWords.add(sortedDocumentMap.get(i).getKey());
      docWords.add(sortedDocumentMap.get(i).getValue());
      _utilityDocWordsFlatSize += 2;
    }
    if(_utilityDocWordsFlatSize > UTILITY_DOC_WORDS_FLAT_SIZE_THRESHOLD) {
      String filePath = _options._indexPrefix + WORDS_DIR + "/words" + _utilityPartialDocWordsCounter++;
      dumpUtilityDocWordsToFileAndClearFromMemory(filePath);
    }
  }

  private void precomputeTfIdfSumSquared(Vector<DocumentIndexed> documents, File docWordsFile) throws IOException {
    DataInputStream docWordsFileDIS = new DataInputStream(new FileInputStream(docWordsFile));

    // Load word frequency lists.
    long docWordsOffset = FileUtils.loadFromFileIntoIndex(docWordsFileDIS, _docWords);
    docWordsFileDIS.close();
    RandomAccessFile docWordsRAF = new RandomAccessFile(docWordsFile, "r");

    for(DocumentIndexed doc : documents) {
      Map<Integer, Integer> documentMap = wordListForDoc(doc._docid, docWordsRAF, docWordsOffset);
      doc.setTfidfSumSquared(computeSquareTFIDFSumSquared(documentMap));
    }
    docWordsRAF.close();
  }

  private double computeSquareTFIDFSumSquared(Map<Integer, Integer> documentMap) {
    // Compute squared tfidf sum of documents.
    double d_sqr = 0;
    double idf, tf_d, tfidf_d;
    for(int word : documentMap.keySet()) {
      String wordString = _dictionary.inverse().get(word);
      idf = 1 + Math.log(((double)_numDocs) / corpusDocFrequencyByTerm(wordString)) / Math.log(2);
      tf_d = documentMap.get(word);
      tfidf_d = tf_d * idf;  // tfidf of term in document
      d_sqr += tfidf_d * tfidf_d; // computing sum(y^2) term of cosine similarity
    }
    return d_sqr;
  }

  protected List<Object> selectMetadataToStore() {
    List<Object> indexMetadata = new ArrayList<Object>();
    indexMetadata.add(_documents);
    indexMetadata.add(_dictionary);
    indexMetadata.add(_termCorpusFrequency);
    indexMetadata.add(_stoppingWords);
    indexMetadata.add(_indexByteSize);
    return indexMetadata;
  }

  protected void setLoadedMetadata(List<Object> indexMetadata) {
    _documents           = (Vector<DocumentIndexed>)indexMetadata.get(0);
    _dictionary          = (BiMap<String, Integer>)indexMetadata.get(1);
    _termCorpusFrequency = (Map<Integer, Integer>)indexMetadata.get(2);
    _stoppingWords       = (Set<String>)indexMetadata.get(3);
    _indexByteSize      = (Long)indexMetadata.get(4);
  }

  @Override
  public void loadIndex() throws IOException, ClassNotFoundException {
    File indexFile = new File(indexFilePath);
    List<Object> indexMetadata = new ArrayList<Object>();
    DataInputStream indexFileDIS = new DataInputStream(new FileInputStream(indexFile));

    // Read metadata from beginning of file.
    long bytesRead = FileUtils.readObjectsFromFileIntoList(indexFileDIS, indexMetadata);
    setLoadedMetadata(indexMetadata);
    _numDocs = _documents.size();
    for(int freq : _termCorpusFrequency.values()) {
      _totalTermFrequency += freq;
    }

    // Load inverted index.
    long indexFileRangesLength = FileUtils.loadFromFileIntoIndex(indexFileDIS, _index);
    bytesRead += indexFileRangesLength;
    _indexOffset = bytesRead;

    // Skip postings lists.
    indexFileDIS.skipBytes((int)(_indexByteSize - indexFileRangesLength));
    bytesRead += _indexByteSize - indexFileRangesLength;

    // Load word frequency lists.
    bytesRead += FileUtils.loadFromFileIntoIndex(indexFileDIS, _docWords);
    _docWordsOffset = bytesRead;

    indexFileDIS.close();
    _indexRAF = new RandomAccessFile(indexFile, "r");
    _indexRAF.seek(_indexOffset);
  }

  public Vector<Integer> processDocument(Document doc, String text) throws BoilerpipeProcessingException, SAXException {
    text = TextUtils.removeNonVisibleContext(doc, text);  // step 1 of document processing
    text = TextUtils.removeInitialsDots(text);
    text = TextUtils.deAccent(text);
    text = TextUtils.convertUnicodeSpecialLettersToASCII(text);
    text = TextUtils.removePunctuation(text, "").toLowerCase();
    return readTermVector(text);
  }

  /**
   * Tokenize {@code content} into terms, translate terms into their integer
   * representation.
   * @param content
   */
  protected Vector<Integer> readTermVector(String content) {
    Vector<Integer> tokens = new Vector<Integer>();
    Scanner s = new Scanner(content);  // Uses white space by default.
    while (s.hasNext()) {
      String token = s.next();
      token = TextUtils.performStemming(token);

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
  public Document getDoc(int docid) {
    return (docid >= _documents.size() || docid < 0) ? null : _documents.get(docid);
  }

  protected abstract void updatePostingsLists(int docId, Vector<Integer> docTokensAsIntegers) throws IOException;

  // This method returns the next docid after the given docid that contains all terms in the query conjunctive.
  // This is equivalent to the nextDoc method in the IndexerInvertedDoconly class.
  @Override
  public Document nextDoc(Query query, int docid) {
    List<List<String>> phrases = new ArrayList<List<String>>();
    if(query instanceof QueryPhrase) {
      QueryPhrase queryPhrase = (QueryPhrase)query;
      phrases = Lists.newArrayList(queryPhrase._phrases.values());
    }

    // Treat all tokens as 1 word phrases.
    for(String token : query._tokens) {
      phrases.add(Lists.newArrayList(token));
    }

    // query is already processed before getting passed into this method
    try {
      List<Integer> docIDs = new ArrayList<Integer>();  // a list containing doc ID for each phrase in the query
      for(List<String> phrase : phrases) {
        if(phrase.size() == 1 && _stoppingWords.contains(phrase.get(0))) {  // skip processing a stop word (if not in phrase)
          continue;
        }
        int nextDocID = nextPhrase(phrase, docid);
        if(nextDocID == -1) return null;
        docIDs.add(nextDocID);
      }

      boolean found = false;

      while(!found) {
        // Get maximum docId for all phrases.
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
            int docIdNew = nextPhrase(phrases.get(pos), maxDocId - 1);
            if (docIdNew < 0) return null;

            // Set this to new docId for that phrase.
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
      return _documents.get(docIDs.get(0));

    } catch (IOException e) {}
    return null;
  }

  // This method returns the next docid after the given docid that contains at least one of the terms in the query disjunctive.
  public Document nextDocDisjunctive(Query query, int docid) {
    List<List<String>> phrases = new ArrayList<List<String>>();
    if(query instanceof QueryPhrase) {
      QueryPhrase queryPhrase = (QueryPhrase)query;
      phrases = Lists.newArrayList(queryPhrase._phrases.values());
    }

    // Treat all tokens as 1 word phrases.
    for(String token : query._tokens) {
      phrases.add(Lists.newArrayList(token));
    }

    // query is already processed before getting passed into this method
    try {
      List<Integer> docIDs = new ArrayList<Integer>();  // a list containing doc ID for each phrase in the query
      for(List<String> phrase : phrases) {
        if(phrase.size() == 1 && _stoppingWords.contains(phrase.get(0))) {  // skip processing a stop word (if not in phrase)
          continue;
        }
        int nextDocID = nextPhrase(phrase, docid);
        docIDs.add(nextDocID);
      }

      // Get minimum docId for all phrases.
      int minDocId = Integer.MAX_VALUE;
      for(int pos = 0; pos < docIDs.size(); pos++) {
        if(docIDs.get(pos) >= 0) {
          minDocId = Math.min(docIDs.get(pos), minDocId);
        }
      }
      if(minDocId == Integer.MAX_VALUE) return null;
      return _documents.get(minDocId);
    } catch (IOException e) {}
    return null;
  }

  // This method may be deprecated in later versions. Use with caution!
  protected List<Integer> postingsListForWord(int word) throws IOException {
    if(_indexCache.containsKey(word)) {
      return _indexCache.get(word);
    }

    List<Integer> postingsList = new LinkedList<Integer>();
    FileUtils.FileRange fileRange = _index.get(word);
    _indexRAF.seek(_indexOffset + fileRange.offset);
    byte[] loadedList = new byte[(int)fileRange.length];
    _indexRAF.read(loadedList);
    DataInputStream loadedListDIS = new DataInputStream(new ByteArrayInputStream(loadedList));
    for(int i = 0; i < fileRange.length / 4; i++) {
      postingsList.add(loadedListDIS.readInt());
    }

    while(postingsList.size() * 4 + _indexCacheFlatSize > IndexerInverted.INDEX_CACHE_THRESHOLD) {
      List<Integer> lists = Lists.newArrayList(_indexCache.keySet());
      Random R = new Random();
      int randomListIndex = R.nextInt(lists.size());

      _indexCacheFlatSize -= _indexCache.get(lists.get(randomListIndex)).size() * 4;
      _indexCache.remove(lists.get(randomListIndex));
    }
    _indexCache.put(word, postingsList);
    _indexCacheFlatSize += postingsList.size() * 4;

    return postingsList;
  }

  public Map<String, Integer> wordListWithoutStopwordsForDoc(int docId) throws IOException {
    Map<String, Integer> result = wordListForDoc(docId);
    for(String stopword : _stoppingWords) {
      result.remove(stopword);
    }
    return result;
  }

  public Map<String, Integer> wordListForDoc(int docId) throws IOException {
    Map<Integer, Integer> internalWordList = wordListForDoc(docId, _indexRAF, _docWordsOffset);
    Map<String, Integer> result = Maps.newTreeMap();
    for(int word : internalWordList.keySet()) {
      result.put(_dictionary.inverse().get(word), internalWordList.get(word));
    }
    return result;
  }

  protected Map<Integer, Integer> wordListForDoc(int docId, RandomAccessFile store, long storeOffset) throws IOException {
    Map<Integer, Integer> wordsList = Maps.newTreeMap();
    FileUtils.FileRange fileRange = _docWords.get(docId);
    store.seek(storeOffset + fileRange.offset);
    byte[] loadedList = new byte[(int)fileRange.length];
    store.read(loadedList);
    DataInputStream loadedListDIS = new DataInputStream(new ByteArrayInputStream(loadedList));
    for(int i = 0; i < fileRange.length / 4; i+=2) {
      wordsList.put(loadedListDIS.readInt(), loadedListDIS.readInt());
    }
    return wordsList;
  }

  protected void dumpUtilityIndexToFileAndClearFromMemory(String filePath) throws IOException {
    FileUtils.dumpIndexToFile(_utilityIndex, new File(filePath));
    _utilityIndex.clear();
    _utilityIndexFlatSize = 0;
  }

  protected void dumpUtilityDocWordsToFileAndClearFromMemory(String filePath) throws IOException {
    FileUtils.dumpIndexToFile(_utilityDocWords, new File(filePath));
    _utilityDocWords.clear();
    _utilityDocWordsFlatSize = 0;
  }

  protected abstract int nextPhrase(List<String> phraseTokens, int docid) throws IOException;

  protected abstract int next(String term, int docid) throws IOException;
}