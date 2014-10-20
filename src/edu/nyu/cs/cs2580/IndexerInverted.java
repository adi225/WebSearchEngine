package edu.nyu.cs.cs2580;

import java.io.*;
import java.text.Normalizer;
import java.util.*;
import java.util.regex.Pattern;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import de.l3s.boilerpipe.BoilerpipeProcessingException;
import de.l3s.boilerpipe.document.TextDocument;
import de.l3s.boilerpipe.extractors.ArticleExtractor;
import de.l3s.boilerpipe.sax.BoilerpipeSAXInput;
import de.l3s.boilerpipe.sax.HTMLDocument;
import edu.nyu.cs.cs2580.FileUtils.FileRange;
import org.xml.sax.SAXException;

/**
 * @CS2580: Implement this class for HW2.
 */
public abstract class IndexerInverted extends Indexer implements Serializable {
  private static final long serialVersionUID = 1077111905740085030L;
  protected static final String WORDS_DIR = "/.partials";
  protected static long UTILITY_INDEX_FLAT_SIZE_THRESHOLD = 1000000;
  protected static long INDEX_CACHE_THRESHOLD = 500000;

  protected RandomAccessFile _indexRAF;
  protected final String indexFilePath = _options._indexPrefix + "/index.idx";

  // Utility index is only used during index construction.
  protected Map<Integer, List<Integer>> _utilityIndex = new HashMap<Integer, List<Integer>>();
  protected long _utilityIndexFlatSize = 0;
  protected long _utilityPartialIndexCounter = 0;
  protected int MAX_DOCS = Integer.MAX_VALUE;

  // An index, which is a mapping between an integer representation of a term
  // and a byte range in the file where the postings list for the term is located.
  protected Map<Integer, FileRange> _index = new HashMap<Integer, FileRange>();

  // A cache for postings lists to contain some of the lists in memory.
  protected Map<Integer, List<Integer>> _indexCache = new HashMap<Integer, List<Integer>>();
  protected long _indexCacheFlatSize = 0;
  protected Map<Integer, Integer> _cachedDocId = Maps.newHashMap();
  protected Map<Integer, Integer> _cachedOffset = Maps.newHashMap();

  // An offset in the file where the postings lists begin (after all metadata).
  protected long _indexOffset = 0;
	
  // Metadata of documents.
  protected Vector<Document> _documents = new Vector<Document>();
  
  // Maps each term to its integer representation
  protected BiMap<String, Integer> _dictionary = HashBiMap.create();

  // Term frequency, key is the integer representation of the term and value is
  // the number of times the term appears in the corpus.
  protected Map<Integer, Integer> _termCorpusFrequency = new HashMap<Integer, Integer>();

  // The set contains stopping words, corresponding to the top 50 most frequent words in a corpus.
  protected Set<String> _stoppingWords = new HashSet<String>();
  
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
      Map<Integer, Vector<Integer>> docBodies = new HashMap<Integer, Vector<Integer>>();
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
        Document docIndexed = new DocumentIndexed(docId);
        docIndexed.setTitle(docFile.getName());
        docIndexed.setUrl(docFile.getAbsolutePath());
        _documents.add(docIndexed);

        try {
          // process the raw content of the document and build maps
          Vector<Integer> processedBody = processDocument(docIndexed, text.toString());
          updatePostingsLists(docId, processedBody);
          docBodies.put(docId, processedBody);
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
        dumpUtilityIndexToFileAndClearFromMemory(
                _options._indexPrefix + WORDS_DIR + "/" + _utilityPartialIndexCounter++);
      }

      populateStoppingWords();
      precomputeSquareTFIDFSum(docBodies);
    } else {
        throw new IOException("Invalid directory.");
    }

    // Merge all partial indexes.
    System.out.println("Generated " + _utilityPartialIndexCounter + " partial indexes.");
    String filePathBase = _options._indexPrefix + WORDS_DIR + "/";
    String filePath1 =  filePathBase + 0;
    String resFilePath = filePath1;
    long mergeStart = System.currentTimeMillis();
    for(int i = 1; i < _utilityPartialIndexCounter; i++) {
      String filePath2 = filePathBase + i;
      Map<Integer, FileRange> tempIndex = new HashMap<Integer, FileRange>();
      resFilePath = filePath1 + i;
      System.out.println("Merging file #" + i);
      long offset = FileUtils.mergeFilesIntoIndexAndFile(filePath1, filePath2, tempIndex, resFilePath);
      filePath1 = resFilePath;
      if(i == _utilityPartialIndexCounter - 1) {
        _index = tempIndex;
        _indexOffset = offset;
      }
    }
    new File(resFilePath).renameTo(indexAuxFile);
    new File(_options._indexPrefix + WORDS_DIR).delete();
    System.out.println("Done merging files.");

    // Add metadata to file.
    FileUtils.writeObjectsToFile(selectMetadataToStore(), indexFile);
    FileUtils.appendFileToFile(indexAuxFile, indexFile);

    System.out.println(
            "Indexed " + Integer.toString(_numDocs) + " docs with " +
                    Long.toString(_totalTermFrequency) + " terms.");

    long timeTaken = (System.currentTimeMillis() - startTime) / 60000;
    long mergingTime = (System.currentTimeMillis() - mergeStart) / 60000;
    System.out.println("Total indexing time: " + timeTaken + " min");
    //System.out.println("Total document processing time: " + timeTaken + " min");
    //System.out.println("Total partial index dumping time: " + timeTaken + " min");
    System.out.println("Total partial index merging time: " + mergingTime + " min");
  }
  
  // This method populates the top 50 most frequent words into _stoppingWords.
  private void populateStoppingWords(){
	  Map<Integer, Integer> sortedTermCorpusFrequency = sortByValues(_termCorpusFrequency); 
	  
      Set termSet = sortedTermCorpusFrequency.entrySet();
      Iterator iterator = termSet.iterator();
      for(int i=0;i<50;i++) {  // extracting the top 50 terms (the order is preserved)
           Map.Entry me = (Map.Entry)iterator.next();
           int termId = (Integer)me.getKey();
           String term = _dictionary.inverse().get(termId);
           _stoppingWords.add(term);
      }
  }
  
  // This helper method sorts the given map by value in a decreasing order.
  private HashMap sortByValues(Map map) { 
      List list = new LinkedList(map.entrySet());
      // Defined Custom Comparator here
      Collections.sort(list, new Comparator() {
           public int compare(Object o1, Object o2) {
              return ((Comparable) ((Map.Entry) (o2)).getValue())
                 .compareTo(((Map.Entry) (o1)).getValue());
           }
      });

      // Here I am copying the sorted list in HashMap
      // using LinkedHashMap to preserve the insertion order
      HashMap sortedHashMap = new LinkedHashMap();
      for (Iterator it = list.iterator(); it.hasNext();) {
             Map.Entry entry = (Map.Entry) it.next();
             sortedHashMap.put(entry.getKey(), entry.getValue());
      } 
      return sortedHashMap;
 }

  private void precomputeSquareTFIDFSum(Map<Integer, Vector<Integer>> docBodies) {
    // Precompute squared tfidf sum of documents.
    System.out.println("Precomputing cosine ranker data.");
    for(int docId = 0; docId < _numDocs; docId++) {
      if(docId % 1000 == 0) {
        System.out.println("Finished precomputing cosine ranker data for " + docId + " documents.");
      }
      DocumentIndexed doc = (DocumentIndexed) this.getDoc(docId);
      Vector<Integer> docBody = docBodies.get(docId);
      docBodies.remove(docId);

      Map<Integer, Integer> documentMap = new HashMap<Integer, Integer>();
      for(int word : docBody) {
        if(documentMap.containsKey(word)) {
          documentMap.put(word, documentMap.get(word) + 1);
        } else {
          documentMap.put(word, 1);
        }
      }

      // we precompute sum(tfidf^2)
      double d_sqr = 0;
      double idf, tf_d, tfidf_d;
      for(int word : documentMap.keySet()) {
        String wordString = _dictionary.inverse().get(word);
        idf = 1 + Math.log(((double)_numDocs) / corpusDocFrequencyByTerm(wordString)) / Math.log(2);
        tf_d = documentMap.get(word);
        tfidf_d = tf_d * idf;  // tfidf of term in document
        d_sqr += tfidf_d * tfidf_d; // computing sum(y^2) term of cosine similarity
      }
      doc.setTfidfSumSquared(d_sqr);
    }
  }

  protected List<Object> selectMetadataToStore() {
    List<Object> indexMetadata = new ArrayList<Object>();
    indexMetadata.add(_documents);
    indexMetadata.add(_dictionary);
    indexMetadata.add(_termCorpusFrequency);
    indexMetadata.add(_stoppingWords);
    return indexMetadata;
  }

  protected void setLoadedMetadata(List<Object> indexMetadata) {
    _documents           = (Vector<Document>)indexMetadata.get(0);
    _dictionary          = (BiMap<String, Integer>)indexMetadata.get(1);
    _termCorpusFrequency = (Map<Integer, Integer>)indexMetadata.get(2);
    _stoppingWords       = (Set<String>)indexMetadata.get(3);
  }

  @Override
  public void loadIndex() throws IOException, ClassNotFoundException {
    File indexFile = new File(indexFilePath);
    List<Object> indexMetadata = new ArrayList<Object>();
    DataInputStream indexFileDIS = new DataInputStream(new FileInputStream(indexFile));
    long bytesRead = FileUtils.readObjectsFromFileIntoList(indexFileDIS, indexMetadata);
    setLoadedMetadata(indexMetadata);
    bytesRead += FileUtils.loadFromFileIntoIndex(indexFileDIS, _index);
    indexFileDIS.close();
    _numDocs = _documents.size();
    for(int freq: _termCorpusFrequency.values()) {
      _totalTermFrequency += freq;
    }
    _indexOffset = bytesRead;
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
   * representation, store the integers in {@code tokens}.
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

  protected void dumpUtilityIndexToFileAndClearFromMemory(String filePath) throws IOException {
    FileUtils.dumpIndexToFile(_utilityIndex, new File(filePath));
    //_utilityIndex = new HashMap<Integer, List<Integer>>();
    _utilityIndex.clear();
    _utilityIndexFlatSize = 0;
  }

  protected abstract int nextPhrase(List<String> phraseTokens, int docid) throws IOException;

  protected abstract int next(String term, int docid) throws IOException;
}