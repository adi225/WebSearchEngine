package edu.nyu.cs.cs2580;

import java.io.*;
import java.util.*;

import de.l3s.boilerpipe.BoilerpipeProcessingException;
import de.l3s.boilerpipe.extractors.ArticleExtractor;

import edu.nyu.cs.cs2580.SearchEngine.Options;

/**
 * @CS2580: Implement this class for HW2.
 */
public class IndexerInvertedDoconly extends Indexer implements Serializable{

  private static final long serialVersionUID = 1077111905740085030L;
  private static final String WORDS_DIR = "/words";
  private static final long UTILITY_INDEX_FLAT_SIZE_THRESHOLD = 100000;

  private RandomAccessFile _indexFile;

  // An index, which is a mapping between an integer representation of a term
  // and a list of document IDs containing that term.
  private Map<Integer, List<Integer>>_utilityIndex = new HashMap<Integer, List<Integer>>();
  private long _utilityIndexFlatSize = 0;
  private long _utilityPartialIndexCounter = 0;

  private Map<Integer, FileRange> _index = new HashMap<Integer, FileRange>();
	
  // Stores all DocumentIndexed in memory.
  private Vector<DocumentIndexed> _documents = new Vector<DocumentIndexed>();
  
  // Maps each term to their integer representation
  private Map<String, Integer> _dictionary = new HashMap<String, Integer>();

  // All unique terms appeared in corpus. Offsets are integer representations.
  private Vector<String> _terms = new Vector<String>();

  // Term frequency, key is the integer representation of the term and value is
  // the number of times the term appears in the corpus.
  private Map<Integer, Integer> _termCorpusFrequency = new HashMap<Integer, Integer>();

  public IndexerInvertedDoconly(Options options) {
    super(options);
    try {
      File indexFile = new File(_options._indexPrefix + "/index.idx");
      _indexFile = new RandomAccessFile(indexFile, "rw");
    } catch (IOException e) {
      System.err.println("Could not open index file.");
      System.exit(-1);
    }
    System.out.println("Using Indexer: " + this.getClass().getSimpleName());
  }

  @Override
  public void constructIndex() throws IOException {
	  System.out.println("Construct index from: " + _options._corpusPrefix);
      new File(_options._indexPrefix + WORDS_DIR).mkdir();

      // TODO Remove debug code.
      int debugCounter = 0;

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

          //if(debugCounter++ >= 10) { break; }
	    }
	  } else {
		  throw new IOException("Invalid directory.");
	  }

	  System.out.println(
	      "Indexed " + Integer.toString(_numDocs) + " docs with " +
	      Long.toString(_totalTermFrequency) + " terms.");

      System.out.println(_dictionary.size());
      int wordCounter = 0;
      for(String word : _dictionary.keySet()) {
        System.out.print(word + " ");
        wordCounter++;
      }

	  String indexFile = _options._indexPrefix + "/corpus.idx";
	  System.out.println("Store index to: " + indexFile);
	  ObjectOutputStream writer = new ObjectOutputStream(new FileOutputStream(indexFile));
	  writer.writeObject(this);
	  writer.close();
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

        System.out.println("Check value: " + _utilityIndex.get(35).get(8));

        dumpIndexToFile(_utilityIndex, filePath);

        // CLEANUP
        _utilityIndexFlatSize = 0;
        _utilityIndex = new HashMap<Integer, List<Integer>>();
        System.gc();
/*
        Map<Integer, FileRange> index = new HashMap<Integer, FileRange>();
        long offset = loadFromFileIntoIndex(filePath, index);
        System.out.println("Size: " + index.size());
        System.out.println("Offset: " + offset);

        long location = index.get(35).offset;
        RandomAccessFile file = new RandomAccessFile(filePath, "r");
        file.seek(offset + location);
        for(int i = 0; i < 8; i++) file.readInt();
        int docId2 = file.readInt();
        System.out.println("Checked value: " + docId2);
        System.exit(-1);
*/
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
    // TODO Treat abreviation specially (I.B.M.)
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
	  
	  RandomAccessFile file = new RandomAccessFile(_options._indexPrefix + "index.idx", "r");
	  
	  int termInt = _dictionary.get(term);  // an integer representation of a term
	  FileRange postingList = _index.get(termInt);
	  file.seek(postingList.offset);  // plus the postinglist offset
	  
	  for(int i=0; i < postingList.length; i++){
		  int posting = file.readInt();
		  if(posting > docid){
			  return posting;
		  }
	  }
	  
	  return -1;
  }

  @Override
  public int corpusDocFrequencyByTerm(String term) {
	  try{
		  int termInt = _dictionary.get(term);  // an integer representation of a term
		  
		  RandomAccessFile file = new RandomAccessFile(_options._indexPrefix + "index.idx", "r");
		  
		  FileRange postingList = _index.get(termInt);
		  file.seek(postingList.offset);  // plus the postinglist offset
		  
		  return (int)postingList.length;
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

  @Override
  public int documentTermFrequency(String term, String url) {
    SearchEngine.Check(false, "Not implemented!");
    return 0;
  }

  private void dumpIndexToFile(Map<Integer, List<Integer>> index, String filePath) throws IOException {
    Map<Integer, FileRange> indexJumpMap = new HashMap<Integer, FileRange>();

    // Write actual index to auxiliary file
    RandomAccessFile partialIndexFileAux = new RandomAccessFile(filePath + "_aux", "rw");
    List<Integer> words = new ArrayList(index.keySet());
    Collections.sort(words);
    for (Integer word : words) {
      List<Integer> postingsList = index.get(word);
      long offset = partialIndexFileAux.getFilePointer();
      long length = postingsList.size();
      indexJumpMap.put(word, new FileRange(offset, length));
      for (int posting : postingsList) {
        partialIndexFileAux.writeInt(posting);
      }
    }

    // Write pointer map to beginning of index file.
    RandomAccessFile partialIndexFile = new RandomAccessFile(filePath, "rw");
    partialIndexFile.writeInt(words.size());
    for (Integer word : words) {
      FileRange fileRange = indexJumpMap.get(word);
      partialIndexFile.writeInt(word);
      partialIndexFile.writeLong(fileRange.offset);
      partialIndexFile.writeLong(fileRange.length);
    }

    int buf;
    partialIndexFileAux.seek(0);
    while((buf = partialIndexFileAux.read()) >= 0) {
      partialIndexFile.write(buf);
    }

    File file = new File(filePath + "_aux");
    file.delete();
  }

  private long loadFromFileIntoIndex(String fileURL, Map<Integer, FileRange> index) throws IOException {
    index.clear();
    RandomAccessFile file = new RandomAccessFile(fileURL, "r");
    int wordsSize = file.readInt();
    for(int i = 0; i < wordsSize; i++) {
      int word = file.readInt();
      long offset = file.readLong();
      long length = file.readLong();
      index.put(word, new FileRange(offset, length));
    }
    long filePointer = file.getFilePointer();
    file.close();
    return filePointer;
  }

  private long mergeFilesIntoIndexAndFile(String file1URL,
                                          String file2URL,
                                          Map<Integer, FileRange> index,
                                          String resFileURL) throws IOException {
    index.clear();
    Map<Integer, FileRange> index1 = new HashMap<Integer, FileRange>();
    Map<Integer, FileRange> index2 = new HashMap<Integer, FileRange>();
    long file1Offset = loadFromFileIntoIndex(file1URL, index1);
    long file2Offset = loadFromFileIntoIndex(file2URL, index2);
    RandomAccessFile file1 = new RandomAccessFile(file1URL, "r");
    RandomAccessFile file2 = new RandomAccessFile(file2URL, "r");
    RandomAccessFile aux = new RandomAccessFile(resFileURL + "_aux", "rw");
    List<Integer> index1Words = new ArrayList<Integer>(index1.keySet());
    List<Integer> index2Words = new ArrayList<Integer>(index2.keySet());
    Collections.sort(index1Words);
    Collections.sort(index2Words);
    ListIterator<Integer> index1WordsIterator = index1Words.listIterator();
    ListIterator<Integer> index2WordsIterator = index2Words.listIterator();

    int word1 = index1WordsIterator.next();
    int word2 = index2WordsIterator.next();
    file1.seek(file1Offset);
    file2.seek(file2Offset);
    while (index1WordsIterator.hasNext() && index2WordsIterator.hasNext()) {
      if(word1 < word2) {
        FileRange word1Range = index1.get(word1);
        index.put(word1, new FileRange(aux.getFilePointer() , word1Range.length));
        for(int i = 0; i < word1Range.length; i++) {
          aux.writeInt(file1.readInt());
        }
        word1 = index1WordsIterator.next();
      } else if (word2 > word1) {
        FileRange word2Range = index2.get(word2);
        index.put(word2, new FileRange(aux.getFilePointer() , word2Range.length));
        for(int i = 0; i < word2Range.length; i++) {
          aux.writeInt(file2.readInt());
        }
        word2 = index2WordsIterator.next();
      } else {
        FileRange word1Range = index1.get(word1);
        FileRange word2Range = index2.get(word2);
        int postingWord1 = file1.readInt();
        int postingWord2 = file2.readInt();
        int word1Counter = 0;
        int word2Counter = 0;
        index.put(word1, new FileRange(aux.getFilePointer(), word1Range.length + word2Range.length));
        while(word1Counter < word1Range.length && word2Counter < word2Range.length) {
          if(postingWord1 < postingWord2) {
            aux.writeInt(postingWord1);
            if(++word1Counter < word1Range.length) postingWord1 = file1.readInt();
          } else if (postingWord1 > postingWord2) {
            aux.writeInt(postingWord2);
            if(++word2Counter < word2Range.length) postingWord2 = file2.readInt();
          } else {
            throw new IllegalArgumentException("Two lists should not have redundant data.");
          }
        }
        while(word1Counter < word1Range.length) {
          aux.writeInt(postingWord1);
          if(++word1Counter < word1Range.length) postingWord1 = file1.readInt();
        }
        while(word2Counter < word2Range.length) {
          aux.writeInt(postingWord2);
          if(++word2Counter < word2Range.length) postingWord2 = file2.readInt();
        }
        word1 = index1WordsIterator.next();
        word2 = index2WordsIterator.next();
      }
    }
    while(index1WordsIterator.hasNext()) {
      FileRange word1Range = index1.get(word1);
      index.put(word1, new FileRange(aux.getFilePointer() , word1Range.length));
      for(int i = 0; i < word1Range.length; i++) {
        aux.writeInt(file1.readInt());
      }
      word1 = index1WordsIterator.next();
    }
    while(index2WordsIterator.hasNext()) {
      FileRange word2Range = index2.get(word2);
      index.put(word2, new FileRange(aux.getFilePointer() , word2Range.length));
      for(int i = 0; i < word2Range.length; i++) {
        aux.writeInt(file2.readInt());
      }
      word2 = index2WordsIterator.next();
    }
    // TODO Add the map first, then the aux file data.
    return 0;
  }


  class FileRange {
    public long offset;
    public long length;

    public FileRange(long _offset, long _length) {
      offset = _offset;
      length = _length;
    }

    @Override
    public String toString() {
      return "(->" + offset + ", " + length + ")";
    }

    @Override
    public boolean equals(Object other) {
      if(other instanceof FileRange) {
        FileRange o = (FileRange)other;
        if(o.offset == this.offset && o.length == this.length) {
          return true;
        }
      }
      return false;
    }

    @Override
    public int hashCode() {
      long res = this.offset * 37 + this.length;
      return (int)res;
    }
  }
}
