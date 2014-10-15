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
  private static final long UTILITY_INDEX_FLAT_SIZE_THRESHOLD = 1000000;

  private RandomAccessFile _indexFile;

  // Utility index is only used during index construction.
  private Map<Integer, List<Integer>> _utilityIndex = new HashMap<Integer, List<Integer>>();
  private long _utilityIndexFlatSize = 0;
  private long _utilityPartialIndexCounter = 0;

  // An index, which is a mapping between an integer representation of a term
  // and a list of document IDs containing that term.
  private Map<Integer, FileRange> _index = new HashMap<Integer, FileRange>();
  private long _indexOffset = 0;
	
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
      File indexFile = new File(_options._indexPrefix + "/hello.idx");
      _indexFile = new RandomAccessFile(indexFile, "rw");
    } catch (IOException e) {
      e.printStackTrace();
      System.err.println("Could not open index file.");
      System.exit(-1);
    }
    System.out.println("Using Indexer: " + this.getClass().getSimpleName());
  }

  @Override
  public void constructIndex() throws IOException {
	System.out.println("Construct index from: " + _options._corpusPrefix);
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
      long offset = mergeFilesIntoIndexAndFile(filePath1, filePath2, tempIndex, resFilePath);
      filePath1 = resFilePath;
      if(i == _utilityPartialIndexCounter - 1) {
        _index = tempIndex;
        _indexOffset = offset;
        new File(resFilePath).renameTo(new File(_options._indexPrefix + "/index.idx"));
        new File(_options._indexPrefix + WORDS_DIR).delete();
      }
    }
    System.out.println("Done merging files.");

    System.out.println(
        "Indexed " + Integer.toString(_numDocs) + " docs with " +
        Long.toString(_totalTermFrequency) + " terms.");
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

  private void dumpUtilityIndexToFileAndClearFromMemory(String filePath) throws IOException {
    dumpIndexToFile(_utilityIndex, new File(filePath));
    _utilityIndex = new HashMap<Integer, List<Integer>>();
    _utilityIndexFlatSize = 0;
    System.gc();
  }

  protected static long dumpIndexToFile(Map<Integer, List<Integer>> partialIndex, File _file) throws IOException {
    System.out.println("Generating partial index: " + _file.getAbsolutePath());
    Map<Integer, FileRange> indexPointerMap = new HashMap<Integer, FileRange>();

    // Write actual index to auxiliary file
    File aux = new File(_file.getAbsolutePath() + "_aux");
    long filePointer = 0;
    DataOutputStream auxDOS = new DataOutputStream(new FileOutputStream(aux));

    List<Integer> words = new ArrayList(partialIndex.keySet());
    Collections.sort(words);
    for (Integer word : words) {
      List<Integer> postingsList = partialIndex.get(word);
      indexPointerMap.put(word, new FileRange(filePointer, postingsList.size()));
      for (int posting : postingsList) {
        auxDOS.writeInt(posting);
      }
      filePointer += postingsList.size() * 4;
    }
    auxDOS.close();

    // Append pointer map to file before actual index.
    long offset = writeObjectToFile(indexPointerMap, _file);

    // Stream actual index from aux file to end of index file.
    FileOutputStream partialIndexFileOS = new FileOutputStream(_file, true);
    FileInputStream partialIndexFileAuxIS = new FileInputStream(aux);
    copyStream(partialIndexFileAuxIS, partialIndexFileOS);
    partialIndexFileAuxIS.close();
    partialIndexFileOS.close();
    aux.delete();
    return offset;
  }

  protected static long loadFromFileIntoIndex(DataInputStream _fileDIS, Map<Integer, FileRange> index) throws IOException {
    index.clear();
    try {
      List<Object> metadata = new ArrayList<Object>();
      long filePointer = readObjectsFromFileIntoList(_fileDIS, metadata);
      index.putAll((Map<Integer, FileRange>) metadata.get(0));
      return filePointer;
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
      throw new IllegalArgumentException("File is not correctly deserializable.");
    }
  }

  protected static long mergeFilesIntoIndexAndFile(String file1URL,
                                          String file2URL,
                                          Map<Integer, FileRange> index,
                                          String resFileURL) throws IOException {
    return mergeFilesIntoIndexAndFile(new File(file1URL), new File(file2URL), index, new File(resFileURL));
  }

  protected static long mergeFilesIntoIndexAndFile(File _file1,
                                                 File _file2,
                                                 Map<Integer, FileRange> index,
                                                 File _resFile) throws IOException {
    index.clear();
    File aux = new File(_resFile.getAbsolutePath() + "_aux");
    Map<Integer, FileRange> index1 = new HashMap<Integer, FileRange>();
    Map<Integer, FileRange> index2 = new HashMap<Integer, FileRange>();
    DataInputStream file1DIS = new DataInputStream(new FileInputStream(_file1));
    DataInputStream file2DIS = new DataInputStream(new FileInputStream(_file2));
    DataOutputStream auxDOS = new DataOutputStream(new FileOutputStream(aux));

    loadFromFileIntoIndex(file1DIS, index1);
    loadFromFileIntoIndex(file2DIS, index2);

    List<Integer> index1Words = new ArrayList<Integer>(index1.keySet());
    List<Integer> index2Words = new ArrayList<Integer>(index2.keySet());
    Collections.sort(index1Words);
    Collections.sort(index2Words);
    int index1WordsSize = index1Words.size();
    int index2WordsSize = index2Words.size();
    int totalSize = index1WordsSize + index2WordsSize;
    int i, li, ri;
    i = li = ri = 0;
    long fileOffset = 0;

    while (i < totalSize) {
      if(li < index1WordsSize && ri < index2WordsSize) {
        int word1 = index1Words.get(li);
        int word2 = index2Words.get(ri);
        if (word1 < word2) {
          FileRange word1Range = index1.get(word1);
          index.put(word1, new FileRange(fileOffset, word1Range.length));
          byte[] buf = new byte[(int)word1Range.length * 4];
          fileOffset += buf.length;
          file1DIS.read(buf);
          auxDOS.write(buf);
          li++; i++;
        } else if (word2 < word1) {
          FileRange word2Range = index2.get(word2);
          index.put(word2, new FileRange(fileOffset, word2Range.length));
          byte[] buf = new byte[(int)word2Range.length * 4];
          fileOffset += buf.length;
          file2DIS.read(buf);
          auxDOS.write(buf);
          ri++; i++;
        } else {
          FileRange word1Range = index1.get(word1);
          FileRange word2Range = index2.get(word2);
          long postingListTotalSize = word1Range.length + word2Range.length;
          index.put(word1, new FileRange(fileOffset, postingListTotalSize));
          byte[] buf1 = new byte[(int)word1Range.length * 4];
          byte[] buf2 = new byte[(int)word2Range.length * 4];
          fileOffset += buf1.length + buf2.length;
          file1DIS.read(buf1);
          file2DIS.read(buf2);
          auxDOS.write(buf1);
          auxDOS.write(buf2);
          li++; ri++; i+=2;
        }
      } else {
        if(li < index1WordsSize) {
          int word1 = index1Words.get(li);
          FileRange word1Range = index1.get(word1);
          index.put(word1, new FileRange(fileOffset, word1Range.length));
          byte[] buf = new byte[(int)word1Range.length * 4];
          fileOffset += buf.length;
          file1DIS.read(buf);
          auxDOS.write(buf);
          li++; i++;
        }
        if(ri < index2WordsSize) {
          int word2 = index2Words.get(ri);
          FileRange word2Range = index2.get(word2);
          index.put(word2, new FileRange(fileOffset , word2Range.length));
          byte[] buf = new byte[(int)word2Range.length * 4];
          fileOffset += buf.length;
          file2DIS.read(buf);
          auxDOS.write(buf);
          ri++; i++;
        }
      }
    }

    file1DIS.close();
    file2DIS.close();
    auxDOS.close();

    // Append pointer map to file before actual index.
    long offset = writeObjectToFile(index, _resFile);

    // Stream actual index from aux file to end of index file.
    FileOutputStream mergedIndexFileOS = new FileOutputStream(_resFile, true);
    FileInputStream mergedIndexFileAuxIS = new FileInputStream(aux);
    copyStream(mergedIndexFileAuxIS, mergedIndexFileOS);
    mergedIndexFileAuxIS.close();
    mergedIndexFileOS.close();
    aux.delete();
    _file1.delete();
    _file2.delete();
    return offset;
  }

  protected static <T> long writeObjectToFile(T object, File _file) throws IOException {
    List<Object> list = new ArrayList<Object>();
    list.add(object);
    return writeObjectsToFile(list, _file);
  }

  protected static long writeObjectsToFile(List<Object> stores, File _file) throws IOException {
    DataOutputStream fileDOS = new DataOutputStream(new FileOutputStream(_file, true));
    //long totalSize = 0;
    for(Object store : stores) {
      ByteArrayOutputStream b = new ByteArrayOutputStream();
      ObjectOutputStream o = new ObjectOutputStream(b);
      o.writeObject(store);
      byte[] bytes = b.toByteArray();
      //totalSize += bytes.length;
      fileDOS.writeInt(bytes.length);
      fileDOS.write(bytes);
    }
    fileDOS.writeInt(0);
    fileDOS.close();
    //return totalSize + stores.size() * 4 + 4;
    return _file.length();
  }

  protected static long readObjectsFromFileIntoList(File file, List<Object> store) throws IOException, ClassNotFoundException {
    DataInputStream fileDIS = new DataInputStream(new FileInputStream(file));
    return readObjectsFromFileIntoList(fileDIS, store);
  }

  protected static long readObjectsFromFileIntoList(DataInputStream fileDIS, List<Object> store) throws IOException, ClassNotFoundException {
    if(store == null) throw new IllegalArgumentException("Cannot read into null list.");
    store.clear();
    int size = fileDIS.readInt();
    long totalSize = size;
    while(size > 0) {
      byte[] bytes = new byte[size];
      fileDIS.read(bytes);
      ObjectInputStream o = new ObjectInputStream(new ByteArrayInputStream(bytes));
      store.add(o.readObject());
      size = fileDIS.readInt();
      totalSize += size;
    }
    // TODO This may mislead if fileDIS is not starting from offset 0.
    return totalSize + store.size() * 4 + 4;
  }

  public static long copyStream(InputStream from, OutputStream to) throws IOException {
    final int BUF_SIZE = 0x1000; // 4K
    if(from == null) throw new IllegalArgumentException("Input Stream must be specified.");
    if(to == null) throw new IllegalArgumentException("Output Stream must be specified");
    byte[] buf = new byte[BUF_SIZE];
    long total = 0;
    while (true) {
      int r = from.read(buf);
      if (r == -1) {
        break;
      }
      to.write(buf, 0, r);
      total += r;
    }
    return total;
  }

  static class FileRange implements Serializable {
    private static final long serialVersionUID = 7526472295622776147L;
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
