package edu.nyu.cs.cs2580;

import java.io.*;
import java.util.*;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import de.l3s.boilerpipe.BoilerpipeProcessingException;
import de.l3s.boilerpipe.extractors.ArticleExtractor;

import edu.nyu.cs.cs2580.FileUtils.FileRange;

/**
 * @CS2580: Implement this class for HW2.
 */
public abstract class IndexerInverted extends Indexer implements Serializable {
  private static final long serialVersionUID = 1077111905740085030L;
  protected static final String WORDS_DIR = "/.partials";
  protected static final long UTILITY_INDEX_FLAT_SIZE_THRESHOLD = 1000000;

  protected RandomAccessFile _indexRAF;
  protected final String indexFilePath = _options._indexPrefix + "/index.idx";

  // Utility index is only used during index construction.
  protected Map<Integer, List<Integer>> _utilityIndex = new HashMap<Integer, List<Integer>>();
  protected long _utilityIndexFlatSize = 0;
  protected long _utilityPartialIndexCounter = 0;

  // An index, which is a mapping between an integer representation of a term
  // and a byte range in the file where the postings list for the term is located.
  protected Map<Integer, FileRange> _index = new HashMap<Integer, FileRange>();

  // An offset in the file where the postings lists begin (after all metadata).
  protected long _indexOffset = 0;
	
  // Metadata of documents.
  protected Vector<Document> _documents = new Vector<Document>();
  
  // Maps each term to its integer representation
  protected BiMap<String, Integer> _dictionary = HashBiMap.create();

  // Term frequency, key is the integer representation of the term and value is
  // the number of times the term appears in the corpus.
  protected Map<Integer, Integer> _termCorpusFrequency = new HashMap<Integer, Integer>();

  // Provided for serialization.
  public IndexerInverted() { }

  // The real constructor
  public IndexerInverted(SearchEngine.Options options) {
    super(options);
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
        Document docIndexed = new Document(docId);
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
    _documents           = (Vector<Document>)indexMetadata.get(0);
    _dictionary          = (BiMap<String, Integer>)indexMetadata.get(1);
    _termCorpusFrequency = (Map<Integer, Integer>)indexMetadata.get(2);
    bytesRead += FileUtils.loadFromFileIntoIndex(indexFileDIS, _index);
    indexFileDIS.close();
    _indexOffset = bytesRead;
    _indexRAF = new RandomAccessFile(indexFile, "r");
    _indexRAF.seek(_indexOffset);
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
  protected Vector<Integer> readTermVector(String content) {
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
  public Document getDoc(int docid) {
    return (docid >= _documents.size() || docid < 0) ? null : _documents.get(docid);
  }

  protected void dumpUtilityIndexToFileAndClearFromMemory(String filePath) throws IOException {
    FileUtils.dumpIndexToFile(_utilityIndex, new File(filePath));
    _utilityIndex = new HashMap<Integer, List<Integer>>();
    _utilityIndexFlatSize = 0;
    System.gc();
  }

  // This method may be deprecated in later versions. Use with caution!
  protected List<Integer> postingsListForWord(int word) throws IOException {
    List<Integer> postingsList = new LinkedList<Integer>();
    FileUtils.FileRange fileRange = _index.get(word);
    _indexRAF.seek(_indexOffset + fileRange.offset);
    for(int i = 0; i < fileRange.length / 4; i++) {
      postingsList.add(_indexRAF.readInt());
    }
    return postingsList;
  }

  public abstract void processDocument(int docId, String text) throws IOException, BoilerpipeProcessingException;

  public abstract int next(String term, int docid) throws IOException;
}