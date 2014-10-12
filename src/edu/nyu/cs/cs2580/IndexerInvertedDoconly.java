package edu.nyu.cs.cs2580;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.Vector;

import de.l3s.boilerpipe.BoilerpipeProcessingException;
import de.l3s.boilerpipe.extractors.ArticleExtractor;

import edu.nyu.cs.cs2580.SearchEngine.Options;

/**
 * @CS2580: Implement this class for HW2.
 */
public class IndexerInvertedDoconly extends Indexer implements Serializable{

  private static final long serialVersionUID = 1077111905740085030L;

  // An index, which is a mapping between an integer representation of a term
  // and a list of document IDs containing that term.
  private Map<Integer, List<Integer>> _index = new HashMap<Integer, List<Integer>>();
	
  // Stores all DocumentIndexed in memory.
  private Vector<DocumentIndexed> _documents = new Vector<DocumentIndexed>();
  
  // Maps each term to their integer representation
  private Map<String, Integer> _dictionary = new HashMap<String, Integer>();

  // All unique terms appeared in corpus. Offsets are integer representations.
  private Vector<String> _terms = new Vector<String>();

  // Term frequency, key is the integer representation of the term and value is
  // the number of times the term appears in the corpus.
  private Map<Integer, Integer> _termCorpusFrequency = new HashMap<Integer, Integer>();

  private final String CORPUS_PREFIX;
  
  public IndexerInvertedDoconly(Options options) {
    super(options);
    CORPUS_PREFIX = options._corpusPrefix;
    System.out.println("Using Indexer: " + this.getClass().getSimpleName());
  }

  @Override
  public void constructIndex() throws IOException {
	  System.out.println("Construct index from: " + CORPUS_PREFIX);
    
	  File dir = new File(CORPUS_PREFIX);
	  File[] directoryListing = dir.listFiles();
	  if (directoryListing != null) {
	    for (File docFile : directoryListing) {
	       StringBuffer text = new StringBuffer();  // the original text of the document
	       
	       // getting the original text of the document
	       BufferedReader reader = new BufferedReader(new FileReader(docFile));
           try {
        	 String line = null;
             while ((line = reader.readLine()) != null) {
               text.append(line+"\n");
             }
             
             processDocument(text.toString());  // process the raw context of the document
             
           } catch (Exception e) {
               throw new IOException("File" + docFile.getPath() + " could not be read.");
  		   } finally {
             reader.close();
           }
	    }
	  } else {
		  throw new IOException("Invalid directory.");
	  }
    
	  System.out.println(
	      "Indexed " + Integer.toString(_numDocs) + " docs with " +
	      Long.toString(_totalTermFrequency) + " terms.");
	
	  String indexFile = _options._indexPrefix + "/corpus.idx";
	  System.out.println("Store index to: " + indexFile);
	  ObjectOutputStream writer = new ObjectOutputStream(new FileOutputStream(indexFile));
	  writer.writeObject(this);
	  writer.close();
  }
  
  

  //   No stop word is removed, you need to dynamically determine whether to
  //   drop the processing of a certain inverted list.
  
  // The input of this method (String text) is the raw context of the document.
  public void processDocument(String text) throws Exception{
	  String visibleContext = removeNonVisibleContext(text);  // step 1 of document processing
	  String stemmedContext = performStemming(visibleContext);  // step 2 of document processing
	  
      // adding an indexed document 
      DocumentIndexed docIndexed = new DocumentIndexed(_numDocs);  // the current number of doc is ID for the current document
      Vector<Integer> docTokensAsIntegers = new Vector<Integer>();
      readTermVector(stemmedContext, docTokensAsIntegers);
      docIndexed.setBodyTokens(docTokensAsIntegers);
      _documents.add(docIndexed);
	  
      HashSet<Integer> uniqueTokens = new HashSet<Integer>();  // unique term ID
      for(Integer term : docTokensAsIntegers){
    	  if(uniqueTokens.contains(term)==false){
    		  uniqueTokens.add(term);
    	  }
    	  _totalTermFrequency++;
      }
      
      // Indexing
	  for(Integer term : uniqueTokens) {
		  if(_index.containsKey(term)) {
			  List<Integer> postingList = _index.get(term);
			  postingList.add(_numDocs);  // add the current doc into the posting list
		  }
		  else {
			  List<Integer> postingList = new ArrayList<Integer>();
			  postingList.add(_numDocs);
			  _index.put(term, postingList);
		  }
	  }
	  
	  System.out.println("Finished indexing document id: "+_numDocs);
	  _numDocs++;
	  
  }
  
  // Non-visible page content is removed, e.g., those inside <script> tags.
  // Right now, the 3rd party library "BoilerPiper" is used to perform the task.
  public String removeNonVisibleContext(String text) throws Exception{
	  String visibleContext = ArticleExtractor.INSTANCE.getText(text);
	  return visibleContext;
  }
  
  // Tokens are stemmed with Step 1 of the Porter's algorithm.
  public String performStemming(String text){
	  return text;
  }

  /**
   * Tokenize {@code content} into terms, translate terms into their integer
   * representation, store the integers in {@code tokens}.
   * @param content
   * @param tokens
   */
  private void readTermVector(String content, Vector<Integer> tokens) {
    Scanner s = new Scanner(content);  // Uses white space by default.
    while (s.hasNext()) {
      String token = s.next();
      int idx = -1;
      if (_dictionary.containsKey(token)) {
        idx = _dictionary.get(token);
        _termCorpusFrequency.put(idx, _termCorpusFrequency.get(idx) + 1);
      } else {
        idx = _terms.size();  // offsets are the integer representations
        _terms.add(token);
        _dictionary.put(token, idx);
        _termCorpusFrequency.put(idx, 1);
      }
      tokens.add(idx);
    }
    return;
  }
  
 
  @Override
  public void loadIndex() throws IOException, ClassNotFoundException {
  }

  @Override
  public Document getDoc(int docid) {
    return null;
  }

  /**
   * In HW2, you should be using {@link DocumentIndexed}
   */
  @Override
  public Document nextDoc(Query query, int docid) {
    return null;
  }
  
  // Just like in the lecture slide, this helper method returns the next document id
  // after the given docid
  public int next(String term, int docid){
	  
	  return -1;
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
}
