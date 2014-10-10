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
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Vector;

import edu.nyu.cs.cs2580.SearchEngine.Options;

/**
 * @CS2580: Implement this class for HW2.
 */
public class IndexerInvertedDoconly extends Indexer implements Serializable{

  private static final long serialVersionUID = 1077111905740085030L;

  // An index, which is a mapping between a term and a list of document IDs containing that term.
  private Map<String,ArrayList<Integer>> _index = new HashMap<String,ArrayList<Integer>>();
	
  // Stores all DocumentIndexed in memory.
  private Vector<DocumentIndexed> _documents = new Vector<DocumentIndexed>();
  
  public IndexerInvertedDoconly(Options options) {
    super(options);
    System.out.println("Using Indexer: " + this.getClass().getSimpleName());
  }

  @Override
  public void constructIndex() throws IOException {	
	  String corpusFile = "./data/wiki";
	  System.out.println("Construct index from: " + corpusFile);
    
	  File dir = new File(corpusFile);
	  File[] directoryListing = dir.listFiles();
	  if (directoryListing != null) {
	    for (File child : directoryListing) {
	       
	       // adding an indexed document 
	       String fileName = child.getName();  // this will be set to a document's title
	       DocumentIndexed docIndexed = new DocumentIndexed(_numDocs);
	       docIndexed.setTitle(fileName);
	       _documents.add(docIndexed);
	    	
	       BufferedReader reader = new BufferedReader(new FileReader(child));
           try {
        	 StringBuffer text = new StringBuffer();;  // the original text of the document
             
        	 String line = null;
             while ((line = reader.readLine()) != null) {
               text.append(line);
             }
             
             processDocument(text.toString());
             
           } finally {
             reader.close();
           }
	    }
	  }
	  else {
		  throw new IOException("Invalid directory.");
	  }
    
	  System.out.println(
	      "Indexed " + Integer.toString(_numDocs) + " docs with " +
	      Long.toString(_totalTermFrequency) + " terms.");
	
	  String indexFile = _options._indexPrefix + "/corpus.idx";
	  System.out.println("Store index to: " + indexFile);
	  ObjectOutputStream writer =
	      new ObjectOutputStream(new FileOutputStream(indexFile));
	  writer.writeObject(this);
	  writer.close();
  }
  
  

  //   No stop word is removed, you need to dynamically determine whether to
  //   drop the processing of a certain inverted list.
  
  // The input of this method (String text) is the raw context of the document.
  public void processDocument(String text){
	  String visibleContext = removeNonVisibleContext(text);  // step 1 of document processing
	  
	  Vector<String> docTokens = new Vector<String>();
	  Scanner scanner = new Scanner(visibleContext);
	  while(scanner.hasNext()){
		  docTokens.add(scanner.next());
	  }
	  
	  performStemming(docTokens);  // step 2 of document processing
	  
	  for(String term : docTokens){
		  if(_index.containsKey(term)){
			  ArrayList<Integer> postingList = _index.get(term);
			  postingList.add(_numDocs);
		  }
		  else{
			  ArrayList<Integer> postingList = new ArrayList<Integer>();
			  postingList.add(_numDocs);
		  }
		  ++_totalTermFrequency;
	  }
	  
	  ++_numDocs;
  }
  
  // Non-visible page content is removed, e.g., those inside <script> tags.
  public String removeNonVisibleContext(String text){
	  
	  return text;
  }
  
  // Tokens are stemmed with Step 1 of the Porter's algorithm.
  public void performStemming(Vector<String> docTokens){
	  
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
