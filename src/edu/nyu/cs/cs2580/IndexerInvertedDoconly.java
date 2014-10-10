package edu.nyu.cs.cs2580;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import edu.nyu.cs.cs2580.SearchEngine.Options;

/**
 * @CS2580: Implement this class for HW2.
 */
public class IndexerInvertedDoconly extends Indexer implements Serializable{

  private static final long serialVersionUID = 1077111905740085030L;

  // An index, which is a mapping between a term and a list of document IDs containing that term.
  private Map<String,List<Integer>> _index = new HashMap<String,List<Integer>>();
	
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
  
  public void processDocument(String text){
	  
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
