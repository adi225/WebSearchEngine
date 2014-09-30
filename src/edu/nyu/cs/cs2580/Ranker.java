package edu.nyu.cs.cs2580;

import java.awt.LinearGradientPaint;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.Vector;
import java.util.Scanner;

class Ranker {
  private Index _index;
  public static enum ranker {COSINE,QL,PHRASE,NUMVIEWS,LINEAR};
  private boolean save_Output = false;

  public Ranker(String index_source) throws FileNotFoundException{
    _index = new Index(index_source);
    if(save_Output){
      saveOutput();
    }
  }
  
  // This method saves the output of each ranking algorithm into files
  public void saveOutput() throws FileNotFoundException {
	// The output file will be saved in the parent directory that contains the .class file
	String current_path = System.getProperty("user.dir");
	
	// please change the input path accordingly
    String input_query_path = "D:/NYU/Courses/Web Search Engine/Fall 2014/HW1/data/queries.tsv";
    
    // output file names
    String vsm_file_name = "hw1.1-vsm.tsv";  // vector space model
    String ql_file_name = "hw1.1-ql.tsv";  // query likelihood with Jelinek-Mercer smoothing
    String phrase_file_name = "hw1.1-phrase.tsv";  // phrase-based ranker
    String numview_file_name = "hw1.1-numviews.tsv";  // numviews-based ranker
    String linear_file_name = "hw1.2-linear.tsv";  // linear ranker
    
    // output query response to be written out to each output file
    String vsm_queryResponse = "";
    String ql_queryResponse = "";
    String phrase_queryResponse = "";
    String numview_queryResponse = "";
    String linear_queryResponse = "";
    
    Vector<String> queries = new Vector<String>();
    
    try{
      BufferedReader reader = new BufferedReader(new FileReader(input_query_path));	
      String line = null;
      while((line=reader.readLine())!=null){
        queries.add(line);
      }
      reader.close();
    }
    catch (IOException ioe){
      System.err.println("Oops " + ioe.getMessage());
    }
    
    for(int i=0;i<queries.size();i++){
      vsm_queryResponse += getQueryResponse(queries.get(i), "COSINE");
      ql_queryResponse += getQueryResponse(queries.get(i), "QL");
      phrase_queryResponse += getQueryResponse(queries.get(i), "PHRASE");
      numview_queryResponse += getQueryResponse(queries.get(i), "NUMVIEWS");
      linear_queryResponse += getQueryResponse(queries.get(i), "LINEAR");
    }
    
    // writing out to files
    
    File vsm_file = new File(current_path+"/"+vsm_file_name);
    PrintWriter vsm_writer = new PrintWriter(vsm_file);
    vsm_writer.write(vsm_queryResponse);
    vsm_writer.close();
    
    File ql_file = new File(current_path+"/"+ql_file_name);
    PrintWriter ql_writer = new PrintWriter(ql_file);
    ql_writer.write(ql_queryResponse);
    ql_writer.close();
    
    File phrase_file = new File(current_path+"/"+phrase_file_name);
    PrintWriter phrase_writer = new PrintWriter(phrase_file);
    phrase_writer.write(phrase_queryResponse);
    phrase_writer.close();
    
    File numviews_file = new File(current_path+"/"+numview_file_name);
    PrintWriter numviews_writer = new PrintWriter(numviews_file);
    numviews_writer.write(numview_queryResponse);
    numviews_writer.close();
    
    File linear_file = new File(current_path+"/"+linear_file_name);
    PrintWriter linear_writer = new PrintWriter(linear_file);
    linear_writer.write(linear_queryResponse);
    linear_writer.close();
  }
  
  public String getQueryResponse(String query, String method){
    if(method.equalsIgnoreCase(ranker.COSINE.toString())){
      Vector < ScoredDocument > scored_documents = runquery(query, "COSINE");
      sortScoredDocuments(scored_documents);
      String queryResponse = getResultsAsString(query, scored_documents);	
      return queryResponse;    	
    }
    else if(method.equalsIgnoreCase(ranker.QL.toString())){
      Vector < ScoredDocument > scored_documents = runquery(query, "QL");
      sortScoredDocuments(scored_documents);
      String queryResponse = getResultsAsString(query, scored_documents);	
      return queryResponse;
    }
    else if(method.equalsIgnoreCase(ranker.PHRASE.toString())){
      Vector < ScoredDocument > scored_documents = runquery(query, "PHRASE");
      sortScoredDocuments(scored_documents);
      String queryResponse = getResultsAsString(query, scored_documents);	
      return queryResponse;	
    }
    else if(method.equalsIgnoreCase(ranker.NUMVIEWS.toString())){
      Vector < ScoredDocument > scored_documents = runquery(query, "NUMVIEWS");
      sortScoredDocuments(scored_documents);
      String queryResponse = getResultsAsString(query, scored_documents);	
      return queryResponse;	
    }
    else if(method.equalsIgnoreCase(ranker.LINEAR.toString())){
      Vector < ScoredDocument > scored_documents = runquery(query, "LINEAR");
      sortScoredDocuments(scored_documents);
      String queryResponse = getResultsAsString(query, scored_documents);	
      return queryResponse;	    	
    }
    return null;
  }
  
  // This method sorts the given vector of scored documents according to their score in a decreasing order
  public void sortScoredDocuments(Vector<ScoredDocument> scored_documents){
    Collections.sort(scored_documents);
  }
  
  // This method returns the result string after sorting the scored documents in the required format:
  // QUERY<TAB>DOCUMENTID-1<TAB>TITLE<TAB>SCORE
  public String getResultsAsString(String query, Vector<ScoredDocument> sorted_documents){
	String result = "";
    for(int i=0;i<sorted_documents.size();i++){
      result += query+"\t"+sorted_documents.get(i).asString()+"\n";
    }
    return result;
  }
  
  public Vector < ScoredDocument > runquery(String query, String method){
    Vector < ScoredDocument > retrieval_results = new Vector < ScoredDocument > ();
    
    // Build query vector
    Scanner s = new Scanner(query);
    Vector < String > qv = new Vector < String > ();
    while (s.hasNext()){
      String term = s.next();
      qv.add(term);
    }
    
    for (int i = 0; i < _index.numDocs(); ++i){
	    ScoredDocument sd = null;
	    if(method.equalsIgnoreCase(ranker.COSINE.toString())){
	      sd = cosineSimilarity(qv, i);
	    }
	    else if(method.equalsIgnoreCase(ranker.QL.toString())){
	      sd = queryLikelihood(qv, i);
	    }
	    else if(method.equalsIgnoreCase(ranker.PHRASE.toString())){
		  sd = phraseRanker(qv, i);
		}
	    else if(method.equalsIgnoreCase(ranker.NUMVIEWS.toString())){
		  sd = numViews(i);
		}
	    else if(method.equalsIgnoreCase(ranker.LINEAR.toString())){
		  sd = simpleLinear(qv, i);
		}
        retrieval_results.add(sd);
    }
    return retrieval_results;
  }
  
  public ScoredDocument cosineSimilarity(Vector < String > qv, int did){
    Document d = _index.getDoc( did );
    Vector < String > dv = d.get_body_vector();

    double score = 0, q_sqr = 0, d_sqr = 0;
    int n = _index.numDocs();
    double idf, wtf_q, wtf_d, q, doc;

    // inserts query into map
    TreeMap < String , Integer > queryList = new TreeMap< String, Integer >();
    for (int i = 0; i<qv.size(); i++) {
      String query = qv.get(i);
      if ( queryList.containsKey( query ) ) {
        queryList.put( query, queryList.get( query ) + 1 );
      } else {
        queryList.put( query, 1 );
      }
    }

    // iterates over all words in query
    Set set = queryList.entrySet();
    Iterator iter = set.iterator();
    while( iter.hasNext() ) {
      Map.Entry < String, Integer > queryWord = ( Map.Entry ) iter.next();
      idf = 1 + ( double ) Math.log( n / _index.documentFrequency( queryWord.getKey() ) ) /  Math.log( 2 );
      // wtf_q = 1 + ( double ) Math.log( queryWord.getValue() );
      wtf_q = queryWord.getValue();
      wtf_d = termFrequencyInDocument( dv, queryWord.getKey() );
      q = wtf_q * idf;
      doc = wtf_d * idf;
      q_sqr += Math.pow( q, 2 );
      d_sqr += Math.pow( doc, 2 );
      score += q * doc;
    }

    if ( q_sqr * d_sqr == 0 )
      score = 0;
    else
      score /= Math.pow( q_sqr * d_sqr , 0.5 );

    return new ScoredDocument(did, d.get_title_string(), score);
  }
  
  public ScoredDocument queryLikelihood(Vector < String > qv, int did){
    Document d = _index.getDoc(did);
    Vector < String > dv = d.get_body_vector();  
    
    int document_size = dv.size();
    int total_words_in_collection = _index.termFrequency();
	double lambda = 0.5;
	
	double score = 0;
	
	for(int i=0;i<qv.size();i++){
	  int word_frequency_in_document = termFrequencyInDocument(dv, qv.get(i));
	  int word_frequency_in_corpus = _index.termFrequency(qv.get(i));
	  
	  // This formula calculates the value of log( P(Q|D) ), which is given by
	  // the summation of log( ((1-lambda)*word_frequency_in_document/document_size) + word_frequency_in_corpus/total_words_in_collection )
	  // Usage of the log is to overcome the problem of multiplying many small numbers together, which might
	  // lead to accuracy problem.
	  score += Math.log(((1-lambda)*((double)word_frequency_in_document)/document_size)+lambda*((double)word_frequency_in_corpus)/total_words_in_collection);
	}
	
    return new ScoredDocument(did, d.get_title_string(), score);
  } 
  
  public ScoredDocument phraseRanker(Vector < String > qv, int did){
    Document d = _index.getDoc(did);
    Vector < String > dv = d.get_body_vector(); 
    double bigramsCount = dv.size()-1;
    int j = 1;    
    double score = 1;
    for(int i = 0; i<qv.size()-1; i++)
    {
    	String word1 = qv.get(i);
    	String word2 = qv.get(j);
    	int bigramFrequencyInDoc = getBigramFrequencyInDocument(dv, word1, word2);
    	score *= bigramFrequencyInDoc/bigramsCount;
    	
    	j++;
    }
    ScoredDocument doc = new ScoredDocument(did, d.get_title_string(), score);
    return doc;
  }
  
  //Method that returns the number of times a bigram appears in the document
  int getBigramFrequencyInDocument(Vector<String> dv, String word1, String word2)
  {
	  int frequency = 0;
	  
	  int j = 1;
	  for(int i = 0; i<dv.size()-1; i++)
	  {
		  //for a bigram to be counted, we need to find the 2 words consecutively
		 if(dv.get(i).equalsIgnoreCase(word1) && dv.get(j).equalsIgnoreCase(word2))
			 frequency++;
		 
		 j++;
	  }
	   
	  return frequency;
  }
  
  public ScoredDocument numViews(int did){
    Document d = _index.getDoc( did );
    int score = d.get_numviews();
    return new ScoredDocument(did, d.get_title_string(), score);
  }
  
  public ScoredDocument simpleLinear(Vector < String > qv, int did){
	Document d = _index.getDoc( did );  
	  
	double beta_cos = 1;
	double beta_ql = 1;
	double beta_phrase = 1;
	double beta_numviews = 1;
	
	ScoredDocument d_cos = cosineSimilarity(qv, did);
	ScoredDocument d_ql = queryLikelihood(qv, did);
	ScoredDocument d_phrase = phraseRanker(qv, did);
	ScoredDocument d_numviews = numViews(did);
	
	double combined_score = (beta_cos*d_cos._score)+ 
			                (beta_ql*d_ql._score)+ 
			                (beta_phrase*d_phrase._score)+ 
			                (beta_numviews*d_numviews._score);
	
	return new ScoredDocument(did, d.get_title_string(), combined_score);
  }

  // This helper method takes a document vector and a term as inputs and returns the number of occurrence of the given term
  // in the given document vector.
  public int termFrequencyInDocument(Vector < String> dv, String term){
    int count = 0;
    for(int i=0;i<dv.size();i++){
    	if(dv.get(i).equalsIgnoreCase(term)){
    	  count++;
    	}
    }
	return count;
  }

}
