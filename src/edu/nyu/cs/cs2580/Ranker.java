package edu.nyu.cs.cs2580;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;

class Ranker {
  private Index _index;
  public static enum RankingMethod { COSINE, QL, PHRASE, NUMVIEWS, LINEAR };

  private boolean saveOutput = true;

  public Ranker(String indexSource) throws FileNotFoundException {
    _index = new Index(indexSource);

    if(saveOutput) {
        saveOutput();
    }
  }
  
  public void saveOutput() throws FileNotFoundException {
	// The output file will be saved in the parent directory that contains the .class file
	String currentPath      = System.getProperty("user.dir");

	// please change the input path accordingly
    String inputQueryPath   = "./data/queries.tsv";

    // output file names
    String vsmFileName      = "hw1.1-vsm.tsv";  // vector space model
    String qlFileName       = "hw1.1-ql.tsv";  // query likelihood with Jelinek-Mercer smoothing
    String phraseFileName   = "hw1.1-phrase.tsv";  // phrase-based RankingMethod
    String numviewFileName  = "hw1.1-numviews.tsv";  // numviews-based RankingMethod
    String linearFileName   = "hw1.2-linear.tsv";  // linear RankingMethod
    
    // output query response to be written out to each output file
    String vsmQueryResponse     = "";
    String qlQueryResponse      = "";
    String phraseQueryResponse  = "";
    String numviewQueryResponse = "";
    String linearQueryResponse  = "";
    
    Vector<String> queries = new Vector<String>();
    
    try {
      BufferedReader reader = new BufferedReader(new FileReader(inputQueryPath));
      String line = null;
      while((line = reader.readLine())!=null){
        queries.add(line);
      }
      reader.close();
    } catch (IOException ioe) {
      System.err.println("Oops " + ioe.getMessage());
    }
    
    for(int i=0; i < queries.size(); i++){
      vsmQueryResponse      += getQueryResponse(queries.get(i), "COSINE");
      qlQueryResponse       += getQueryResponse(queries.get(i), "QL");
      phraseQueryResponse   += getQueryResponse(queries.get(i), "PHRASE");
      numviewQueryResponse  += getQueryResponse(queries.get(i), "NUMVIEWS");
      linearQueryResponse   += getQueryResponse(queries.get(i), "LINEAR");
    }
    
    // writing out to files
    File vsmFile = new File(currentPath + "/" + vsmFileName);
    PrintWriter vsmWriter = new PrintWriter(vsmFile);
    vsmWriter.write(vsmQueryResponse);
    vsmWriter.close();
    
    File qlFile = new File(currentPath + "/" + qlFileName);
    PrintWriter qlWriter = new PrintWriter(qlFile);
    qlWriter.write(qlQueryResponse);
    qlWriter.close();
    
    File phraseFile = new File(currentPath + "/" + phraseFileName);
    PrintWriter phraseWriter = new PrintWriter(phraseFile);
    phraseWriter.write(phraseQueryResponse);
    phraseWriter.close();
    
    File numviewFile = new File(currentPath + "/" + numviewFileName);
    PrintWriter numviewWriter = new PrintWriter(numviewFile);
    numviewWriter.write(numviewQueryResponse);
    numviewWriter.close();
    
    File linearFile = new File(currentPath + "/" + linearFileName);
    PrintWriter linearWriter = new PrintWriter(linearFile);
    linearWriter.write(linearQueryResponse);
    linearWriter.close();
  }
  
  public String getQueryResponse(String query, String method){
    method = method.toUpperCase();
    try {
        RankingMethod m = RankingMethod.valueOf(method); // throws IllegalArgumentException if not a valid enum
        Vector<ScoredDocument> scoredDocuments = runquery(query, m);
        Collections.sort(scoredDocuments);
        return getResultsAsString(query, scoredDocuments);
    } catch (IllegalArgumentException e) {
        return null;
    }
  }

  // This method returns the result string after sorting the scored documents in the required format:
  // QUERY<TAB>DOCUMENTID-1<TAB>TITLE<TAB>SCORE
  public String getResultsAsString(String query, Vector<ScoredDocument> sortedDocuments){
	String result = "";
    for(int i = 0; i < sortedDocuments.size(); i++) {
      result += query + "\t" + sortedDocuments.get(i).asString() + "\n";
    }
    return result;
  }
  
  public Vector<ScoredDocument> runquery(String query, RankingMethod method) {
    Vector<ScoredDocument> retrievalResults = new Vector<ScoredDocument>();
    
    // Build query vector
    Scanner s = new Scanner(query);
    Vector<String> qv = new Vector<String>();
    while (s.hasNext()){
      String term = s.next();
      qv.add(term);
    }
    
    for (int i = 0; i < _index.numDocs(); ++i){
        switch (method) {
            case COSINE:
                retrievalResults.add(cosineSimilarity(qv, i));
                break;
            case QL:
                retrievalResults.add(queryLikelihood(qv, i));
                break;
            case PHRASE:
                retrievalResults.add(phraseRanker(qv, i));
                break;
            case NUMVIEWS:
                retrievalResults.add(numViews(i));
                break;
            case LINEAR:
                retrievalResults.add(simpleLinear(qv, i));
                break;
        }
    }
    return retrievalResults;
  }
  
  public ScoredDocument cosineSimilarity(Vector<String> qv, int did){
    Document d = _index.getDoc(did);
    Vector<String> dv = d.get_body_vector();

    double score = 0, q_sqr = 0, d_sqr = 0;
    int n = _index.numDocs();
    double idf, wtf_q, wtf_d, q, doc;

    // inserts query terms into map
    Map<String, Integer> queryMap = new TreeMap<String, Integer>();
    for(String query : qv) {
      if(queryMap.containsKey(query)) {
        queryMap.put(query, queryMap.get(query) + 1);
      } else {
        queryMap.put(query, 1);
      }
    }

    // iterates over all words in query
    for(String term : queryMap.keySet()) {
      idf = 1 + Math.log(n / _index.documentFrequency(term)) / Math.log(2);
      wtf_q = queryMap.get(term);
      wtf_d = termFrequencyInDocument(dv, term);
      q = wtf_q * idf;
      doc = wtf_d * idf;
      q_sqr += q * q;
      d_sqr += doc * doc;
      score += q * doc;
    }

    if (q_sqr * d_sqr == 0)
      score = 0;
    else
      score /= Math.sqrt(q_sqr * d_sqr);

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
