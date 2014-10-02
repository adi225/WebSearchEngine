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
    File vsmFile = new File(currentPath + "/results/" + vsmFileName);
    PrintWriter vsmWriter = new PrintWriter(vsmFile);
    vsmWriter.write(vsmQueryResponse);
    vsmWriter.close();
    
    File qlFile = new File(currentPath + "/results/" + qlFileName);
    PrintWriter qlWriter = new PrintWriter(qlFile);
    qlWriter.write(qlQueryResponse);
    qlWriter.close();
    
    File phraseFile = new File(currentPath + "/results/" + phraseFileName);
    PrintWriter phraseWriter = new PrintWriter(phraseFile);
    phraseWriter.write(phraseQueryResponse);
    phraseWriter.close();
    
    File numviewFile = new File(currentPath + "/results/" + numviewFileName);
    PrintWriter numviewWriter = new PrintWriter(numviewFile);
    numviewWriter.write(numviewQueryResponse);
    numviewWriter.close();
    
    File linearFile = new File(currentPath + "/results/" + linearFileName);
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
        Map<String, Integer> documentMap = new HashMap<String, Integer>();
        Vector<String> dv = _index.getDoc(i).get_body_vector();

        for(String word : dv) {
            if(documentMap.containsKey(word)) {
                documentMap.put(word, documentMap.get(word) + 1);
            } else {
                documentMap.put(word, 1);
            }
        }

        switch (method) {
            case COSINE:
                retrievalResults.add(cosineSimilarity(qv, documentMap, i));
                break;
            case QL:
                retrievalResults.add(queryLikelihood(qv, documentMap, i));
                break;
            case PHRASE:
                retrievalResults.add(phraseRanker(qv, documentMap, i));
                break;
            case NUMVIEWS:
                retrievalResults.add(numViews(i));
                break;
            case LINEAR:
                retrievalResults.add(simpleLinear(qv, documentMap, i));
                break;
        }
    }
    return retrievalResults;
  }
  
  public ScoredDocument cosineSimilarity(Vector<String> qv, Map<String, Integer> documentMap, int did){
    double score = 0, q_sqr = 0, d_sqr = 0;
    int n = _index.numDocs();
    double idf, tf_q, tf_d, tfidf_q, tfidf_d;

    // inserts query terms into map
    Map<String, Integer> queryMap = new HashMap<String, Integer>();
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
      tf_q = queryMap.get(term); // count of term in query
      tf_d = documentMap.containsKey(term) ? documentMap.get(term) : 0; // count of term in document
      tfidf_q = tf_q * idf;  // tfidf of term in query
      tfidf_d = tf_d * idf;  // tfidf of term in document
      q_sqr += tfidf_q * tfidf_q;  // computing x^2 term of cosine similarity
      d_sqr += tfidf_d * tfidf_d;  // computing y^2 term of cosine similarity
      score += tfidf_q * tfidf_d;  // computing x*y term of cosine similarity
    }

    if (q_sqr * d_sqr == 0)
      score = 0;
    else
      score /= Math.sqrt(q_sqr * d_sqr); // computing cosine similarity

    return new ScoredDocument(did, _index.getDoc(did).get_title_string(), score);
  }
  
  public ScoredDocument queryLikelihood(Vector<String> qv, Map<String, Integer> documentMap, int did){
    Document d = _index.getDoc(did);
    Vector<String> dv = d.get_body_vector();
    
    int documentSize = dv.size();
    int totalWordsInCorpus = _index.termFrequency();
	double lambda = 0.5;
	
	double score = 0;
	
	for(String word : qv) {
	  int wordFrequencyInDocument = documentMap.containsKey(word) ? documentMap.get(word) : 0;
	  int wordFrequencyInCorpus = _index.termFrequency(word);

	  // This formula calculates the value of log( P(Q|D) ), which is given by
	  // the summation of log( ((1-lambda) * wordFrequencyInDocument/documentSize) + wordFrequencyInCorpus/totalWordsInCorpus )
	  // Usage of the log is to overcome the problem of multiplying many small numbers together, which might
	  // lead to accuracy problem.
	  score += Math.log(((1-lambda)*((double)wordFrequencyInDocument)/documentSize)+lambda*((double)wordFrequencyInCorpus)/totalWordsInCorpus);
	}
	
    return new ScoredDocument(did, d.get_title_string(), score);
  } 
  
  public ScoredDocument phraseRanker(Vector<String> qv, Map<String, Integer> documentMap, int did){
    Document d = _index.getDoc(did);
    Vector<String> dv = d.get_body_vector();
    double score = 0;

    if(qv.size() ==  1) {
        String term = qv.firstElement();
        score = documentMap.containsKey(term) ? documentMap.get(term) : 0;
        return new ScoredDocument(did, d.get_title_string(), score);
    }

    for(int i = 0; i < qv.size()-1; i++) {
    	score += getBigramFrequencyInDocument(dv, qv.get(i), qv.get(i+1));
    }
    return new ScoredDocument(did, d.get_title_string(), score);
  }
  
  // Method that returns the number of times a bigram appears in the document
  int getBigramFrequencyInDocument(Vector<String> dv, String word1, String word2)
  {
	  int frequency = 0;
      for(int i = 0; i < dv.size()-1; i++) {
          //for a bigram to be counted, we need to find the 2 words consecutively
          if (dv.get(i).equalsIgnoreCase(word1) && dv.get(i + 1).equalsIgnoreCase(word2))
              frequency++;
      }
	  return frequency;
  }
  
  public ScoredDocument numViews(int did){
    Document d = _index.getDoc(did);
    return new ScoredDocument(did, d.get_title_string(), d.get_numviews());
  }
  
  public ScoredDocument simpleLinear(Vector<String> qv, Map<String, Integer> documentMap, int did){
	double betaCos = 1;
	double betaQL = 1;
	double betaPhrase = 1;
	double betaNumviews = 1;
	
	double combined_score = (betaCos      * cosineSimilarity(qv, documentMap, did)._score) +
			                (betaQL       * queryLikelihood(qv, documentMap, did)._score) +
			                (betaPhrase   * phraseRanker(qv, documentMap, did)._score) +
			                (betaNumviews * numViews(did)._score);
	
	return new ScoredDocument(did, _index.getDoc(did).get_title_string(), combined_score);
  }
}
