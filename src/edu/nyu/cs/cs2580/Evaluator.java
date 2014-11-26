package edu.nyu.cs.cs2580;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.Vector;

/**
 * Evaluator for HW1.
 * 
 * @author fdiaz
 * @author congyu
 */

/**
 * Usage: java -cp src edu.nyu.cs.cs2580.Evaluator [judge_file]
 */

class Evaluator {
  public static class DocumentRelevances {
    private Map<Integer, Double> relevances = new HashMap<Integer, Double>();
    
    public DocumentRelevances() { }
    
    public void addDocument(int docid, String grade) {
      relevances.put(docid, convertToRealRelevance(grade));
    }
    
    public boolean hasRelevanceForDoc(int docid) {
      return relevances.containsKey(docid);
    }
    
    public double getRelevanceForDoc(int docid) {
      return relevances.get(docid);
    }
    
    private static double convertToRealRelevance(String grade) {
      if (grade.equalsIgnoreCase("Perfect")){
        return 10.0;
      }
      else if (grade.equalsIgnoreCase("Excellent")){
        return 7.0;
      }
      else if (grade.equalsIgnoreCase("Good")){
        return 5.0;
      }
      else if (grade.equalsIgnoreCase("Fair")){
        return 1.0;
      }
      else {  // grade = bad
      	return 0.0;
      }
    }
    
    private static double convertToBinaryRelevance(String grade) {
      if (grade.equalsIgnoreCase("Perfect") ||
          grade.equalsIgnoreCase("Excellent") ||
          grade.equalsIgnoreCase("Good")) {
        return 1.0;
      }
      return 0.0;
    }
  }
  

  public static void main(String[] args) throws IOException {
    Map<String, DocumentRelevances> judgments =
        new HashMap<String, DocumentRelevances>();
    SearchEngine.Check(args.length == 1, "Must provide judgements!");
    readRelevanceJudgments(args[0], judgments);
    evaluateStdin(judgments);
  }

  public static void readRelevanceJudgments(
      String judgeFile, Map<String, DocumentRelevances> judgements)
      throws IOException {
    String line = null;
    BufferedReader reader = new BufferedReader(new FileReader(judgeFile));
    while ((line = reader.readLine()) != null) {
      // Line format: query \t docid \t grade
      Scanner s = new Scanner(line).useDelimiter("\t");
      String query = s.next();
      DocumentRelevances relevances = judgements.get(query);
      if (relevances == null) {
        relevances = new DocumentRelevances();
        judgements.put(query, relevances);
      }
      relevances.addDocument(Integer.parseInt(s.next()), s.next());
      s.close();
    }
    reader.close();
  }

/*  - The input into the Evaluator is via standard input, corresponding to
    the format of the output of the ranker: QUERY<TAB>DOCUMENTID-1<TAB>TITLE<TAB>SCORE
  - It is assumed that the input fed into the EvaluatorHW1 is more than
    or equal to 10 lines. This is because some metrics, e.g, precision at 10,
    require such a minimum amount line of input.
  - It is also assumed that all lines of input has the same query (only considering one query per call).*/
  public static void evaluateStdin(Map<String, DocumentRelevances> judgments) throws IOException {
 
  	BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
    System.out.println("Please provide at least 10 lines of input to be evaluated.");
    
    // Strings in this vector is the output of the retrieval system (RankingMethod)
    // That is, it is in the format: QUERY<TAB>DOCUMENTID-1<TAB>TITLE<TAB>SCORE
    Vector<String> retrievalResults = new Vector<String>();
    
    String fixedQuery = "";
    Vector<String> queries = new Vector<String>();
    String line = null;
    // Assuming that all lines of input have the same query (only considering one query per call)
    while (!(line = reader.readLine()).isEmpty()){
    	Scanner scanner = new Scanner(line).useDelimiter("\t");
    	String currentQuery = scanner.next();
    	queries.add(currentQuery);
    	retrievalResults.add(line);
    	scanner.close();
    }
    
    // Ensuring that all lines have the same query
    if(queries.size() != 1){
    	System.out.println("Input must have the same query (only considering one query per call)");
    	return;
    }
    
    fixedQuery = queries.get(0);
    
    // If the number of input line is less than 10, then return
    // since this is the minimum requirement as some metrics,
    // e.g., precision at 10, need at least 10 lines of input
    if(retrievalResults.size() < 10){
  	System.out.println("Please give at least 10 lines of input");
      return;
    }
    
    String outputString = getEvaluationAsString(fixedQuery, retrievalResults, judgments);
    System.out.println(outputString);
    
    return;
  }
  
  // This method returns a full evaluation for a given vector of string inputs
  // in the format of QUERY<TAB>DOCUMENTID-1<TAB>TITLE<TAB>SCORE.
  // Assuming that all Strings contain the same query (that we process each 
  // query at a time)
  public static String getEvaluationAsString(String query, Vector<String> retrievalResults, Map<String, DocumentRelevances> judgments) throws IOException{
    double precisionAt1 = precisionAtK(retrievalResults, 1, judgments);
    double precisionAt5 = precisionAtK(retrievalResults, 5, judgments);
    double precisionAt10 = precisionAtK(retrievalResults, 10, judgments);
    double recallAt1 = recallAtK(retrievalResults, 1, judgments);
    double recallAt5 = recallAtK(retrievalResults, 5, judgments);
    double recallAt10 = recallAtK(retrievalResults, 10, judgments);
    double F05At1 = F05AtK(retrievalResults, 1, judgments);
    double F05At5 = F05AtK(retrievalResults, 5, judgments);
    double F05At10 = F05AtK(retrievalResults, 10, judgments);
    double[] precisionsAtRecalls = precisionAtRecall(retrievalResults, judgments);
    double averagePrecision = averagePrecision(retrievalResults, judgments);
    double NDCGAt1 = NDCGAtK(retrievalResults, 1, judgments);
    double NDCGAt5 = NDCGAtK(retrievalResults, 5, judgments);
    double NDCGAt10 = NDCGAtK(retrievalResults, 10, judgments);
    double reciprocalRank = reciprocalRank(retrievalResults, judgments);
      
    String output = query+"\n"+
  		  "Precision at 1: "+Double.toString(precisionAt1)+"\n"+
  		  "Precision at 5: "+Double.toString(precisionAt5)+"\n"+
  		  "Precision at 10: "+Double.toString(precisionAt10)+"\n"+
  		  "Recall at 1: "+Double.toString(recallAt1)+"\n"+
  		  "Recall at 5: "+Double.toString(recallAt5)+"\n"+
  		  "Recall at 10: "+Double.toString(recallAt10)+"\n"+
  		  "F05 at 1: "+Double.toString(F05At1)+"\n"+
  		  "F05 at 5: "+Double.toString(F05At5)+"\n"+
  		  "F05 at 10: "+Double.toString(F05At10)+"\n"+
  		  "Precision at Recall 0.0: "+Double.toString(precisionsAtRecalls[0])+"\n"+
  		  "Precision at Recall 0.1: "+Double.toString(precisionsAtRecalls[1])+"\n"+
  		  "Precision at Recall 0.2: "+Double.toString(precisionsAtRecalls[2])+"\n"+
  		  "Precision at Recall 0.3: "+Double.toString(precisionsAtRecalls[3])+"\n"+
  		  "Precision at Recall 0.4: "+Double.toString(precisionsAtRecalls[4])+"\n"+
  		  "Precision at Recall 0.5: "+Double.toString(precisionsAtRecalls[5])+"\n"+
  		  "Precision at Recall 0.6: "+Double.toString(precisionsAtRecalls[6])+"\n"+
  		  "Precision at Recall 0.7: "+Double.toString(precisionsAtRecalls[7])+"\n"+
  		  "Precision at Recall 0.8: "+Double.toString(precisionsAtRecalls[8])+"\n"+
  		  "Precision at Recall 0.9: "+Double.toString(precisionsAtRecalls[9])+"\n"+
  		  "Precision at Recall 1.0: "+Double.toString(precisionsAtRecalls[10])+"\n"+
  		  "Average precision: "+Double.toString(averagePrecision)+"\n"+
  		  "NDCG at 1: "+Double.toString(NDCGAt1)+"\n"+
  		  "NDCG at 5: "+Double.toString(NDCGAt5)+"\n"+
  		  "NDCG at 10: "+Double.toString(NDCGAt10)+"\n"+
  		  "Reciprocal rank: "+Double.toString(reciprocalRank);
								
    return output;
  }
  
  public static double precisionAtK(Vector<String> inputs, int k, Map<String, DocumentRelevances> judgments) throws IOException{
    double RR = 0.0;
    for(int i = 0; i < k; i++){
      Scanner scanner = new Scanner(inputs.get(i)).useDelimiter("\t");
      String query = scanner.next();
      int did = Integer.parseInt(scanner.next());
      if (judgments.containsKey(query) == false){
      	throw new IOException("query not found");
      }
      DocumentRelevances relevances = judgments.get(query);
		  if (relevances.hasRelevanceForDoc(did)){
				if(relevances.getRelevanceForDoc(did) > 1){  // if the relevance value is greater than 1, it is considered as relevant (as suggested in the assignment)
				  RR++;
				}
		  }
    }
    double precision_at_k = RR/k;
    return precision_at_k;
  }
  
  public static double recallAtK(Vector<String> inputs, int k, Map<String, DocumentRelevances> judgments) throws IOException{
    double RR = 0.0;
    double N = 0.0;  // number of relevant document in the given inputs
    
	  // first pass to determine RR (number of relevant documents in the first k inputs)
		for(int i=0; i < k; i++){
		  Scanner scanner = new Scanner(inputs.get(i)).useDelimiter("\t");
		  String query = scanner.next();
		  int did = Integer.parseInt(scanner.next());
      if (judgments.containsKey(query) == false){
      	throw new IOException("query not found");
      }
      DocumentRelevances relevances = judgments.get(query);
      if (relevances.hasRelevanceForDoc(did)){
	    	if(relevances.getRelevanceForDoc(did) > 1){
	    	  RR++;
	    	}
      }
	   }
	   // second pass to determine N (number of relevant documents in the entire input)
		 for(int i=0;i<inputs.size();i++){
		   Scanner scanner = new Scanner(inputs.get(i)).useDelimiter("\t");
		   String query = scanner.next();
		   int did = Integer.parseInt(scanner.next());
	     if (judgments.containsKey(query) == false){
	       throw new IOException("query not found");
	     }
	     DocumentRelevances relevances = judgments.get(query);
	     if (relevances.hasRelevanceForDoc(did)){
	    	 if(relevances.getRelevanceForDoc(did) > 1){
	    	   N++;
	    	 }
		   }
	   }
		 if(N != 0){
	     double recall_at_k = RR/N;
	     return recall_at_k;		
		 }
	   return 0;
	}
  
  public static double F05AtK(Vector<String> inputs, int k, Map<String, DocumentRelevances> judgments) throws IOException{
    double alpha = 0.5;
    double precisionAtK = precisionAtK(inputs, k, judgments);
    double recallAtK = recallAtK(inputs, k, judgments);
    double F05 = 1 / (alpha*(1/precisionAtK)+(1-alpha)*(1/recallAtK));  // formula in slide 1, page 42
    return F05;
  }
  
  // This method returns precision at {0,0.1,...,1.0} recalls.
  public static double[] precisionAtRecall(Vector<String> inputs, Map<String, DocumentRelevances> judgments) throws IOException{
    double[] precisions = new double[11];
    HashMap < Double , Double > p = new HashMap<Double,Double>();  // this maps between recall and precision
    // putting the mapping between recall and precision into p
    for(int i = 0; i < inputs.size(); i++){
		  Scanner scanner = new Scanner(inputs.get(i)).useDelimiter("\t");
		  String query = scanner.next();
		  int did = Integer.parseInt(scanner.next());
	    if (judgments.containsKey(query) == false){
	    	throw new IOException("query not found");
	    }
	    DocumentRelevances relevances = judgments.get(query);
	    if (relevances.hasRelevanceForDoc(did)){
	    	if(relevances.getRelevanceForDoc(did) > 1){
	    	  double recall = recallAtK(inputs, i+1, judgments);
	    	  double precision = precisionAtK(inputs, i+1, judgments);
	    	  p.put(recall, precision);  // putting recall and precision at position i+1 of the given line of input
	    	}
	    }
    }
    
    for(int i = 0; i <= 10; i++){
      precisions[i] = getMaxPrecision(p, ((double)i)/10.0);
    }
    return precisions;
  }
  
  // This helper method takes a mapping between recall and precision as an input
  // and returns the maximum value of precision such that its respective recall
  // is greater or equal than R.
  // Note that p is a mapping between recall and precision of the given lines of input
  public static double getMaxPrecision(HashMap<Double,Double> p, double R){
    double max_P = 0;
    
    // Follows the definition in the textbook, page 316
    for(Double recall : p.keySet()){
    	if(recall >= R){
    		double precision = Double.parseDouble(p.get(recall).toString());
    		if(precision >= max_P){
    			max_P = precision;
    		}
    	}
    }
    
    return max_P;
  }
  
  public static double averagePrecision(Vector<String> inputs, Map<String, DocumentRelevances> judgments) throws IOException{
		double AP = 0.0;
		double RR = 0.0;
		for(int i = 0; i < inputs.size(); i++){
		  Scanner scanner = new Scanner(inputs.get(i)).useDelimiter("\t");
		  String query = scanner.next();
		  int did = Integer.parseInt(scanner.next());
	    if (judgments.containsKey(query) == false){
	    	throw new IOException("query not found");
	    }
	    DocumentRelevances relevances = judgments.get(query);
	    if (relevances.hasRelevanceForDoc(did)){  // follows slide 1, page 51
	    	if(relevances.getRelevanceForDoc(did) > 1){
	    	  RR++;
	    	  AP += RR/(i+1);
	    	}
		  }
	  }
		if(RR!=0){
		  double averagePrecision = AP/RR;
		  return averagePrecision;
		}
		return 0;
  }
  
  public static double NDCGAtK(Vector<String> inputs, int k, Map<String, DocumentRelevances> judgments) throws IOException{
		Vector<Double> rels = new Vector<Double>();  // a vector of relavance values for each line of input
		// get the relevance values
		for(int i = 0; i < k; i++){
		  Scanner scanner = new Scanner(inputs.get(i)).useDelimiter("\t");
		  String query = scanner.next();
		  int did = Integer.parseInt(scanner.next());
	    if (judgments.containsKey(query) == false){
	    	throw new IOException("query not found");
	    }
	    DocumentRelevances relevances = judgments.get(query);
	    if (relevances.hasRelevanceForDoc(did)){
	      rels.add(relevances.getRelevanceForDoc(did));
		  }
	    else{  // assuming that un-judged document receives a grade of "Bad"
	    	rels.add(0.0);
	    }
	  }
		
		// Follows the textbook, page 319-321
		Vector<Double> idealRels = getPerfectRanking(rels);
		double DCG_k = 0.0;
		double IDCG_k = 0.0;
		
		// computing DCG at k
		DCG_k += rels.get(0);
		for(int i = 1; i < k; i++){
		  DCG_k += rels.get(i) / logBase2(i+1);
		}
		// computing IDCG at k
		IDCG_k += idealRels.get(0);
		for(int i = 1; i < k; i++){
		  IDCG_k += idealRels.get(i) / logBase2(i+1);
		}
		if(IDCG_k != 0){
		  double NDCG_k = DCG_k / IDCG_k;
		  return NDCG_k;
		}
	  return 0;
  }
  
  public static double reciprocalRank(Vector<String> inputs, Map<String, DocumentRelevances> judgments) throws IOException{
    for(int i=0;i<inputs.size();i++){
  	  Scanner scanner = new Scanner(inputs.get(i)).useDelimiter("\t");
  	  String query = scanner.next();
  	  int did = Integer.parseInt(scanner.next());
      if (judgments.containsKey(query) == false){
      	throw new IOException("query not found");
      }
      DocumentRelevances relevances = judgments.get(query);
      if (relevances.hasRelevanceForDoc(did)){
        if(relevances.getRelevanceForDoc(did) > 1){
          return 1.0 / ((double)(i+1));
        }
  	  }
    }
    return 0;
  }
  
  // This helper method takes a vector of relevance values as an input
  // and returns a perfect ranking.
  public static Vector<Double> getPerfectRanking(Vector<Double> rels){
    Vector<Double> ideal = new Vector<Double>();
    for(int i=0;i<rels.size();i++){
      ideal.add(rels.get(i));
    }
    Comparator<Double> comp = Collections.reverseOrder(); 		
    Collections.sort(ideal,comp);
    return ideal;
  }
  
  // This helper method returns a log base 2 of x.
  public static double logBase2(double x){
    return Math.log(x)/Math.log(2);
  }
}
