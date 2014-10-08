package edu.nyu.cs.cs2580;

import java.io.IOException;
import java.io.File;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.PrintWriter;
import java.util.Collections;
import java.util.Comparator;
import java.util.Vector;
import java.util.HashMap;
import java.util.Scanner;

class EvaluatorHW1 {

  private static boolean save_Output = false;
	
  public static void main(String[] args) throws IOException {
    HashMap < String , HashMap < Integer , Double > > relevance_judgments =
      new HashMap < String , HashMap < Integer , Double > >();
    if (args.length < 1){
      System.out.println("need to provide relevance_judgments");
      return;
    }
    String p = args[0];
    // first read the relevance judgments into the HashMap
    readRelevanceJudgments(p,relevance_judgments);
        
    // save the evaluation results to files
    if(save_Output){
      saveOutput(relevance_judgments);
    }
    
    // now evaluate the results from stdin
    evaluateStdin(relevance_judgments);
  }

  // This method saves the output of each ranking algorithm into files
  public static void saveOutput(HashMap < String , HashMap < Integer , Double > > relevance_judgments) throws IOException {
  	// please change this path accordingly
	String index_path = "./data/corpus.tsv";
	RankerHW1 ranker = new RankerHW1(index_path);
	
	// please change the input path accordingly
    String input_query_path = "../data/queries.tsv";
    
    // output file names
    String vsm_file_name = "hw1.3-vsm.tsv";  // vector space model
    String ql_file_name = "hw1.3-ql.tsv";  // query likelihood with Jelinek-Mercer smoothing
    String phrase_file_name = "hw1.3-phrase.tsv";  // phrase-based RankingMethod
    String numview_file_name = "hw1.3-numviews.tsv";  // numviews-based RankingMethod
    String linear_file_name = "hw1.3-linear.tsv";  // linear RankingMethod
    
    // output of the evaluator to be written out to each output file
    String vsm_evaluation = "";
    String ql_evaluation = "";
    String phrase_evaluation = "";
    String numview_evaluation = "";
    String linear_evaluation = "";
    
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
    	String vsm_queryResponse = ranker.getQueryResponse(queries.get(i), "COSINE", "TEXT");
        vsm_evaluation += getEvaluation(queries.get(i), convertToVector(vsm_queryResponse), relevance_judgments)+"\n";
    	
    	String ql_queryResponse = ranker.getQueryResponse(queries.get(i), "QL", "TEXT");
        ql_evaluation += getEvaluation(queries.get(i), convertToVector(ql_queryResponse), relevance_judgments)+"\n";
        
        String phrase_queryResponse = ranker.getQueryResponse(queries.get(i), "PHRASE", "TEXT");
        phrase_evaluation += getEvaluation(queries.get(i), convertToVector(phrase_queryResponse), relevance_judgments)+"\n";

        String numviews_queryResponse = ranker.getQueryResponse(queries.get(i), "NUMVIEWS", "TEXT");
        numview_evaluation += getEvaluation(queries.get(i), convertToVector(numviews_queryResponse), relevance_judgments)+"\n";

        String linear_queryResponse = ranker.getQueryResponse(queries.get(i), "LINEAR", "TEXT");
        linear_evaluation += getEvaluation(queries.get(i), convertToVector(linear_queryResponse), relevance_judgments)+"\n";
    }
    
    // writing out to files
    
    File vsm_file = new File(RankerHW1.RESULTS_FOLDER + vsm_file_name);
    PrintWriter vsm_writer = new PrintWriter(vsm_file);
    vsm_writer.write(vsm_evaluation);
    vsm_writer.close();
    
    File ql_file = new File(RankerHW1.RESULTS_FOLDER + ql_file_name);
    PrintWriter ql_writer = new PrintWriter(ql_file);
    ql_writer.write(ql_evaluation);
    ql_writer.close();
    
    File phrase_file = new File(RankerHW1.RESULTS_FOLDER + phrase_file_name);
    PrintWriter phrase_writer = new PrintWriter(phrase_file);
    phrase_writer.write(phrase_evaluation);
    phrase_writer.close();
    
    File numviews_file = new File(RankerHW1.RESULTS_FOLDER + numview_file_name);
    PrintWriter numviews_writer = new PrintWriter(numviews_file);
    numviews_writer.write(numview_evaluation);
    numviews_writer.close();
    
    File linear_file = new File(RankerHW1.RESULTS_FOLDER + linear_file_name);
    PrintWriter linear_writer = new PrintWriter(linear_file);
    linear_writer.write(linear_evaluation);
    linear_writer.close();
  }
  
  public static void readRelevanceJudgments(
    String p,HashMap < String , HashMap < Integer , Double > > relevance_judgments){
    try {
      BufferedReader reader = new BufferedReader(new FileReader(p));
      try {
    	String line = null;
        while ((line = reader.readLine()) != null){
          // parse the query,did,relevance line
          Scanner s = new Scanner(line).useDelimiter("\t");
          String query = s.next();
          int did = Integer.parseInt(s.next());
          String grade = s.next();
          double rel = 0.0;
          // convert to binary relevance using the gain values according to the lecture slide
          if (grade.equals("Perfect")) {
            rel = 10.0;
          }
          else if(grade.equals("Excellent")){
        	rel = 7.0;
          }
          else if(grade.equals("Good")){
        	rel = 5.0;
          }
          else if(grade.equals("Fair")){
        	rel = 1.0;
          }
          else if(grade.equals("Bad")){
        	rel = 0.0;
          }
          if (relevance_judgments.containsKey(query) == false){
            HashMap < Integer , Double > qr = new HashMap < Integer , Double >();
            relevance_judgments.put(query,qr);
          }
          HashMap < Integer , Double > qr = relevance_judgments.get(query);
          qr.put(did,rel);
        }
        System.out.println("Finished loading relevance judgements.");
       } finally {
        reader.close();
      }
    } catch (IOException ioe){
      System.err.println("Oops " + ioe.getMessage());
    }
  }

  
  // - The input into the EvaluatorHW1 is via standard input, corresponding to
  //   the format of the output of the ranker: 
  //   QUERY<TAB>DOCUMENTID-1<TAB>TITLE<TAB>SCORE
  // - It is assumed that the input fed into the EvaluatorHW1 is more than
  //   or equal to 10 lines. This is because some metrics, 
  //   e.g, precision at 10, require such a minimum amount line of input.
  // - It is also assumed that all lines of input has the same query (only considering one query per call).
  public static void evaluateStdin(
    HashMap < String , HashMap < Integer , Double > > relevance_judgments){
    // only consider one query per call    
    try {
      BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
      System.out.println("Please provide at least 10 lines of input to be evaluated.");
      
      // Strings in this vector is the output of the retrieval system (RankingMethod)
      // That is, it is in the format: QUERY<TAB>DOCUMENTID-1<TAB>TITLE<TAB>SCORE
      Vector<String> retrieval_results = new Vector<String>();
      
      String line = null;
      while (!(line = reader.readLine()).isEmpty()){
        retrieval_results.add(line);
      }
      
      // If the number of input line is less than 10, then return
      // since this is the minimum requirement as some metrics,
      // e.g., precision at 10, need at least 10 lines of input
      if(retrieval_results.size()<10){
    	System.out.println("Please give at least 10 lines of input");
        return;
      }
      
      // This is just to get the query (which is assumed to be the same for all
      // lines of input) from the first line of input
      String temp_line = retrieval_results.get(0);
      Scanner temp_scanner = new Scanner(temp_line).useDelimiter("\t");
      String query = temp_scanner.next();
      temp_scanner.close();
      
      String output_evaluation = getEvaluation(query, retrieval_results, relevance_judgments);
      
      System.out.println(output_evaluation);
      
    } catch (Exception e){
      System.err.println("Error:" + e.getMessage());
    }
  }
  
  // This method returns a full evaluation for a given vector of string inputs
  // in the format of QUERY<TAB>DOCUMENTID-1<TAB>TITLE<TAB>SCORE.
  // Assuming that all Strings contain the same query (that we process each 
  // query at a time)
  public static String getEvaluation(String query, Vector<String> retrieval_results, HashMap < String , HashMap < Integer , Double > > relevance_judgments ) throws IOException{
      double precision_at_1 = precisionAtK(retrieval_results,1,relevance_judgments);
      double precision_at_5 = precisionAtK(retrieval_results,5,relevance_judgments);
      double precision_at_10 = precisionAtK(retrieval_results,10,relevance_judgments);
      double recall_at_1 = recallAtK(retrieval_results,1,relevance_judgments);
      double recall_at_5 = recallAtK(retrieval_results,5,relevance_judgments);
      double recall_at_10 = recallAtK(retrieval_results,10,relevance_judgments);
      double F05_at_1 = F05AtK(retrieval_results, 1, relevance_judgments);
      double F05_at_5 = F05AtK(retrieval_results, 5, relevance_judgments);
      double F05_at_10 = F05AtK(retrieval_results, 10, relevance_judgments);
      double[] precisions_at_recalls = precisionAtRecall(retrieval_results, relevance_judgments);
      double average_precision = averagePrecision(retrieval_results, relevance_judgments);
      double NDCG_at_1 = NDCGAtK(retrieval_results, 1, relevance_judgments);
      double NDCG_at_5 = NDCGAtK(retrieval_results, 5, relevance_judgments);
      double NDCG_at_10 = NDCGAtK(retrieval_results, 10, relevance_judgments);
      double reciprocalRank = reciprocalRank(retrieval_results, relevance_judgments);
      
      String output = query+"\t"+
    		  		  Double.toString(precision_at_1)+"\t"+
    		  		  Double.toString(precision_at_5)+"\t"+
    		  		  Double.toString(precision_at_10)+"\t"+
    		  		  Double.toString(recall_at_1)+"\t"+
    		  		  Double.toString(recall_at_5)+"\t"+
    		  		  Double.toString(recall_at_10)+"\t"+
    		  		  Double.toString(F05_at_1)+"\t"+
    		  		  Double.toString(F05_at_5)+"\t"+
    		  		  Double.toString(F05_at_10)+"\t"+
    		  		  Double.toString(precisions_at_recalls[0])+"\t"+
    		  		  Double.toString(precisions_at_recalls[1])+"\t"+
    		  		  Double.toString(precisions_at_recalls[2])+"\t"+
    		  		  Double.toString(precisions_at_recalls[3])+"\t"+
    		  		  Double.toString(precisions_at_recalls[4])+"\t"+
    		  		  Double.toString(precisions_at_recalls[5])+"\t"+
    		  		  Double.toString(precisions_at_recalls[6])+"\t"+
    		  		  Double.toString(precisions_at_recalls[7])+"\t"+
    		  		  Double.toString(precisions_at_recalls[8])+"\t"+
    		  		  Double.toString(precisions_at_recalls[9])+"\t"+
    		  		  Double.toString(precisions_at_recalls[10])+"\t"+
    		  		  Double.toString(average_precision)+"\t"+
    		  		  Double.toString(NDCG_at_1)+"\t"+
    		  		  Double.toString(NDCG_at_5)+"\t"+
    		  		  Double.toString(NDCG_at_10)+"\t"+
    		  		  Double.toString(reciprocalRank);
      return output;
  }
  
  // This helper method takes an input string containing multiple lines
  // and return a vector consisting each line of string
  public static Vector<String> convertToVector(String input){
	Vector<String> v = new Vector<String>();
    Scanner scanner = new Scanner(input).useDelimiter("\n");
    while(scanner.hasNext()){
      v.add(scanner.next().trim());
    }
    return v;
  }
  
  public static double precisionAtK(Vector<String> inputs, int k, HashMap < String , HashMap < Integer , Double > > relevance_judgments) throws IOException{
    double RR = 0.0;
	for(int i=0;i<k;i++){
      Scanner scanner = new Scanner(inputs.get(i)).useDelimiter("\t");
      String query = scanner.next();
      int did = Integer.parseInt(scanner.next());
      String title = scanner.next();
      double rel = Double.parseDouble(scanner.next());
	  if (relevance_judgments.containsKey(query) == false){
	    throw new IOException("query not found");
	  }
	  HashMap < Integer , Double > qr = relevance_judgments.get(query);
	  if (qr.containsKey(did) != false){
		if(qr.get(did)>1){  // if the relevance value is greater than 1, it is considered as relevant (as suggested in the assignment)
		  RR++;
		}
	  }
    }
	double precision_at_k = RR/k;
	return precision_at_k;
  }
  
  public static double recallAtK(Vector<String> inputs, int k, HashMap < String , HashMap < Integer , Double > > relevance_judgments) throws IOException{
    double RR = 0.0;
    double N = 0.0;  // number of relevant document in the given inputs
    
    // first pass to determine RR (number of relevant documents in the first k inputs)
	for(int i=0;i<k;i++){
	  Scanner scanner = new Scanner(inputs.get(i)).useDelimiter("\t");
	  String query = scanner.next();
	  int did = Integer.parseInt(scanner.next());
	  String title = scanner.next();
	  double rel = Double.parseDouble(scanner.next());
      if (relevance_judgments.containsKey(query) == false){
		throw new IOException("query not found");
      }
      HashMap < Integer , Double > qr = relevance_judgments.get(query);
      if (qr.containsKey(did) != false){
    	if(qr.get(did)>1){
    	  RR++;
    	}
	  }
    }
    // second pass to determine N (number of relevant documents in the entire input)
	for(int i=0;i<inputs.size();i++){
	  Scanner scanner = new Scanner(inputs.get(i)).useDelimiter("\t");
	  String query = scanner.next();
	  int did = Integer.parseInt(scanner.next());
	  String title = scanner.next();
	  double rel = Double.parseDouble(scanner.next());
      if (relevance_judgments.containsKey(query) == false){
		throw new IOException("query not found");
      }
      HashMap < Integer , Double > qr = relevance_judgments.get(query);
      if (qr.containsKey(did) != false){
    	if(qr.get(did)>1){
    	  N++;
    	}
	  }
    }
	if(N!=0){
      double recall_at_k = RR/N;
      return recall_at_k;		
	}
    return 0;
  }
  
  public static double F05AtK(Vector<String> inputs, int k, HashMap < String , HashMap < Integer , Double > > relevance_judgments) throws IOException{
    double alpha = 0.5;
	double precision_at_k = precisionAtK(inputs, k, relevance_judgments);
    double recall_at_k = recallAtK(inputs, k, relevance_judgments);
    double F_05 = 1 / (alpha*(1/precision_at_k)+(1-alpha)*(1/recall_at_k));  // formula in slide 1, page 42
    return F_05;
  }
  
  // This method returns precision at {0,0.1,...,1.0} recalls.
  public static double[] precisionAtRecall(Vector<String> inputs, HashMap < String , HashMap < Integer , Double > > relevance_judgments) throws IOException{
    double[] precisions = new double[11];
    HashMap < Double , Double > p = new HashMap<Double,Double>();  // this maps between recall and precision
	// putting the mapping between recall and precision into p
    for(int i=0;i<inputs.size();i++){
	  Scanner scanner = new Scanner(inputs.get(i)).useDelimiter("\t");
	  String query = scanner.next();
	  int did = Integer.parseInt(scanner.next());
	  String title = scanner.next();
	  double rel = Double.parseDouble(scanner.next());
      if (relevance_judgments.containsKey(query) == false){
		throw new IOException("query not found");
      }
      HashMap < Integer , Double > qr = relevance_judgments.get(query);
      if (qr.containsKey(did) != false){
    	if(qr.get(did)>1){
    	  double recall = recallAtK(inputs, i+1, relevance_judgments);
    	  double precision = precisionAtK(inputs, i+1, relevance_judgments);
    	  p.put(recall, precision);  // putting recall and precision at position i+1 of the given line of input
    	}
	  }
    }
    
    for(int i=0;i<=10;i++){
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
  
  public static double averagePrecision(Vector<String> inputs, HashMap < String , HashMap < Integer , Double > > relevance_judgments) throws IOException{
	double AP = 0.0;
	double RR = 0.0;
	for(int i=0;i<inputs.size();i++){
	  Scanner scanner = new Scanner(inputs.get(i)).useDelimiter("\t");
	  String query = scanner.next();
	  int did = Integer.parseInt(scanner.next());
	  String title = scanner.next();
	  double rel = Double.parseDouble(scanner.next());
      if (relevance_judgments.containsKey(query) == false){
		throw new IOException("query not found");
      }
      HashMap < Integer , Double > qr = relevance_judgments.get(query);
      if (qr.containsKey(did) != false){  // follows slide 1, page 51
    	if(qr.get(did)>1){
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
  
  public static double NDCGAtK(Vector<String> inputs, int k, HashMap < String , HashMap < Integer , Double > > relevance_judgments) throws IOException{
	Vector<Double> rels = new Vector<Double>();  // a vector of relavance values for each line of input
	// get the relevance values
	for(int i=0;i<k;i++){
	  Scanner scanner = new Scanner(inputs.get(i)).useDelimiter("\t");
	  String query = scanner.next();
	  int did = Integer.parseInt(scanner.next());
	  String title = scanner.next();
	  double rel = Double.parseDouble(scanner.next());
      if (relevance_judgments.containsKey(query) == false){
		throw new IOException("query not found");
      }
      HashMap < Integer , Double > qr = relevance_judgments.get(query);
      if (qr.containsKey(did) != false){
        rels.add(qr.get(did));
	  }
      else{  // assuming that un-judged document receives a grade of "Bad"
    	rels.add(0.0);
      }
    }
	
	// Follows the textbook, page 319-321
	Vector<Double> ideal_rels = getPerfectRanking(rels);
	double DCG_k = 0.0;
	double IDCG_k = 0.0;
	
	// computing DCG at k
	DCG_k += rels.get(0);
	for(int i=1;i<k;i++){
	  DCG_k += rels.get(i)/logBase2(i+1);
	}
	// computing IDCG at k
	IDCG_k += ideal_rels.get(0);
	for(int i=1;i<k;i++){
	  IDCG_k += ideal_rels.get(i)/logBase2(i+1);
	}
	if(IDCG_k!=0){
	  double NDCG_k = DCG_k/IDCG_k;
	  return NDCG_k;
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
  
  public static double reciprocalRank(Vector<String> inputs, HashMap < String , HashMap < Integer , Double > > relevance_judgments) throws IOException{
    for(int i=0;i<inputs.size();i++){
  	  Scanner scanner = new Scanner(inputs.get(i)).useDelimiter("\t");
  	  String query = scanner.next();
  	  int did = Integer.parseInt(scanner.next());
  	  String title = scanner.next();
  	  double rel = Double.parseDouble(scanner.next());
      if (relevance_judgments.containsKey(query) == false){
  		throw new IOException("query not found");
      }
      HashMap < Integer , Double > qr = relevance_judgments.get(query);
      if (qr.containsKey(did) != false){
        if(qr.get(did)>1){
          return 1.0/((double)(i+1));
        }
  	  }
    }
    return 0;
  }
  
  // This helper method returns a log base 2 of x.
  public static double logBase2(double x){
    return Math.log(x)/Math.log(2);
  }
  
}
