package edu.nyu.cs.cs2580;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.Vector;

/**
 * Evaluator for HW1.
 * 
 * @author fdiaz
 * @author congyu
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
  
  /**
   * Usage: java -cp src edu.nyu.cs.cs2580.Evaluator [judge_file]
   */
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
  public static void evaluateStdin(Map<String, DocumentRelevances> judgments)
      throws IOException {
    BufferedReader reader =
        new BufferedReader(new InputStreamReader(System.in));
    System.out.println("Please provide at least 10 lines of input to be evaluated.");
    
    double RR = 0.0;
    double N = 0.0;
    String line = null;
    while ((line = reader.readLine()) != null) {
      Scanner s = new Scanner(line).useDelimiter("\t");
      String query = s.next();
      int docid = Integer.parseInt(s.next());
      DocumentRelevances relevances = judgments.get(query);
      if (relevances == null) {
        System.out.println("Query \'" + query + "\' not found!");
      } else {
        if (relevances.hasRelevanceForDoc(docid)) {
          RR += relevances.getRelevanceForDoc(docid);
        }
        ++N;
      }
      s.close();
    }
    reader.close();
    System.out.println("Accuracy: " + Double.toString(RR / N));
  }
  
  // This method returns a full evaluation for a given vector of string inputs
  // in the format of QUERY<TAB>DOCUMENTID-1<TAB>TITLE<TAB>SCORE.
  // Assuming that all Strings contain the same query (that we process each 
  // query at a time)
  public static String getEvaluation(String query, Vector<String> retrieval_results, HashMap < String , HashMap < Integer , Double > > relevance_judgments ) throws IOException{
//      double precision_at_1 = precisionAtK(retrieval_results,1,relevance_judgments);
//      double precision_at_5 = precisionAtK(retrieval_results,5,relevance_judgments);
//      double precision_at_10 = precisionAtK(retrieval_results,10,relevance_judgments);
//      double recall_at_1 = recallAtK(retrieval_results,1,relevance_judgments);
//      double recall_at_5 = recallAtK(retrieval_results,5,relevance_judgments);
//      double recall_at_10 = recallAtK(retrieval_results,10,relevance_judgments);
//      double F05_at_1 = F05AtK(retrieval_results, 1, relevance_judgments);
//      double F05_at_5 = F05AtK(retrieval_results, 5, relevance_judgments);
//      double F05_at_10 = F05AtK(retrieval_results, 10, relevance_judgments);
//      double[] precisions_at_recalls = precisionAtRecall(retrieval_results, relevance_judgments);
//      double average_precision = averagePrecision(retrieval_results, relevance_judgments);
//      double NDCG_at_1 = NDCGAtK(retrieval_results, 1, relevance_judgments);
//      double NDCG_at_5 = NDCGAtK(retrieval_results, 5, relevance_judgments);
//      double NDCG_at_10 = NDCGAtK(retrieval_results, 10, relevance_judgments);
//      double reciprocalRank = reciprocalRank(retrieval_results, relevance_judgments);
      
      String output = query+"\t"
//    		  		  Double.toString(precision_at_1)+"\t"+
//    		  		  Double.toString(precision_at_5)+"\t"+
//    		  		  Double.toString(precision_at_10)+"\t"+
//    		  		  Double.toString(recall_at_1)+"\t"+
//    		  		  Double.toString(recall_at_5)+"\t"+
//    		  		  Double.toString(recall_at_10)+"\t"+
//    		  		  Double.toString(F05_at_1)+"\t"+
//    		  		  Double.toString(F05_at_5)+"\t"+
//    		  		  Double.toString(F05_at_10)+"\t"+
//    		  		  Double.toString(precisions_at_recalls[0])+"\t"+
//    		  		  Double.toString(precisions_at_recalls[1])+"\t"+
//    		  		  Double.toString(precisions_at_recalls[2])+"\t"+
//    		  		  Double.toString(precisions_at_recalls[3])+"\t"+
//    		  		  Double.toString(precisions_at_recalls[4])+"\t"+
//    		  		  Double.toString(precisions_at_recalls[5])+"\t"+
//    		  		  Double.toString(precisions_at_recalls[6])+"\t"+
//    		  		  Double.toString(precisions_at_recalls[7])+"\t"+
//    		  		  Double.toString(precisions_at_recalls[8])+"\t"+
//    		  		  Double.toString(precisions_at_recalls[9])+"\t"+
//    		  		  Double.toString(precisions_at_recalls[10])+"\t"+
//    		  		  Double.toString(average_precision)+"\t"+
//    		  		  Double.toString(NDCG_at_1)+"\t"+
//    		  		  Double.toString(NDCG_at_5)+"\t"+
//    		  		  Double.toString(NDCG_at_10)+"\t"+
//    		  		  Double.toString(reciprocalRank)
									;
      return output;
  }
}
