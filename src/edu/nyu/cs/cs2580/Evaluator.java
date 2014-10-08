package edu.nyu.cs.cs2580;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

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
      relevances.put(docid, convertToBinaryRelevance(grade));
    }
    
    public boolean hasRelevanceForDoc(int docid) {
      return relevances.containsKey(docid);
    }
    
    public double getRelevanceForDoc(int docid) {
      return relevances.get(docid);
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

  public static void evaluateStdin(Map<String, DocumentRelevances> judgments)
      throws IOException {
    BufferedReader reader =
        new BufferedReader(new InputStreamReader(System.in));
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
}
