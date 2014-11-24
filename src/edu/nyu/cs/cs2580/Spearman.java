package edu.nyu.cs.cs2580;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import static com.google.common.base.Preconditions.*;

import java.io.*;
import java.util.*;

/**
 * This is the main entry class for the Spearman coefficient
 *
 * Usage (must be running from the parent directory of src):
 *  0) Compiling
 *   javac src/edu/nyu/cs/cs2580/*.java
 *  1) Running
 *   java -cp src edu.nyu.cs.cs2580.Spearman <PATH-TO-PAGERANK> <PATH-TO-NUMVIEWS>
 */

public class Spearman
{
  static String pathToPagerank = null;
  static String pathToNumviews = null;
  static Scanner scanner = null;

  public static void main(String[] args) throws Exception {
    checkArgument(checkNotNull(args).length == 2, "Fatal error: needs 2 parameters.");

    pathToPagerank = args[0];
    pathToNumviews = args[1];

    // collect PageRank and Numviews data
    Map<String, Float> pageRanks = Maps.newHashMap();
    Map<String, Integer> numViews = Maps.newHashMap();

    scanner = new Scanner(new BufferedReader(new FileReader(pathToNumviews)));
    while(scanner.hasNextLine()) {
      String[] line = scanner.nextLine().split("\\s+");
      checkState(line.length == 2);
      numViews.put(line[0], Integer.parseInt(line[1]));
    }
    scanner.close();

    scanner = new Scanner(new BufferedReader(new FileReader(pathToPagerank)));
    while(scanner.hasNextLine()) {
      String[] line = scanner.nextLine().split("\\s+");
      checkState(line.length == 2);
      if(numViews.containsKey(line[0])) {
        pageRanks.put(line[0], Float.parseFloat(line[1]));
      }
    }
    scanner.close();

    // Verify assumption that pageRanks pages and numViews pages are the same.
    checkState(pageRanks.size() == numViews.size(), "Different number of pages for numviews and pagerank.");
    for(String page : pageRanks.keySet()) {
      checkState(numViews.containsKey(page), "Page " + page + " does not have numViews.");
    }

    // assign rank for each document
    List<Float> x_k = assignRank(pageRanks);
    List<Float> y_k = assignRank(numViews);

    // calculate Spearman coefficient
    double rho = spearmanCoefficient(x_k,y_k);
    System.out.println(rho);
  }

  public static double spearmanCoefficient(List<Float> x_k, List<Float> y_k){
    // calculate Spearman coefficient
    float z = 0;
    for(int i = 0; i < x_k.size(); i++) {
      z += x_k.get(i);
    }
    z /= x_k.size();

    float top = 0, bottom1 = 0, bottom2 = 0;
    for(int i = 0; i < x_k.size(); i++) {
      top     += (x_k.get(i) - z) * (y_k.get(i) - z);
      bottom1 += (x_k.get(i) - z) * (x_k.get(i) - z);
      bottom2 += (y_k.get(i) - z) * (y_k.get(i) - z);
    }
    double rho = top / Math.sqrt(bottom1 * bottom2);
    return rho;
  }

  public static <V extends Comparable<V>> List<Float> assignRank(Map<String, V> values) {
    return assignRankWithTieAveraging(values);
  }

  public static <V extends Comparable<V>> List<Float> assignRankWithTieAveraging(Map<String, V> values) {
    Map<String, Float> ranks = Maps.newHashMap();
    List<Map.Entry<String, V>> sortedValues = Utils.sortByValues(values, true);
    int actualRank = 1;

    for(int i = 0; i < sortedValues.size(); i++) {
      float calculatedRank = actualRank++;
      Set<String> duplicates = Sets.newHashSet(sortedValues.get(i).getKey());
      while(i+1 < sortedValues.size()
            && sortedValues.get(i+1).getValue().compareTo(sortedValues.get(i).getValue()) == 0) {
        calculatedRank += actualRank++;
        duplicates.add(sortedValues.get(i+1).getKey());
        i++;
      }
      for(String key : duplicates) {
        ranks.put(key, calculatedRank / duplicates.size());
      }
    }
    List<Map.Entry<String, Float>> sortedResults = Utils.sortByKeys(ranks, false);
    List<Float> results = Lists.newArrayList();
    for(Map.Entry<String, Float> result : sortedResults) {
      results.add(result.getValue());
    }
    checkState(values.size() == results.size(), "Ranks incorrectly generated.");
    return results;
  }

  public static <V extends Comparable<V>> List<Float> assignRankWithTieBreaking(Map<String, V> values) {
    Map<String, Float> ranks = Maps.newHashMap();
    List<Map.Entry<String, V>> sortedValues = Utils.sortByValuesThenKeys(values, true, false);
    int actualRank = 1;

    for(Map.Entry<String, V> entry : sortedValues) {
      ranks.put(entry.getKey(), (float)actualRank++);
    }

    List<Map.Entry<String, Float>> sortedResults = Utils.sortByKeys(ranks, false);
    List<Float> results = Lists.newArrayList();
    for(Map.Entry<String, Float> result : sortedResults) {
      results.add(result.getValue());
    }
    checkState(values.size() == results.size(), "Ranks incorrectly generated.");
    return results;
  }
}
