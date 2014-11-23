package edu.nyu.cs.cs2580;

import com.google.common.collect.Lists;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * Created by andreidinuionita on 11/20/14.
 */
public class Utils {

  // This helper method sorts the given map by value.
  public static <K,V extends Comparable<V>> List<Map.Entry<K,V>>
  sortByValues(Map<K, V> map, final boolean desc) {
    Comparator<Map.Entry<K,V>> byMapValues = new Comparator<Map.Entry<K,V>>() {
      int reverse = desc ? -1 : 1;
      @Override
      public int compare(Map.Entry<K,V> left, Map.Entry<K,V> right) {
        return left.getValue().compareTo(right.getValue()) * reverse;
      }
    };
    List<Map.Entry<K,V>> sortedMap = Lists.newArrayList(map.entrySet());
    Collections.sort(sortedMap, byMapValues);
    return sortedMap;
  }

  // This helper method sorts the given map by key.
  public static <K extends Comparable<K>, V> List<Map.Entry<K,V>>
  sortByKeys(Map<K, V> map, final boolean desc) {
    Comparator<Map.Entry<K,V>> byMapValues = new Comparator<Map.Entry<K,V>>() {
      int reverse = desc ? -1 : 1;
      @Override
      public int compare(Map.Entry<K,V> left, Map.Entry<K,V> right) {
        return left.getKey().compareTo(right.getKey()) * reverse;
      }
    };
    List<Map.Entry<K,V>> sortedMap = Lists.newArrayList(map.entrySet());
    Collections.sort(sortedMap, byMapValues);
    return sortedMap;
  }

  // This helper method sorts the given map by value, breaking ties by keys.
  public static <K extends Comparable<K>, V extends Comparable<V>> List<Map.Entry<K,V>>
  sortByValuesThenKeys(Map<K, V> map, final boolean descVals, final boolean descKeys) {
    Comparator<Map.Entry<K,V>> byMapValues = new Comparator<Map.Entry<K,V>>() {
      int reverseVals = descVals ? -1 : 1;
      int reverseKeys = descKeys ? -1 : 1;
      @Override
      public int compare(Map.Entry<K,V> left, Map.Entry<K,V> right) {
        int comparisonCode = left.getValue().compareTo(right.getValue()) * reverseVals;
        if(comparisonCode == 0) {
          comparisonCode = left.getKey().compareTo(right.getKey()) * reverseKeys;
        }
        return comparisonCode;
      }
    };
    List<Map.Entry<K,V>> sortedMap = Lists.newArrayList(map.entrySet());
    Collections.sort(sortedMap, byMapValues);
    return sortedMap;
  }

  public static <K extends Comparable<K>, V extends Comparable<V>> List<Map.Entry<K,V>>
  sortByValuesThenKeys(Map<K, V> map) {
    return sortByValuesThenKeys(map, false, false);
  }
}
