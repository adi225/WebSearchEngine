package edu.nyu.cs.cs2580;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.List;
import java.util.Scanner;
import java.util.Set;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

public class CorpusAnalyzerPagerankTest {

  String corpusDirectory = new String("tests/pagerankcorpus/");
  CorpusAnalyzerPagerank prAnalyzer;

  @Before
  public void setup() throws Exception {
    SearchEngine.Options options = new SearchEngine.Options("conf/engine.conf");
    options._corpusPrefix = corpusDirectory;
    prAnalyzer = new CorpusAnalyzerPagerank(options);
  }

  @Test
  public void testPrepare() throws Exception {
    prAnalyzer.prepare();

    BiMap<String, Integer> expectedDocs = HashBiMap.create();
    expectedDocs.put("A", 0);
    expectedDocs.put("B", 1);
    expectedDocs.put("C", 2);
    expectedDocs.put("D", 3);

    assertEquals(expectedDocs, prAnalyzer._documents);

    int[] expectedOutlinks = new int[expectedDocs.size()];
    expectedOutlinks[0] = 3;
    expectedOutlinks[1] = 2;
    expectedOutlinks[2] = 1;
    expectedOutlinks[3] = 2;

    assertEquals(expectedOutlinks.length, prAnalyzer.outlinks.length);
    for(int i = 0; i < expectedOutlinks.length; i++) {
      assertEquals(expectedOutlinks[i], prAnalyzer.outlinks[i]);
    }

    List<Set<Integer>> expectedIAL = Lists.newArrayList();
    expectedIAL.add(Sets.newHashSet(2, 3));
    expectedIAL.add(Sets.newHashSet(0));
    expectedIAL.add(Sets.newHashSet(0, 1, 3));
    expectedIAL.add(Sets.newHashSet(0, 1));

    assertEquals(expectedIAL, prAnalyzer.invertedAdjacencyList);
  }

  @Test
  public void testCompute() throws Exception {
    prAnalyzer.prepare();
    prAnalyzer.compute();

    double[] expectedPageRank = new double[prAnalyzer._documents.size()];
    expectedPageRank[0] = 0.413125;
    expectedPageRank[1] = 0.13375;
    expectedPageRank[2] = 0.274375;
    expectedPageRank[3] = 0.17875;

    File pageRankFile = new File(prAnalyzer._options._indexPrefix + "/pagerank");
    Scanner scanner = new Scanner(pageRankFile);
    double[] pageRank = new double[expectedPageRank.length];
    for(int i = 0; i < pageRank.length; i++) {
      String[] tokens = scanner.nextLine().split(" ");
      pageRank[i] = Double.parseDouble(tokens[1]);
    }
    assertFalse(scanner.hasNextLine());

    for(int i = 0; i < expectedPageRank.length; i++) {
      assertEquals(expectedPageRank[i], pageRank[i], 0.001);
    }
  }

}