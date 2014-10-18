package edu.nyu.cs.cs2580;

import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.RandomAccessFile;
import java.util.*;

import static org.junit.Assert.*;

public class IndexerInvertedCompressedTest {

  IndexerInvertedCompressed _indexer;
  File testDirectory;

  @Before
  public void setUp() throws Exception {
    SearchEngine.Options options = new SearchEngine.Options("conf/engine.conf");
    _indexer = new IndexerInvertedCompressed(options);
    testDirectory = new File("testDirectory");
    testDirectory.mkdir();
  }

  @Test
  public void testUpdatePostingsLists() throws Exception {
    Vector<Integer> docTokensAsInteger1 = new Vector<Integer>(Lists.newArrayList(
      1, 2, 1, 2, 1, 2, 6, 234, 8, 767, 3, 9, 8
    ));
    Vector<Integer> docTokensAsInteger2 = new Vector<Integer>(Lists.newArrayList(
      6, 7, 7, 3, 6, 7, 1, 6
    ));
    _indexer._utilityPrevDocId = new HashMap<Integer, Integer>();
    _indexer.updatePostingsLists(7, docTokensAsInteger1);
    _indexer.updatePostingsLists(12, docTokensAsInteger2);
    File testFile = new File(testDirectory, "miniindex");
    _indexer.dumpUtilityIndexToFileAndClearFromMemory(testFile.getAbsolutePath());

    DataInputStream indexFileDIS = new DataInputStream(new FileInputStream(testFile));
    _indexer._indexOffset = FileUtils.loadFromFileIntoIndex(indexFileDIS, _indexer._index);
    indexFileDIS.close();
    _indexer._indexRAF = new RandomAccessFile(testFile, "r");

    List<Integer> postingList = _indexer.postingsListForWord(1);
    assertEquals(Lists.newArrayList(7, 3, 0, 2, 4, 12, 1, 6), postingList);

    postingList = _indexer.postingsListForWord(2);
    assertEquals(Lists.newArrayList(7, 3, 1, 3, 5), postingList);

    postingList = _indexer.postingsListForWord(3);
    assertEquals(Lists.newArrayList(7, 1, 10, 12, 1, 3), postingList);

    postingList = _indexer.postingsListForWord(234);
    assertEquals(Lists.newArrayList(7, 1, 7), postingList);

    postingList = _indexer.postingsListForWord(767);
    assertEquals(Lists.newArrayList(7, 1, 9), postingList);

    postingList = _indexer.postingsListForWord(6);
    assertEquals(Lists.newArrayList(7, 1, 6, 12, 3, 0, 4, 7), postingList);

    postingList = _indexer.postingsListForWord(7);
    assertEquals(Lists.newArrayList(12, 3, 1, 2, 5), postingList);

    postingList = _indexer.postingsListForWord(8);
    assertEquals(Lists.newArrayList(7, 2, 8, 12), postingList);

    postingList = _indexer.postingsListForWord(9);
    assertEquals(Lists.newArrayList(7, 1, 11), postingList);
  }

  @After
  public void tearDown() throws Exception {
    String files[] = testDirectory.list();

    for (String temp : files) {
      File fileDelete = new File(testDirectory, temp);
      fileDelete.delete();
    }
    testDirectory.delete();
  }
}