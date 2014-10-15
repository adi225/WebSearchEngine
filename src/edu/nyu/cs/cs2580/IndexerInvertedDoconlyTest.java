package edu.nyu.cs.cs2580;

import org.junit.*;

import java.io.*;
import java.util.*;

import static org.junit.Assert.*;

public class IndexerInvertedDoconlyTest {

  File testDirectory;

  @org.junit.Before
  public void setUp() throws Exception {
    testDirectory = new File("testDirectory");
    testDirectory.mkdir();
  }

  @Test
  public void testReadWriteToFileOneObject() throws Exception {
    Map<Integer, String> map = new HashMap<Integer, String>();
    map.put(1, "a");
    map.put(2, "b");
    map.put(3, "c");
    map.put(4, "d");

    File file = new File(testDirectory, "testFileOJIHUIUYGJOI");
    long writeOffset = IndexerInvertedDoconly.writeObjectToFile(map, file);

    List<Object> list = new ArrayList<Object>();
    long readOffset = IndexerInvertedDoconly.readObjectsFromFileIntoList(file, list);
    assertEquals(file.length(), writeOffset);
    assertEquals(file.length(), readOffset);
    Map<Integer, String> mapConfirm = (Map<Integer, String>) list.get(0);

    assertEquals(map.get(1), mapConfirm.get(1));
    assertEquals(map.get(2), mapConfirm.get(2));
    assertEquals(map.get(3), mapConfirm.get(3));
    assertEquals(map.get(4), mapConfirm.get(4));
    assertEquals(map.size(), mapConfirm.size());
  }

  @Test
  public void testReadWriteToFileMultipleObjects() throws Exception {
    Map<Integer, String> map = new HashMap<Integer, String>();
    map.put(1, "a");
    map.put(2, "b");
    map.put(3, "c");
    map.put(4, "d");

    Vector<Long> vector = new Vector<Long>();
    vector.add(5L);
    vector.add(6L);
    vector.add(7L);

    List<Object> list = new ArrayList<Object>();
    list.add(map);
    list.add(vector);

    File file = new File(testDirectory, "testFileOJIHUIUYGJOI");
    long writeOffset = IndexerInvertedDoconly.writeObjectsToFile(list, file);

    List<Object> list2 = new ArrayList<Object>();
    long readOffset = IndexerInvertedDoconly.readObjectsFromFileIntoList(file, list2);
    assertEquals(file.length(), writeOffset);
    assertEquals(file.length(), readOffset);

    Map<Integer, String> mapConfirm = (Map<Integer, String>) list2.get(0);

    assertEquals(map.get(1), mapConfirm.get(1));
    assertEquals(map.get(2), mapConfirm.get(2));
    assertEquals(map.get(3), mapConfirm.get(3));
    assertEquals(map.get(4), mapConfirm.get(4));
    assertEquals(map.size(), mapConfirm.size());

    Vector<Long> vectorConfirm = (Vector<Long>) list2.get(1);

    assertEquals(vector.get(0), vectorConfirm.get(0));
    assertEquals(vector.get(1), vectorConfirm.get(1));
    assertEquals(vector.get(2), vectorConfirm.get(2));
    assertEquals(vector.size(), vectorConfirm.size());
  }

  @Test
  public void testWriteOffsetSameFile() throws Exception {
    Map<Integer, String> map = new HashMap<Integer, String>();
    map.put(1, "a");
    map.put(2, "b");
    map.put(3, "c");
    map.put(4, "d");

    Vector<Long> vector = new Vector<Long>();
    vector.add(5L);
    vector.add(6L);
    vector.add(7L);

    File file = new File(testDirectory, "testFileOJIHUIUYGJOI");
    long writeOffset1 = IndexerInvertedDoconly.writeObjectToFile(map, file);
    assertEquals(file.length(), writeOffset1);
    long writeOffset2 = IndexerInvertedDoconly.writeObjectToFile(vector, file);
    assertEquals(file.length(), writeOffset2);
  }

  @Test
  public void testCopyFileStream() throws Exception {
    Map<Integer, String> map = new HashMap<Integer, String>();
    map.put(1, "a");
    map.put(2, "b");
    map.put(3, "c");
    map.put(4, "d");

    File file = new File(testDirectory, "testFileOJIHUIUYGJOI");
    File file2 = new File(testDirectory, "testFileOJIHhtrhtJOI");
    long offs = IndexerInvertedDoconly.writeObjectToFile(map, file);
    assertEquals(file.length(), offs);
    offs = IndexerInvertedDoconly.writeObjectToFile(map, file2);
    assertEquals(file2.length(), offs);

    FileOutputStream partialIndexFileOS = new FileOutputStream(file2, true);
    FileInputStream partialIndexFileAuxIS = new FileInputStream(file);
    IndexerInvertedDoconly.copyStream(partialIndexFileAuxIS, partialIndexFileOS);
    partialIndexFileAuxIS.close();
    partialIndexFileOS.close();

    assertEquals(file.length() * 2, file2.length());

    List<Object> list = new ArrayList<Object>();
    long offset = IndexerInvertedDoconly.readObjectsFromFileIntoList(file2, list);
    assertEquals(offset, file.length());
    Map<Integer, String> mapConfirm = (Map<Integer, String>) list.get(0);

    assertEquals(map.get(1), mapConfirm.get(1));
    assertEquals(map.get(2), mapConfirm.get(2));
    assertEquals(map.get(3), mapConfirm.get(3));
    assertEquals(map.get(4), mapConfirm.get(4));
    assertEquals(map.size(), mapConfirm.size());
  }

  @Test
  public void testPartialIndexDumpAndRead() throws Exception {
    Map<Integer, List<Integer>> fullIndex = new HashMap<Integer, List<Integer>>();
    List<Integer> a = Arrays.asList(new Integer[] { 1, 2, 3, 4, 8});
    List<Integer> b = Arrays.asList(new Integer[] { 5, 6, 9});
    List<Integer> c = Arrays.asList(new Integer[] { 7, 8, 9});
    fullIndex.put(10, a);
    fullIndex.put(20, b);
    fullIndex.put(30, c);

    File file = new File(testDirectory, "testFileOJIHUIUYGJOI");
    long writeOffset = IndexerInvertedDoconly.dumpIndexToFile(fullIndex, file);

    DataInputStream fileDIS = new DataInputStream(new FileInputStream(file));
    Map<Integer, IndexerInvertedDoconly.FileRange> index = new HashMap<Integer, IndexerInvertedDoconly.FileRange>();
    long readOffset = IndexerInvertedDoconly.loadFromFileIntoIndex(fileDIS, index);
    assertEquals(writeOffset, readOffset);
    assertEquals(fullIndex.keySet(), index.keySet());

    RandomAccessFile indexFile = new RandomAccessFile(file, "r");

    for(int key : index.keySet()) {
      List<Integer> list = fullIndex.get(key);
      List<Integer> listConfirm = new ArrayList<Integer>();
      IndexerInvertedDoconly.FileRange fileRange = index.get(key);
      indexFile.seek(readOffset + fileRange.offset);
      for(int i = 0; i < fileRange.length; i++) {
        listConfirm.add(indexFile.readInt());
      }
      assertEquals(list, listConfirm);
    }
    indexFile.close();
  }

  @Test
  public void testMergePartialIndexes() throws Exception {
    Map<Integer, List<Integer>> fullIndex1 = new HashMap<Integer, List<Integer>>();
    List<Integer> a = Arrays.asList(new Integer[] { 1, 2, 3, 4, 8});
    List<Integer> b = Arrays.asList(new Integer[] { 5, 6, 9});
    List<Integer> c = Arrays.asList(new Integer[] { 7, 8, 9});
    fullIndex1.put(10, a);
    fullIndex1.put(20, b);
    fullIndex1.put(30, c);

    Map<Integer, List<Integer>> fullIndex2 = new HashMap<Integer, List<Integer>>();
    List<Integer> d = Arrays.asList(new Integer[] { 11, 12, 13, 14, 18});
    List<Integer> e = Arrays.asList(new Integer[] { 25, 26, 29});
    List<Integer> f = Arrays.asList(new Integer[] { 37, 38, 39});
    fullIndex2.put(15, d);
    fullIndex2.put(20, e);
    fullIndex2.put(25, f);

    File file1 = new File(testDirectory, "testFileOJIHUIUYGJOI");
    IndexerInvertedDoconly.dumpIndexToFile(fullIndex1, file1);
    File file2 = new File(testDirectory, "testFileOJYUGTRHUGUJOI");
    IndexerInvertedDoconly.dumpIndexToFile(fullIndex2, file2);
    File mergedFile = new File(testDirectory, "testFileOJYUGTRHUFYGUGTUJOI");
    Map<Integer, IndexerInvertedDoconly.FileRange> mergedIndex = new HashMap<Integer, IndexerInvertedDoconly.FileRange>();
    long offset = IndexerInvertedDoconly.mergeFilesIntoIndexAndFile(file1, file2, mergedIndex, mergedFile);
    Map<Integer, IndexerInvertedDoconly.FileRange> readMergedIndex = new HashMap<Integer, IndexerInvertedDoconly.FileRange>();
    DataInputStream mergeDIS = new DataInputStream(new FileInputStream(mergedFile));
    long readOffset = IndexerInvertedDoconly.loadFromFileIntoIndex(mergeDIS, readMergedIndex);

    assertEquals(readOffset, offset);

    Set<Integer> unionKeys = new HashSet<Integer>();
    unionKeys.addAll(fullIndex1.keySet());
    unionKeys.addAll(fullIndex2.keySet());
    assertEquals(unionKeys, mergedIndex.keySet());

    RandomAccessFile indexFile = new RandomAccessFile(mergedFile, "r");
    indexFile.seek(offset);
    assertEquals(1, indexFile.readInt());

    for(int key : mergedIndex.keySet()) {
      List<Integer> list = new ArrayList<Integer>();
      if (fullIndex1.containsKey(key)) list.addAll(fullIndex1.get(key));
      if (fullIndex2.containsKey(key)) list.addAll(fullIndex2.get(key));

      List<Integer> listConfirm = new ArrayList<Integer>();
      IndexerInvertedDoconly.FileRange fileRange = mergedIndex.get(key);
      indexFile.seek(offset + fileRange.offset);
      for(int i = 0; i < fileRange.length; i++) {
        listConfirm.add(indexFile.readInt());
      }
      assertEquals(list, listConfirm);
    }
    indexFile.close();
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