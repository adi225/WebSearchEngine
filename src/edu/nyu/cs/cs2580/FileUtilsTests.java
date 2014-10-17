package edu.nyu.cs.cs2580;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.*;

import java.io.*;
import java.util.*;
import edu.nyu.cs.cs2580.FileUtils.FileRange;

import static org.junit.Assert.*;

public class FileUtilsTests {

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
    long writeOffset = FileUtils.writeObjectToFile(map, file);

    List<Object> list = new ArrayList<Object>();
    long readOffset = FileUtils.readObjectsFromFileIntoList(file, list);
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
    long writeOffset = FileUtils.writeObjectsToFile(list, file);

    List<Object> list2 = new ArrayList<Object>();
    long readOffset = FileUtils.readObjectsFromFileIntoList(file, list2);
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
    long bytesWritten1 = FileUtils.writeObjectToFile(map, file);
    assertEquals(file.length(), bytesWritten1);
    long bytesWritten2 = FileUtils.writeObjectToFile(vector, file);
    assertEquals(file.length() - bytesWritten1, bytesWritten2);
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
    long offs = FileUtils.writeObjectToFile(map, file);
    long fileLength = file.length();
    assertEquals(fileLength, offs);
    offs = FileUtils.writeObjectToFile(map, file2);
    assertEquals(fileLength, offs);

    FileUtils.appendFileToFile(file, file2);
    assertEquals(fileLength * 2, file2.length());

    List<Object> list = new ArrayList<Object>();
    long offset = FileUtils.readObjectsFromFileIntoList(file2, list);
    assertEquals(offset, fileLength);
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
    List<Integer> a = Lists.newArrayList(1, 2, 3, 4, 8);
    List<Integer> b = Lists.newArrayList(5, 6, 9);
    List<Integer> c = Lists.newArrayList(7, 8, 9);
    fullIndex.put(10, a);
    fullIndex.put(20, b);
    fullIndex.put(30, c);
    Map<Integer, List<Integer>> fullIndex2 = Maps.newHashMap(fullIndex);

    File file = new File(testDirectory, "testFileOJIHUIUYGJOI");
    long writeOffset = FileUtils.dumpIndexToFile(fullIndex, file);

    DataInputStream fileDIS = new DataInputStream(new FileInputStream(file));
    Map<Integer, FileRange> index = new HashMap<Integer, FileRange>();
    long readOffset = FileUtils.loadFromFileIntoIndex(fileDIS, index);
    assertEquals(writeOffset, readOffset);
    assertEquals(fullIndex2.keySet(), index.keySet());

    RandomAccessFile indexFile = new RandomAccessFile(file, "r");

    for(int key : index.keySet()) {
      List<Integer> list = fullIndex2.get(key);
      List<Integer> listConfirm = new ArrayList<Integer>();
      FileRange fileRange = index.get(key);
      indexFile.seek(readOffset + fileRange.offset);
      for(int i = 0; i < fileRange.length / 4; i++) {
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
    Map<Integer, List<Integer>> fullIndex1Use = Maps.newHashMap(fullIndex1);
    FileUtils.dumpIndexToFile(fullIndex1Use, file1);
    File file2 = new File(testDirectory, "testFileOJYUGTRHUGUJOI");
    Map<Integer, List<Integer>> fullIndex2Use = Maps.newHashMap(fullIndex2);
    FileUtils.dumpIndexToFile(fullIndex2Use, file2);
    File mergedFile = new File(testDirectory, "testFileOJYUGTRHUFYGUGTUJOI");
    Map<Integer, FileRange> mergedIndex = new HashMap<Integer, FileRange>();
    long offset = FileUtils.mergeFilesIntoIndexAndFile(file1, file2, mergedIndex, mergedFile);
    Map<Integer, FileRange> readMergedIndex = new HashMap<Integer, FileRange>();
    DataInputStream mergeDIS = new DataInputStream(new FileInputStream(mergedFile));
    long readOffset = FileUtils.loadFromFileIntoIndex(mergeDIS, readMergedIndex);

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
      FileRange fileRange = mergedIndex.get(key);
      indexFile.seek(offset + fileRange.offset);
      for(int i = 0; i < fileRange.length / 4; i++) {
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