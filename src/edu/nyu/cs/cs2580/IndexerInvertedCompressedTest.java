package edu.nyu.cs.cs2580;

import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.util.*;

import static org.junit.Assert.*;

public class IndexerInvertedCompressedTest {

  IndexerInvertedCompressed _indexer;
  File testDirectory;
  private static final int MAX_DOC_TEST = 100;

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
    _indexer.updatePostingsLists(88807, docTokensAsInteger1);
    _indexer.updatePostingsLists(88812, docTokensAsInteger2);
    File testFile = new File(testDirectory, "miniindex");
    _indexer.dumpUtilityIndexToFileAndClearFromMemory(testFile.getAbsolutePath());

    DataInputStream indexFileDIS = new DataInputStream(new FileInputStream(testFile));
    _indexer._indexOffset = FileUtils.loadFromFileIntoIndex(indexFileDIS, _indexer._index);
    indexFileDIS.close();
    _indexer._indexRAF = new RandomAccessFile(testFile, "r");

    List<Integer> postingList = _indexer.postingsListForWord(1);
    assertEquals(Lists.newArrayList(88807, 3, 0, 2, 4, 88812, 1, 6), postingList);

    postingList = _indexer.postingsListForWord(2);
    assertEquals(Lists.newArrayList(88807, 3, 1, 3, 5), postingList);

    postingList = _indexer.postingsListForWord(3);
    assertEquals(Lists.newArrayList(88807, 1, 10, 88812, 1, 3), postingList);

    postingList = _indexer.postingsListForWord(234);
    assertEquals(Lists.newArrayList(88807, 1, 7), postingList);

    postingList = _indexer.postingsListForWord(767);
    assertEquals(Lists.newArrayList(88807, 1, 9), postingList);

    postingList = _indexer.postingsListForWord(6);
    assertEquals(Lists.newArrayList(88807, 1, 6, 88812, 3, 0, 4, 7), postingList);

    postingList = _indexer.postingsListForWord(7);
    assertEquals(Lists.newArrayList(88812, 3, 1, 2, 5), postingList);

    postingList = _indexer.postingsListForWord(8);
    assertEquals(Lists.newArrayList(88807, 2, 8, 12), postingList);

    postingList = _indexer.postingsListForWord(9);
    assertEquals(Lists.newArrayList(88807, 1, 11), postingList);
  }

  @Test
  public void testConstructDecodedAndLoadDecodedCompressed() throws Exception {

    _indexer._utilityIndex = new UnshrinkableHashMap<Integer, List<Byte>>();
    _indexer.UTILITY_INDEX_FLAT_SIZE_THRESHOLD = Integer.MAX_VALUE;
    _indexer.MAX_DOCS = MAX_DOC_TEST;

    _indexer.constructIndex();
    Map<Integer, List<Byte>> utilityIndex = _indexer._utilityIndex;

    SearchEngine.Options options = new SearchEngine.Options("conf/engine.conf");
    _indexer = new IndexerInvertedCompressed(options);
    _indexer.loadIndex();

    Map<Integer, List<Integer>> loadedIndex = new HashMap<Integer, List<Integer>>();

    for(int word : utilityIndex.keySet()) {
      List<Integer> postingList = _indexer.postingsListForWord(word);
      loadedIndex.put(word, postingList);
    }

    //Map<Integer, List<Byte>> convertedLoadedPostingList = VByteUtils.integerPostingListAsBytes(loadedIndex);
    Map<Integer, List<Integer>> convertedUtilityIndex = VByteUtils.bytesPostingListAsIntegers(utilityIndex);

    for(int key : convertedUtilityIndex.keySet()) {
      convertedUtilityIndex.put(key, VByteUtils.deltaPostingsListToSequentialPostingsList(convertedUtilityIndex.get(key)));
    }

    assertEquals(convertedUtilityIndex, loadedIndex);
  }

  @Test
  public void testConstructAndLoadOccurences() throws Exception {
/*
    SearchEngine.Options options = new SearchEngine.Options("conf/engine.conf");
    IndexerInvertedOccurrence indexer = new IndexerInvertedOccurrence(options);

    indexer._utilityIndex = new UnshrinkableHashMap<Integer, List<Integer>>();
    indexer.UTILITY_INDEX_FLAT_SIZE_THRESHOLD = Integer.MAX_VALUE;
    indexer.MAX_DOCS = MAX_DOC_TEST;

    indexer.constructIndex();
    Map<Integer, List<Integer>> utilityIndex = indexer._utilityIndex;

    indexer = new IndexerInvertedOccurrence(options);
    indexer.loadIndex();

    Map<Integer, List<Integer>> loadedIndex = new HashMap<Integer, List<Integer>>();

    for(int word : utilityIndex.keySet()) {
      List<Integer> postingList = indexer.postingsListForWord(word);
      loadedIndex.put(word, postingList);
    }

    assertEquals(utilityIndex, loadedIndex);
*/
  }

  @Test
  public void testCompressedConstructDecodedOnOccurancesConstruct() throws Exception {

    SearchEngine.Options options = new SearchEngine.Options("conf/engine.conf");
    IndexerInvertedOccurrence indexerOccurances = new IndexerInvertedOccurrence(options);
    IndexerInvertedCompressed indexerCompressed = new IndexerInvertedCompressed(options);

    indexerOccurances._utilityIndex = new UnshrinkableHashMap<Integer, List<Integer>>();
    indexerOccurances.UTILITY_INDEX_FLAT_SIZE_THRESHOLD = Integer.MAX_VALUE;
    indexerOccurances.MAX_DOCS = MAX_DOC_TEST;

    indexerOccurances.constructIndex();
    Map<Integer, List<Integer>> utilityIndexOccurances = indexerOccurances._utilityIndex;

    indexerCompressed._utilityIndex = new UnshrinkableHashMap<Integer, List<Byte>>();
    indexerCompressed.UTILITY_INDEX_FLAT_SIZE_THRESHOLD = Integer.MAX_VALUE;
    indexerCompressed.MAX_DOCS = MAX_DOC_TEST;

    indexerCompressed.constructIndex();

    Map<Integer, List<Byte>> utilityIndexCompressed = indexerCompressed._utilityIndex;
    Map<Integer, List<Byte>> utilityIndexOccurancesConverted =
            VByteUtils.integerPostingListAsBytes(utilityIndexOccurances);
    Map<Integer, List<Integer>> utilityIndexCompressedConverted =
            VByteUtils.bytesPostingListAsIntegers(indexerCompressed._utilityIndex);

    // Occurances and compressed have same keys and same size of lists.
    assertEquals(utilityIndexOccurances.keySet(), utilityIndexOccurancesConverted.keySet());
    assertEquals(utilityIndexOccurancesConverted.keySet(), utilityIndexCompressed.keySet());
    for(int key : utilityIndexOccurances.keySet()) {
      assertEquals(utilityIndexOccurances.get(key).size(), utilityIndexCompressedConverted.get(key).size());
      utilityIndexCompressedConverted.put(key,
              VByteUtils.deltaPostingsListToSequentialPostingsList(utilityIndexCompressedConverted.get(key)));
    }

    assertEquals(utilityIndexOccurances, utilityIndexCompressedConverted);
  }

  @Test
  public void testConstructAndLoadCompressed() throws Exception {

    _indexer._utilityIndex = new UnshrinkableHashMap<Integer, List<Byte>>();
    _indexer.UTILITY_INDEX_FLAT_SIZE_THRESHOLD = Integer.MAX_VALUE;
    _indexer.MAX_DOCS = MAX_DOC_TEST;

    _indexer.constructIndex();
    Map<Integer, List<Byte>> utilityIndex = _indexer._utilityIndex;

    SearchEngine.Options options = new SearchEngine.Options("conf/engine.conf");
    _indexer = new IndexerInvertedCompressed(options);
    _indexer.loadIndex();

    Map<Integer, List<Byte>> loadedIndex = new HashMap<Integer, List<Byte>>();

    for(int word : utilityIndex.keySet()) {
      List<Byte> postingsList = new LinkedList<Byte>();
      FileUtils.FileRange fileRange = _indexer._index.get(word);
      _indexer._indexRAF.seek(_indexer._indexOffset + fileRange.offset);
      for(int i = 0 ; i < fileRange.length; i++) {
        postingsList.add(_indexer._indexRAF.readByte());
      }
      loadedIndex.put(word, postingsList);
    }

    //Map<Integer, List<Byte>> convertedLoadedPostingList = VByteUtils.integerPostingListAsBytes(loadedIndex);
    //Map<Integer, List<Integer>> convertedUtilityIndex = VByteUtils.bytesPostingListAsIntegers(utilityIndex);

    assertEquals(utilityIndex, loadedIndex);
  }

  @Test
  public void testCompressedConstructOnOccurancesConstructEncoded() throws Exception {

    SearchEngine.Options options = new SearchEngine.Options("conf/engine.conf");
    IndexerInvertedOccurrence indexerOccurances = new IndexerInvertedOccurrence(options);
    IndexerInvertedCompressed indexerCompressed = new IndexerInvertedCompressed(options);

    indexerOccurances._utilityIndex = new UnshrinkableHashMap<Integer, List<Integer>>();
    indexerOccurances.UTILITY_INDEX_FLAT_SIZE_THRESHOLD = Integer.MAX_VALUE;
    indexerOccurances.MAX_DOCS = MAX_DOC_TEST;

    indexerOccurances.constructIndex();
    Map<Integer, List<Integer>> utilityIndexOccurances = indexerOccurances._utilityIndex;

    indexerCompressed._utilityIndex = new UnshrinkableHashMap<Integer, List<Byte>>();
    indexerCompressed.UTILITY_INDEX_FLAT_SIZE_THRESHOLD = Integer.MAX_VALUE;
    indexerCompressed.MAX_DOCS = MAX_DOC_TEST;

    indexerCompressed.constructIndex();

    Map<Integer, List<Byte>> utilityIndexCompressed = indexerCompressed._utilityIndex;
    Map<Integer, List<Byte>> utilityIndexOccurancesConverted = new HashMap<Integer, List<Byte>>();

    for(int key : utilityIndexOccurances.keySet()) {
      List<Integer> postinglist = utilityIndexOccurances.get(key);
      List<Byte> converted = new LinkedList<Byte>();

      int intsRead = 0;
      int _utilityPrevDocId = 0;

      while (intsRead < postinglist.size()) {
        int docId = postinglist.get(intsRead++);
        // Put delta-encoded VByte docId into postings list
        int deltaDocId = docId - _utilityPrevDocId;

        _utilityPrevDocId = docId;
        byte[] deltaDocIdAsBytes = VByteUtils.encodeInt(deltaDocId);
        for (byte b : deltaDocIdAsBytes) {
          converted.add(b);
        }

        int numOccurances = postinglist.get(intsRead++);
        // Put number of occurances VByte into posting list
        byte[] sizeAsBytes = VByteUtils.encodeInt(numOccurances);
        for (byte b : sizeAsBytes) {
          converted.add(b);
        }

        // Put the delta-encoded occurances VBytes into posting list
        int _prevOccurance = 0;
        for (int i = 0; i < numOccurances; i++) {
          int occurance = postinglist.get(intsRead++);
          int deltaOccurances = occurance - _prevOccurance;
          _prevOccurance = occurance;
          byte[] occuranceAsBytes = VByteUtils.encodeInt(deltaOccurances);
          for (byte b : occuranceAsBytes) {
            converted.add(b);
          }
        }
      }
      utilityIndexOccurancesConverted.put(key, converted);
    }

    // Occurances and compressed have same keys and same size of lists.
    assertEquals(utilityIndexOccurances.keySet(), utilityIndexOccurancesConverted.keySet());
    assertEquals(utilityIndexOccurancesConverted.keySet(), utilityIndexCompressed.keySet());
    for(int key : utilityIndexOccurances.keySet()) {
      assertEquals(utilityIndexOccurancesConverted.get(key).size(), utilityIndexCompressed.get(key).size());
    }

    assertEquals(utilityIndexOccurancesConverted, utilityIndexCompressed);
  }

  @Test
  public void testUnmergedVsMergedEquality() throws Exception {
    File unmergedFile = new File(_indexer._options._indexPrefix + "/index_unmerged_compressed.idx");
    File mergedFile = new File(_indexer._options._indexPrefix + "/index_comp.idx");
    //File mergedFile = new File(_indexer._options._indexPrefix + "/index.idx");


    RandomAccessFile unmerged = new RandomAccessFile(unmergedFile, "r");
    RandomAccessFile merged = new RandomAccessFile(mergedFile, "r");

    Map<Integer, FileUtils.FileRange> unmergedLists = new HashMap<Integer, FileUtils.FileRange>();
    DataInputStream unmergedDIS = new DataInputStream(new FileInputStream(unmergedFile));
    List<Object> indexMetadata = new ArrayList<Object>();
    long unmergedOffset = FileUtils.readObjectsFromFileIntoList(unmergedDIS, indexMetadata);
    unmergedOffset += FileUtils.loadFromFileIntoIndex(unmergedDIS, unmergedLists);
    unmergedDIS.close();

    Map<Integer, FileUtils.FileRange> mergedLists = new HashMap<Integer, FileUtils.FileRange>();
    DataInputStream mergedDIS = new DataInputStream(new FileInputStream(mergedFile));
    long mergedOffset = FileUtils.readObjectsFromFileIntoList(mergedDIS, indexMetadata);
    mergedOffset += FileUtils.loadFromFileIntoIndex(mergedDIS, mergedLists);
    mergedDIS.close();

    assertEquals(unmergedLists.keySet(), mergedLists.keySet());

    for(int key : unmergedLists.keySet()) {
      FileUtils.FileRange unmergedFR = mergedLists.get(key);
      FileUtils.FileRange mergedFR = mergedLists.get(key);
      assertEquals(unmergedFR.length, mergedFR.length);

      _indexer._indexRAF = unmerged;
      _indexer._indexOffset = unmergedOffset;
      _indexer._index = unmergedLists;
      _indexer._indexCache.clear();
      _indexer._indexCacheFlatSize = 0;
      List<Integer> unmergedList = _indexer.postingsListForWord(key);

      _indexer._indexRAF = merged;
      _indexer._indexOffset = mergedOffset;
      _indexer._index = mergedLists;
      _indexer._indexCache.clear();
      _indexer._indexCacheFlatSize = 0;
      List<Integer> mergedList = _indexer.postingsListForWord(key);

      assertEquals(unmergedList, mergedList);
    }
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

  class UnshrinkableHashMap<K, V> extends HashMap<K, V> {

    @Override
    public void clear() {
      return;
    }

    @Override
    public V remove(Object key) {
      return this.get(key);
    }

    @Override
    public V put(K key, V value) {
      if(value == null  && this.containsKey(key)) {
        return value;
      }
      if(value instanceof Collection && ((Collection) value).isEmpty() && this.containsKey(key)) {
        return value;
      }
      return super.put(key, value);
    }
  }
}