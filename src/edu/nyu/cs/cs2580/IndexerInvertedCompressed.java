package edu.nyu.cs.cs2580;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import edu.nyu.cs.cs2580.SearchEngine.Options;

import java.io.*;
import java.util.*;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * @CS2580: Implement this class for HW2.
 */
public class IndexerInvertedCompressed extends IndexerInvertedOccurrence {

  protected Map<Integer, List<Byte>> _utilityIndex = Maps.newHashMap();
  protected Map<Integer, List<Byte>> _utilityDocWords =  Maps.newHashMap();

  protected Map<Integer, Integer> _utilityPrevDocId;

  public IndexerInvertedCompressed(Options options) {
    super(options);
  }

  @Override
  public void constructIndex() throws IOException {
    _utilityPrevDocId = new HashMap<Integer, Integer>();
    super.constructIndex();
  }

  @Override
  protected void updatePostingsLists(int docId, Vector<Integer> docTokensAsIntegers) throws IOException {
    // Indexing
    Map<Integer, List<Integer>> occurences = new HashMap<Integer,List<Integer>>();

    for(int position = 0; position < docTokensAsIntegers.size(); position++) {
      int word = docTokensAsIntegers.get(position);
      if (!occurences.containsKey(word)) {
        occurences.put(word, new LinkedList<Integer>());
      }
      List<Integer> occurancesList = occurences.get(word);
      occurancesList.add(position);
    }

    for(int word : occurences.keySet()) {
      if(!_utilityIndex.containsKey(word)) {
        _utilityIndex.put(word, new LinkedList<Byte>());
        if(!_utilityPrevDocId.containsKey(word)) {
          _utilityPrevDocId.put(word, 0);
        }
      }
      List<Byte> postingList = _utilityIndex.get(word);
      int postingListInitialSize = postingList.size();
      List<Integer> occurancesList = occurences.get(word);

      // Put delta-encoded VByte docId into postings list
      int deltaDocId = docId - _utilityPrevDocId.get(word);
      _utilityPrevDocId.put(word, docId);
      incrementCorpusDocFrequencyForTerm(word);
      byte[] deltaDocIdAsBytes = VByteUtils.encodeInt(deltaDocId);
      for(byte b : deltaDocIdAsBytes) {
        postingList.add(b);
      }

      // Put number of occurances VByte into posting list
      byte[] sizeAsBytes = VByteUtils.encodeInt(occurancesList.size());
      for(byte b : sizeAsBytes) {
        postingList.add(b);
      }

      // Put the delta-encoded occurances VBytes into posting list
      int _prevOccurance = 0;
      for(int occurance : occurancesList) {
        int deltaOccurances = occurance - _prevOccurance;
        _prevOccurance = occurance;
        byte[] occuranceAsBytes = VByteUtils.encodeInt(deltaOccurances);
        for (byte b : occuranceAsBytes) {
          postingList.add(b);
        }
      }

      int bytesWritten = postingList.size() - postingListInitialSize;

      _utilityIndexFlatSize += bytesWritten / 2;
      occurences.put(word, null);

      if(_utilityIndexFlatSize > UTILITY_INDEX_FLAT_SIZE_THRESHOLD) {
        String filePath = _options._indexPrefix + WORDS_DIR + "/" + _utilityPartialIndexCounter++;
        dumpUtilityIndexToFileAndClearFromMemory(filePath);
      }
    }
  }

  @Override
  protected void updateDocWords(int docId, Vector<Integer> docBody) throws IOException {
    checkArgument(docId < _documents.size());
    DocumentIndexed doc = _documents.get(docId);

    doc.setDocumentSize(docBody.size());

    // Count word frequencies.
    Map<Integer, Integer> documentMap = Maps.newTreeMap();
    for(int word : docBody) {
      int frequency = documentMap.containsKey(word) ? documentMap.get(word) : 0;
      documentMap.put(word, frequency + 1);
    }
    docBody.clear(); // discard the words from memory.

    // Sort the words by most frequent ones first.
    List<Map.Entry<Integer, Integer>> sortedDocumentMap = Utils.sortByValues(documentMap, true);
    documentMap = null; // discard the map from memory.

    List<Byte> docWords = Lists.newArrayList();
    _utilityDocWords.put(docId, docWords);
    for(int i = 0; i < TOP_WORDS_TO_STORE && i < sortedDocumentMap.size(); i++) {
      byte[] word = VByteUtils.encodeInt(sortedDocumentMap.get(i).getKey());
      for(byte b : word) {
        docWords.add(b);
      }
      byte[] freq = VByteUtils.encodeInt(sortedDocumentMap.get(i).getValue());
      for(byte b : freq) {
        docWords.add(b);
      }
      _utilityDocWordsFlatSize += word.length + freq.length;
    }
    if(_utilityDocWordsFlatSize > UTILITY_DOC_WORDS_FLAT_SIZE_THRESHOLD * 2) {
      String filePath = _options._indexPrefix + WORDS_DIR + "/words" + _utilityPartialDocWordsCounter++;
      dumpUtilityDocWordsToFileAndClearFromMemory(filePath);
    }
  }

  // This method may be deprecated in later versions. Use with caution!
  @Override
  protected List<Integer> postingsListForWord(int word) throws IOException {
    if(_indexCache.containsKey(word)) {
      return _indexCache.get(word);
    }

    LinkedList<Integer> deltaPostingsList = deltaPostingsListForWord(word);
    List<Integer> postingsList = new ArrayList<Integer>();

    int _prevDocId = 0;
    while(deltaPostingsList.size() > 0) {
      _prevDocId += deltaPostingsList.pollFirst();
      postingsList.add(_prevDocId);

      int _prevOccurance = 0;
      int numOccurances = deltaPostingsList.pollFirst();
      postingsList.add(numOccurances);
      for(int i = 0; i < numOccurances; i++) {
        _prevOccurance += deltaPostingsList.pollFirst();;
        postingsList.add(_prevOccurance);
      }
    }

    while(_indexCacheFlatSize > 0 && postingsList.size() * 4 + _indexCacheFlatSize > IndexerInverted.INDEX_CACHE_THRESHOLD) {
      List<Integer> lists = Lists.newArrayList(_indexCache.keySet());
      Random R = new Random();
      int randomListIndex = R.nextInt(lists.size());

      _indexCacheFlatSize -= _indexCache.get(lists.get(randomListIndex)).size() * 4;
      _indexCache.remove(lists.get(randomListIndex));
    }
    _indexCache.put(word, postingsList);
    _indexCacheFlatSize += postingsList.size() * 4;

    return postingsList;
  }

  @Override
  protected Map<Integer, Integer> wordListForDoc(int docId, RandomAccessFile store, long storeOffset) throws IOException {
    Map<Integer, Integer> wordsList = Maps.newTreeMap();
    FileUtils.FileRange fileRange = _docWords.get(docId);
    store.seek(storeOffset + fileRange.offset);
    byte[] loadedList = new byte[(int)fileRange.length];
    store.read(loadedList);

    int bytesRead = 0;
    boolean keepGoing;
    List<Integer> integerStream = Lists.newArrayList();
    while(bytesRead < fileRange.length) {
      int pos = 0;
      byte[] buf = new byte[8];
      do {
        buf[pos] = loadedList[bytesRead++];
        keepGoing = (buf[pos++] & 128) == 0;
      } while(keepGoing);
      byte[] asBytes = new byte[pos];
      for(int i = 0; i < asBytes.length; i++) {
        asBytes[i] = buf[i];
      }
      integerStream.add(VByteUtils.decodeByteArray(asBytes));
    }
    for(int i = 0; i < integerStream.size(); i+=2) {
      wordsList.put(integerStream.get(i), integerStream.get(i+1));
    }
    return wordsList;
  }

  protected LinkedList<Integer> deltaPostingsListForWord(int word) throws IOException {
    LinkedList<Integer> postingsList = new LinkedList<Integer>();
    FileUtils.FileRange fileRange = _index.get(word);
    _indexRAF.seek(_indexOffset + fileRange.offset);
    int bytesRead = 0;
    boolean keepGoing;
    byte[] loadedList = new byte[(int)fileRange.length];
    int readConfirm = _indexRAF.read(loadedList);
    SearchEngine.Check(readConfirm == loadedList.length, "IO problems reading postings list.");

    while(bytesRead < fileRange.length) {
      int pos = 0;
      byte[] buf = new byte[8];
      do {
        buf[pos] = loadedList[bytesRead++];
        keepGoing = (buf[pos++] & 128) == 0;
      } while(keepGoing);
      byte[] asBytes = new byte[pos];
      for(int i = 0; i < asBytes.length; i++) {
        asBytes[i] = buf[i];
      }
      postingsList.add(VByteUtils.decodeByteArray(asBytes));
    }

    return postingsList;
  }

  @Override
  protected void dumpUtilityIndexToFileAndClearFromMemory(String filePath) throws IOException {
    FileUtils.dumpIndexToFileBytes(_utilityIndex, new File(filePath));
    _utilityIndex.clear();
    _utilityIndexFlatSize = 0;
  }

  @Override
  protected void dumpUtilityDocWordsToFileAndClearFromMemory(String filePath) throws IOException {
    FileUtils.dumpIndexToFileBytes(_utilityDocWords, new File(filePath));
    _utilityDocWords.clear();
    _utilityDocWordsFlatSize = 0;
  }
}
