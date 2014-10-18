package edu.nyu.cs.cs2580;

import java.io.File;
import java.io.IOException;
import java.util.*;

import edu.nyu.cs.cs2580.SearchEngine.Options;

/**
 * @CS2580: Implement this class for HW2.
 */
public class IndexerInvertedCompressed extends IndexerInvertedOccurrence {

  protected Map<Integer, List<Byte>> _utilityIndex = new HashMap<Integer, List<Byte>>();

  protected Map<Integer, Integer> _utilityPrevDocId;

  public IndexerInvertedCompressed(Options options) {
    super(options);
  }

  @Override
  public void constructIndex() throws IOException {
    _utilityPrevDocId = new HashMap<Integer, Integer>();
    super.constructIndex();
  }

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
        _utilityPrevDocId.put(word, 0);
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

  // This method may be deprecated in later versions. Use with caution!
  @Override
  protected List<Integer> postingsListForWord(int word) throws IOException {
    LinkedList<Integer> deltaPostingsList = (LinkedList<Integer>) deltaPostingsListForWord(word);
    List<Integer> postingsList = new LinkedList<Integer>();

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
    return postingsList;
  }

  protected List<Integer> deltaPostingsListForWord(int word) throws IOException {
    List<Integer> postingsList = new LinkedList<Integer>();
    FileUtils.FileRange fileRange = _index.get(word);
    _indexRAF.seek(_indexOffset + fileRange.offset);
    int bytesRead = 0;
    boolean keepGoing;
    while(bytesRead < fileRange.length) {
      int pos = 0;
      byte[] buf = new byte[8];
      keepGoing = true;
      do {
        buf[pos] = _indexRAF.readByte();
        bytesRead++;
        keepGoing = (buf[pos++] & 0b10000000) == 0;
      } while(keepGoing);
      byte[] asBytes = new byte[pos];
      for(int i = 0; i < asBytes.length; i++) {
        asBytes[i] = buf[i];
      }
      postingsList.add(VByteUtils.decodeByteArray(asBytes));
    }

    return postingsList;
  }

  protected void dumpUtilityIndexToFileAndClearFromMemory(String filePath) throws IOException {
    FileUtils.dumpIndexToFileBytes(_utilityIndex, new File(filePath));
    _utilityIndex = new HashMap<Integer, List<Byte>>();
    _utilityIndexFlatSize = 0;
  }
}
