package edu.nyu.cs.cs2580;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

import com.google.common.collect.BiMap;
import edu.nyu.cs.cs2580.SearchEngine.Options;

/**
 * @CS2580: Implement this class for HW2.
 */
public class IndexerInvertedOccurrence extends IndexerInverted implements Serializable{

  private static final long serialVersionUID = 1077111905740085030L;

  protected Map<Integer, Integer> _corpusDocFrequencyByTerm;
	
  public IndexerInvertedOccurrence(Options options) {
    super(options);
  }

  @Override
  public void constructIndex() throws IOException {
    _corpusDocFrequencyByTerm = new HashMap<Integer, Integer>();
    super.constructIndex();
  }

  @Override
  protected List<Object> selectMetadataToStore() {
    List<Object> indexMetadata = super.selectMetadataToStore();
    indexMetadata.add(_corpusDocFrequencyByTerm);
    return indexMetadata;
  }

  @Override
  protected void setLoadedMetadata(List<Object> indexMetadata) {
    super.setLoadedMetadata(indexMetadata);
    _corpusDocFrequencyByTerm = (Map<Integer, Integer>)indexMetadata.get(indexMetadata.size() - 1);
  }

  protected void incrementCorpusDocFrequencyForTerm(int word) {
    if(_corpusDocFrequencyByTerm.containsKey(word)) {
      _corpusDocFrequencyByTerm.put(word, _corpusDocFrequencyByTerm.get(word) + 1);
    } else {
      _corpusDocFrequencyByTerm.put(word, 1);
    }
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
        _utilityIndex.put(word, new LinkedList<Integer>());
      }
      List<Integer> postingList = _utilityIndex.get(word);
      List<Integer> occurancesList = occurences.get(word);

      postingList.add(docId);
      incrementCorpusDocFrequencyForTerm(word);
      postingList.add(occurancesList.size());
      postingList.addAll(occurancesList);

      _utilityIndexFlatSize += occurancesList.size() + 2;
      occurences.put(word, null);

      if(_utilityIndexFlatSize > UTILITY_INDEX_FLAT_SIZE_THRESHOLD) {
        String filePath = _options._indexPrefix + WORDS_DIR + "/" + _utilityPartialIndexCounter++;
        dumpUtilityIndexToFileAndClearFromMemory(filePath);
      }
    }
  }

  /**
   * In HW2, you should be using {@link DocumentIndexed}.
   */
  @Override
  public Document nextDoc(Query query, int docid)
  {
	// 3 cases to handle:
	// 1.) query containing only conjunctive terms (easily dealt with nextDocConjunctive)
	// 2.) query containing only the phrase portion
	// 3.) query containing both conjunctive and phrase parts
	  
	  if(query instanceof QueryPhrase)
	  {
		  
		  QueryPhrase queryPhrase = (QueryPhrase)query;
		 
		  List<Integer> docsForAllPhrases = new ArrayList<Integer>();
		  
		  //need to make sure all phrases are found
		  Iterator it = queryPhrase._phrases.entrySet().iterator();
		  while (it.hasNext()) 
		  {	
			  //loop over each phrase
			  Map.Entry pairs = (Map.Entry)it.next();
			  
			  List<Integer> docIDs = new ArrayList<Integer>();  // a list containing doc ID for each term in the phrase
			  
			  List<String> phraseTokens = (List<String>)pairs.getValue();
				try
				{
					//Check the next doc for each token in the phrase
					for(String token : phraseTokens)
					{
						int docID = next(token,docid);
						if(docID == -1)
						{
							return null;
						}
						docIDs.add(docID);
					}
				} 
				catch (IOException e)
				{
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				boolean foundDocID = true;
				int docIDFixed = docIDs.get(0); 
				int docIDNew = Integer.MIN_VALUE;
				
				for(Integer docID : docIDs)
				{  // check if all the doc IDs are equal
					if(docID != docIDFixed)
					{
						foundDocID = false;
					}
					if(docID > docIDNew)
					{
						docIDNew = docID;
					}
				}
				
				if(foundDocID) //We found a doc 
				{
					//Check that the terms match the phrase
					int phrasePosition = -1;
					phrasePosition = nextPhrase((String)pairs.getKey(), phraseTokens ,docIDFixed , phrasePosition);
			
					if(phrasePosition == -1) //phrase not found
						return nextDoc(query,docIDNew-1);
					  
					docsForAllPhrases.add(docIDFixed);
				}
				else
					return nextDoc(query,docIDNew-1);
		  }
		  
		  // all phrases done - check if docs are correct
		  boolean foundFinalDocID = true;
			int finalDocIDFixed = docsForAllPhrases.get(0); 
			int finalDocIDNew = Integer.MIN_VALUE;
			
			for(Integer docID : docsForAllPhrases)
			{  // check if all the doc IDs are equal
				if(docID != finalDocIDFixed)
				{
					foundFinalDocID = false;
				}
				if(docID > finalDocIDNew)
				{
					finalDocIDNew = docID;
				}
			}
			
			if(!foundFinalDocID)
				 return nextDoc(query,finalDocIDNew-1);
					  
			/* check regular tokens */
					  
			  int regularTokensDoc;
			  //need to make sure all regular tokens are found
			  if(queryPhrase._tokens != null && queryPhrase._tokens.size() != 0)
			  {
				  regularTokensDoc = nextDocConjunctive(query, docid)._docid;
				  if(regularTokensDoc == finalDocIDFixed)
					  return _documents.get(finalDocIDFixed);
				  else
					  return nextDoc(query,finalDocIDNew-1);
			  }
			  
			  return _documents.get(finalDocIDFixed);
		  
	  }
	  else
	  {
		  return nextDocConjunctive(query, docid);
	  }
  }
  
  // This method returns the next docid after the given docid that contains all terms in the query conjunctive.
  // This is equivalent to the nextDoc method in the IndexerInvertedDoconly class.
  public Document nextDocConjunctive(Query query, int docid){
	// query is already processed before getting passed into this method
    // TODO Optimize performance using PriorityQueue, Set, or more sequential lookup of terms, etc.
	try {
      List<Integer> docIDs = new ArrayList<Integer>();  // a list containing doc ID for each term in the query
      for(String token : query._tokens){
        if(_stoppingWords.contains(token)){  // skip processing a stop word
          continue;
        }
        int docID = next(token, docid);
        if(docID == -1){
          return null;
        }
        docIDs.add(docID);
      }

      boolean foundDocID = true;
      int docIDFixed = docIDs.get(0);
      int docIDNew = Integer.MIN_VALUE;

      for(Integer docID : docIDs){  // check if all the doc IDs are equal
        if(docID != docIDFixed){
          foundDocID = false;
        }
        if(docID > docIDNew){
          docIDNew = docID;
        }
      }

      if(foundDocID){
        return _documents.get(docIDFixed);
      }
      return nextDoc(query, docIDNew - 1);
	} catch (IOException e) {}
	return null;
  }
  
  // Just like in the lecture slide 3, page 14, this helper method returns the next document id
  // after the given docid. It returns -1 if not found.
  protected int next(String term, int docid) throws IOException {
    if(!_dictionary.containsKey(term)) {
      return -1;
    }

    int termInt = _dictionary.get(term);  // an integer representation of a term
    List<Integer> postingList = postingsListForWord(termInt);

    int occurrenceIndex = 1;  // the first index of occurrence position in the list
    while(occurrenceIndex < postingList.size()){
      int docIndex = occurrenceIndex - 1;
      if(postingList.get(docIndex) > docid){
        return postingList.get(docIndex);
      }
      int occurrence = postingList.get(occurrenceIndex);
      occurrenceIndex += occurrence + 2;  // jump to the next occurrence position
    }
    return -1;
  }
  
  // Lecture 3 slide, page 23
  // This method returns the next position of the phrase after pos within the docid.
  public int nextPhrase(String phrase, List<String> phraseTokens, int docid, int pos){
	  // need to pass the phrase portion into the query
	  
	  List<Integer> positions = new ArrayList<Integer>();
	  int maxPosition = Integer.MIN_VALUE;
	  for(int i=0; i<phraseTokens.size(); i++){
		  int termPosition = nextPosition(phraseTokens.get(i), docid, pos);
		  if(termPosition == -1){
			  return -1;
		  }
		  maxPosition = Math.max(maxPosition, termPosition);
		  positions.add(termPosition);
	  }
	  
	  boolean foundPhrase = true;
	  for(int i=1; i<positions.size(); i++){
		  if(positions.get(i) != positions.get(i-1) + 1){
			  foundPhrase = false;
			  break;
		  }
	  }
	  
	  if(foundPhrase){
		  return positions.get(0);
	  }
	  return nextPhrase(phrase, phraseTokens, docid, maxPosition);
  }
  
  // This method returns the next occurrence of the term in docid after pos
  protected int nextPosition(String term, int docid, int pos) {
    if(!_dictionary.containsKey(term)) {
      return -1;
    }
    try {
      int termInt = _dictionary.get(term);  // an integer representation of a term
      List<Integer> postingList = postingsListForWord(termInt);
      int occurrenceIndex = 1;  // the first index of occurrence position in the list
      while (occurrenceIndex < postingList.size()) {
        int docIndex = occurrenceIndex - 1;
        if (postingList.get(docIndex) > docid) {
          return -1;
        }
        int occurrence = postingList.get(occurrenceIndex);
        if (postingList.get(docIndex) == docid) {  // found the docid
          // iterating through the positions of docid
          for (int posIndex = occurrenceIndex + 1; posIndex < occurrenceIndex + 1 + occurrence; posIndex++) {
            if (postingList.get(posIndex) > pos) {
              return postingList.get(posIndex);
            }
          }
          return -1;
        }
        occurrenceIndex += occurrence + 2;  // jump to the next occurrence position
      }
    } catch (IOException e) {}
    return -1; // not found
  }

  @Override
  public int corpusDocFrequencyByTerm(String term) {
    if(_dictionary.containsKey(term)) {
      int word = _dictionary.get(term);
      return _corpusDocFrequencyByTerm.get(word);
    }
    return 0;
  }

  @Override
  public int corpusTermFrequency(String term) {
    if(_dictionary.containsKey(term)) {
      int word = _dictionary.get(term);  // an integer representation of a term
      return _termCorpusFrequency.get(word);
    }
    return 0;
  }

  @Override
  public int documentTermFrequency(String term, String url) {
    // TODO get docid from url here, should we have a mapping from url to docid?
    int docId = mapUrlToDocId(url);
    if(docId < 0 || !_dictionary.containsKey(term)) return 0;
    int termId = _dictionary.get(term);
    
    try {
      List<Integer> postingList = postingsListForWord(termId);
      int termFrequency = 0;
      int occurrenceIndex = 1;  // the first index of occurrence position in the list
      while(occurrenceIndex < postingList.size()){
        int docIndex = occurrenceIndex - 1;
        if(postingList.get(docIndex) > docId){
          return 0;
        }
        int occurrence = postingList.get(occurrenceIndex);
        if(postingList.get(docIndex) == docId){
          return occurrence;
        }
        occurrenceIndex += occurrence + 2;  // jump to the next occurrence position
      }
	} catch (IOException e) {}
    return 0;
  }
  
  // This method returns the corresponding docid of the given url.
  public int mapUrlToDocId(String url) {
    for(Document doc : _documents){
      if(doc.getUrl().equals(url)){
        return doc._docid;
      }
    }
    return -1;
  }
}
