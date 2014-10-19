GROUP 14

---------- INDEX CONSTRUCTION ----------

Assuming that the current directory is "src", compilation of source codes can be done as:

tt1161@linserv1[src]$ javac -cp .:../lib/* edu/nyu/cs/cs2580/*.java

Please make sure that the all JAR files in the "lib" folder are included in the compilation, like above.

We construct the index using the merging approach, as opposed to in-memory construction as provided by the original source code. 

// explain more of how it works here

Only metadata of the index is loaded into memory during "serve" mode. The metadata is the integer representation of all terms in the corpus with the offsets, which are used to jump into the posting list of the term in the index file. This is the first portion in the index file. In the later portion is the posting lists corresponding to each term. The client codes can read the posting list from the file by supplying the integer representation. This way, only the metadata is stored in memory, not the posting lists.


---------- INDEX LOADING ----------

In finding a reasonable compromise between space and time, we chose to maintain the following data in memory while SearchEngine is running in the "serve" mode:

Map<Integer, FileRange> _index:   An index, which is a mapping between an integer representation of a term and a byte range in the file where the postings list for the term is located.

long _indexOffset:	An offset in the file where the postings lists begin (after all metadata).
	
Vector<Document> _documents:	A vector of documents consisting of metadata.
  
BiMap<String, Integer> _dictionary:	Maps each term to its integer representation

Map<Integer, Integer> _termCorpusFrequency:	Term frequency, key is the integer representation of the term and value is the number of times the term appears in the corpus.

Set<String> _stoppingWords:		The set contains stopping words, corresponding to the top 50 most frequent words in a corpus.

In addition to these fields, the IndexerInvertedOccurrence and IndexerInvertedCompressed maintain Map<Integer, Integer> _corpusDocFrequencyByTerm


---------- REPRESENTATION OF QUERYPHRASE ----------


---------- REPRESENTATION OF DOCUMENTINDEXED ----------



---------- DOCUMENT PROCESSING ----------

To remove non-visible context of the page, the 3rd library, Boilerpipe, is used. The library website is at: https://code.google.com/p/boilerpipe/

PorterStemmer class is used to perform only step 1 of the Porter's algorithm.

A set of stop words is determined by the top 50 most frequent terms in a corpus. However, no stop word is removed during construction of the index. At run-time, when a user issues a query, processing of inverted list of stop words is skipped.



---------- FAVORITE RANKER ----------

Cosine ranker was chosen as the favorite. The RankerFavorite class extends RankerCosine.

In the construction index phase, the normalizing factor of a document vector is precomputed so that body tokens do not need to be stored in DocumentIndexed. This saves significant memory.
