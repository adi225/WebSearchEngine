WebSearchEngine: Group 14
=========================

This repository contains homework solutions for the Web Search Engines class taught at New York University during Fall 2014 by Professors Cong Yu and Fernando Diaz.

The homeworks lead to building a simple web search engine.

#Assignment 3

##COMPILATION AND RUNNING THE SEARCH ENGINE

Assuming that the current directory is the parent directory of src. Please also make sure that the all JAR files in the "lib" folder are included in the compilation, like below.

To compile,
$ javac -cp .:./lib/*:./src src/edu/nyu/cs/cs2580/*.java

To run in mining mode,
$ java -Xmx512m -cp .:./lib/*:./src edu.nyu.cs.cs2580.SearchEngine --mode=mining --options=conf/engine.conf

To run in indexing mode,
$ java -Xmx512m -cp .:./lib/*:./src edu.nyu.cs.cs2580.SearchEngine --mode=index --options=conf/engine.conf

To run in serving mode,
$ java -Xmx512m -cp .:./lib/*:./src edu.nyu.cs.cs2580.SearchEngine --mode=serve --port=25814 --options=conf/engine.conf

##CHOICE OF PAGERANK PARAMETERS

We conform to the following formula in calculating the values of PageRank

PageRank[i] = (1 - DAMPING_FACTOR)*(sum of contributions from other pages) + DAMPING_FACTOR*(1/n)

That is, lambda = 1 - DAMPING_FACTOR.

The parameters that yield the best estimated results are lambda = 1 - DAMPING_FACTOR = 0.9 with 2 iterations. This is because the portion of the value of PageRank that comes from the outlinks of the other pages should have more impact than the random jump from any page, in which its purpose is to resolve the problem of pages that have no incoming links. 2 iterations give more accurate values since it is closer to the values at convergence.

##HANDLING NUMVIEWS

For documents that were present in our corpus, but not in the log file, we assumed that the numViews number is 0. We also tried avoiding this assumption by only calculating the Spearman coefficient using the documents for which we have an explicit number of views given. This however slightly reduced the Spearman coefficient, giving us more confidence in the assumption that missing documents should have a numViews score of 0.

##SPEARMAN'S RANK CORRELATION COEFFICIENT

To run the Spearman class,
$ java -cp .:./lib/*:./src edu.nyu.cs.cs2580.Spearman ./data/index/pagerank ./data/index/numviews

We implemented the method of assigning ranks for Spearman coefficient in such a way that we are averaging the ranks of multiple documents that have the same score value (whether it is pagerank or numviews).

The computed correlation coefficient between PageRank and NumViews is 0.4228893347462366 when using the canonical method of averaging ranks of documents with same pagerank/numviews. When using tie breaking based on URL, we obtained a Spearman coefficient of 0.4033400123446214.


#Assignment 2

##INDEX CONSTRUCTION

Assuming that the current directory contains the "src" , "data", "lib", and "conf" folders. Compilation of source codes can be done with the following command:

$ javac -cp .:./lib/*:./src src/edu/nyu/cs/cs2580/*.java

To run the SearchEngine in the "index" mode:

$ java -Xmx512m -cp .:./lib/*:./src edu.nyu.cs.cs2580.SearchEngine --mode=index --options=./conf/engine.conf

To run the SearchEngine in the "serve" mode:

$ java  -Xmx512m -cp .:./lib/*:./src edu.nyu.cs.cs2580.SearchEngine --mode=serve --port=25814 --options=./conf/engine.conf

Please make sure that the all JAR files in the "lib" folder are included in the compilation, like above.

We construct the index using the merging approach, as opposed to in-memory construction as provided by the original source code. We construct partial indexes in memory, until we reach a certain memory use threshold. We then dump the in-memory index to file, and clear the memory. Once all partial indexes are constructed, we merge the files using a variant of the "merge-sort" merge() function. This is done efficiently using Input/Output streams rather than byte-at-a-time.

Only metadata of the index is loaded into memory during "serve" mode (with the exception of a small cache of posting lists). The metadata consists of a mapping from the integer representation of each term in the corpus to offset in the index file where its posting list is located. It is used to jump to the posting list of the term in the index file. A few other metadata stores are loaded at the beginning of the index file, such as a mapping of string terms to their integer representations, etc. In the later portion is the posting lists corresponding to each term. The client code can read the posting list from the file by supplying the integer representation. This way, only the metadata is stored in memory, not the posting lists (except for a small cache for performace reasons).

We also maintain a round-robin style cache of posting lists, such that whenever a posting list is requested, after it is loaded from the file, it is stored in memory during serve mode. This way, the next time the same posting list is requested, it is fetched from the in-memory cache rather than from file again. When the cache grows past a certain threshold, some of its list are released at random, allowing new lists to be added to the cache without overwhelming the memory usage.

##INDEX LOADING

Our index size for IndexerInvertedDoconly, IndexerInvertedOccurrence, IndexerInvertedCompressed are roughly 30, 102, 44 MBs respectively.

In finding a reasonable compromise between space and time, we chose to maintain the following data in memory while SearchEngine is running in the "serve" mode:

Map<Integer, FileRange> _index:   An index metadata, which is a mapping between an integer representation of a term and a byte range in the file where the postings list for the term is located.

long _indexOffset:	An offset in the file where the postings lists begin (after all metadata).
	
Vector<Document> _documents:	A vector of documents consisting of metadata (no actual information on content).
  
BiMap<String, Integer> _dictionary:	Maps each term to its integer representation (one-to-one bidirectional mapping)

Map<Integer, Integer> _termCorpusFrequency:	Term frequency, key is the integer representation of the term and value is the number of times the term appears in the corpus.

Set<String> _stoppingWords:		The set contains stopping words, corresponding to the top 50 most frequent words in a corpus.

In addition to these fields, the IndexerInvertedOccurrence and IndexerInvertedCompressed maintain Map<Integer, Integer> _corpusDocFrequencyByTerm, which allows faster access to this information without reading an entire posting list.

##INDEX COMPRESSION
We compressed the index using the v-byte and delta compression method. The best-case compression that can be expected is 4x, since each integer would at best be represented as 1 byte. Using delta-compression, we are able to facilitate this even more by encouraging the storage of small integers.

We experienced a significant compresion from 102 MB to 44MB, which is in line with what we expected. In future work, we may attempt to use Elias encoding to reduce this further. However, we found that our integers after delta compression are relatively small (in the range of hundreds), but not very small (not often smaller than 20). 

##DOCUMENT PROCESSING

The TextUtils.java is provided to perform document processing. The same processing is applied identically to queries (except for the quotation mark) in serve mode as they arrive.

The functionality includes:

1.) Removing non-visible context of the page: The 3rd library, Boilerpipe, is used. The library website is at: https://code.google.com/p/boilerpipe/

2.) Removing abbreviation dots (converting U.K. to UK, I.B.M. to IBM)
We use regular expressions to detect the abbreviation pattern and replace it with the non-abbreviate version.

3.) deAccent (such that Beyoncé becomes Beyonce)
We use special features of regular expressions to detect accented characters and replace them with their non-accented versions.

4.) Converting Unicode special characters into their unicode codepoint encoding as a String.
We use the codepoint of the characters, and if they match the Unicode character class general category "Lo" (in Java, Character. OTHER_LETTER), meaning they are non-punctuation special letters, we replace them by the encoding "u", followed by their code point. This generates a repeatable (deterministic) unique identifier for this character, which can be treated as an ASCII word. We also pad this encoding with spaces, so that it is treated as a separate word.

5.) Removing punctuations
We replace all characters that are not English letters, Roman digits or newlines with space. In query processing, we also ignore quotation marks and do not replace them with spaces. This is because they have a special meaning of "query phrase", so we can detect them properly.

6.) Stemming: use of step 1 of Porter's algorithm to perform stemming
We use the Porter stemming, which performs gramatical stemming based on English language gramatical constructs.

A set of stop words is determined by the top 50 most frequent terms in a corpus. However, no stop word is removed during construction of the index. At run-time, when a user issues a query, processing of inverted list of stop words is skipped.


##REPRESENTATION OF DOCUMENTINDEXED

In addition to the super class's fields, DocumentIndexed has the field:  private double _tfidfSumSquared, which is precomputed in the index construction process, so that the body token vectors do not have to be maintained.

This precomputed number allows us to normalize tfidf vectors for RankerCosine ranking, without needing to access the document's list of words.

##REPRESENTATION OF QUERYPHRASE

In addition to the super class's fields, QueryPhrase maintains the field: public Map<String, List<String>> _phrases. The key of the map is the phrase, and the value is a list of tokens in the phrase.


##FAVORITE RANKER

Cosine ranker was chosen as the favorite. The RankerFavorite class extends RankerCosine without any extra functionality (for testing convenience).

In the construction index phase, the normalizing factor of a document vector is precomputed so that body tokens do not need to be stored in DocumentIndexed. This saves significant memory.

##CARRYOVERS FROM ASSIGNMENT 1
We maintained some important features from assignment 1, such as HTML result display and click logging. 


End of assignment 2
-------------------


#Assignment 1

##RUNNING THE SEARCH ENGINE

The followings assume that you are currently in the "src" folder. Please adjust the path in the source codes respectively (in the Evaluator, Ranker, and QueryHandler files) if you are not executing within the "src" folder.

In order to run the SearchEngine, you can issue:
java edu.nyu.cs.cs2580.SearchEngine 25814 ../data/corpus.tsv  

You can then go to the browser and enter:
http://localhost:25814/search?query%3Dgoogle%26ranker%3Dphrase%26format%3Dhtml

Please note that the url must be encoded like above


##FILES GENERATION

To enable the Ranker and Evaluator to produce the output files, which will be saved in the "results" directory, change the boolean "saveOutput" in both classes to true. The default setting is false, which does not generate output files. If you prefer different output path, please change them in both classes corressponding.

Each output file from the Ranker corresponds to each ranking method and contains the rankings for all queries in the "queries.tsv" file.

Each output file from the Evalutor corresponds to each ranking method and contains the evaluation of all metrics, separated by a tab, for all queries in the "queries.tsv" file.

Not that the output files are generated by the server and is independent of calls from the clients, which do not generate any files.


##RUNNING THE EVALUATOR

The input into the Evaluator is via standard input, corresponding to the format of the output of the ranker: QUERY<TAB>DOCUMENTID-1<TAB>TITLE<TAB>SCORE

It is assumed that the input fed into the Evaluator is more than or equal to 10 lines. This is because some metrics, e.g, precision at 10, require such a minimum amount line of input.

It is also assumed that all lines of input has the same query (only considering one query per call and that the evaluator is agnostic of the ranking method).


##LINEAR MODEL

Below are the beta parameters for the construction of the simple linear model,

 betaCos = 1.0/0.8
 betaQL = 1.0/9.0
 betaPhrase = 1.0/300.0
 betaNumviews = 1.0/20000.0


##CLICK LOGGING

Click logging is also implemented. The log file is saved in the "results" folder. In implementing this, a cookie on the browser with a session id is set, which is read from every request. The session is set to expire in 20 minutes.

The result of the click tracking links is an HTTP redirect to the search results page.

Note that this works well in Firefox and Chrome, but not in Safari because of a known bug.

