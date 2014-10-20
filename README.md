WebSearchEngine: Group 14
=========================

This repository contains homework solutions for the Web Search Engines class taught at New York University during Fall 2014 by Professors Cong Yu and Fernando Diaz.

The homeworks lead to building a simple web search engine.


#Assignment 2

##INDEX CONSTRUCTION

Assuming that the current directory contains the "src" , "data", "lib", and "conf" folders. Compilation of source codes can be done with the following command:

tt1161@linserv1[hw2]$ javac -cp .:./lib/*:./src edu/nyu/cs/cs2580/*.java

To run the SearchEngine in the "index" mode:

tt1161@linserv1[hw2]$ java -Xmx512m -cp .:./lib/*:./src edu.nyu.cs.cs2580.SearchEngine --mode=index --options=./conf/engine.conf

To run the SearchEngine in the "serve" mode:

tt1161@linserv1[hw2]$ java -Xmx512m -cp .:./lib/*:./src edu.nyu.cs.cs2580.SearchEngine --mode=serve --port=25814 --options=./conf/engine.conf

Please make sure that the all JAR files in the "lib" folder are included in the compilation, like above.

We construct the index using the merging approach, as opposed to in-memory construction as provided by the original source code. 

Only metadata of the index is loaded into memory during "serve" mode. The metadata is the integer representation of all terms in the corpus with the offsets, which are used to jump into the posting list of the term in the index file. This is the first portion in the index file. In the later portion is the posting lists corresponding to each term. The client codes can read the posting list from the file by supplying the integer representation. This way, only the metadata is stored in memory, not the posting lists.


##INDEX LOADING

Our index size for IndexerInvertedDoconly, IndexerInvertedOccurrence, IndexerInvertedCompressed are roughly 30, 102, 44 MBs respectively.

In finding a reasonable compromise between space and time, we chose to maintain the following data in memory while SearchEngine is running in the "serve" mode:

Map<Integer, FileRange> _index:   An index, which is a mapping between an integer representation of a term and a byte range in the file where the postings list for the term is located.

long _indexOffset:	An offset in the file where the postings lists begin (after all metadata).
	
Vector<Document> _documents:	A vector of documents consisting of metadata.
  
BiMap<String, Integer> _dictionary:	Maps each term to its integer representation

Map<Integer, Integer> _termCorpusFrequency:	Term frequency, key is the integer representation of the term and value is the number of times the term appears in the corpus.

Set<String> _stoppingWords:		The set contains stopping words, corresponding to the top 50 most frequent words in a corpus.

In addition to these fields, the IndexerInvertedOccurrence and IndexerInvertedCompressed maintain Map<Integer, Integer> _corpusDocFrequencyByTerm


##DOCUMENT PROCESSING

The TextUtils.java is provided to perform document processing.

The functionality includes:

1.) Removing non-visible context of the page: The 3rd library, Boilerpipe, is used. The library website is at: https://code.google.com/p/boilerpipe/

2.) Removing punctuations

3.) Stemming: use of step 1 of Porter's algorithm to perform stemming

4.) Removing initial dots

5.) deAccent

A set of stop words is determined by the top 50 most frequent terms in a corpus. However, no stop word is removed during construction of the index. At run-time, when a user issues a query, processing of inverted list of stop words is skipped.


##REPRESENTATION OF DOCUMENTINDEXED

In addition to the super class's fields, DocumentIndexed has the field:  private double _tfidfSumSquared, which is precomputed in the index construction process, so that the body token vectors do not have to be maintained.


##REPRESENTATION OF QUERYPHRASE

In addition to the super class's fields, QueryPhrase maintains the field: public Map<String, List<String>> _phrases. The key of the map is the phrase, and the value is a list of tokens in the phrase.


##FAVORITE RANKER

Cosine ranker was chosen as the favorite. The RankerFavorite class extends RankerCosine.

In the construction index phase, the normalizing factor of a document vector is precomputed so that body tokens do not need to be stored in DocumentIndexed. This saves significant memory.





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