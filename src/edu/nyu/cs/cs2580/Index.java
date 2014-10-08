package edu.nyu.cs.cs2580;

import java.io.IOException;
import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;
import java.util.Vector;

class Index {
  public Vector < Document > _documents;
  public Index(String index_source){
    System.out.println("Indexing documents ...");

    _documents = new Vector < Document >();
    try {
      BufferedReader reader = new BufferedReader(new FileReader(index_source));
      try {
        String line = null;
        int did = 0;
        while ((line = reader.readLine()) != null){
          Document d = new Document(did);
          _documents.add(d);
          did++;
        }
      } finally {
        reader.close();
      }
    }catch (IOException ioe){
      System.err.println("Oops " + ioe.getMessage());
    }
    System.out.println("Done indexing " + Integer.toString(_documents.size()) + " documents...");
  }

  public int documentFrequency(String s){ return 0; }
  public int termFrequency(String s){
    return 0;
  }
  public int termFrequency(){ return 0; }
  public int numDocs(){
    return _documents.size();
  }

  public Document getDoc(int did){
    return (did >= _documents.size() || did < 0) ? null : _documents.get(did);
  } 
}
