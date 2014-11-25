package edu.nyu.cs.cs2580;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;


/*This class is used to generate a simple corpus into multiple files*/

public class GenerateSimpleCorpus {

	public static final String corpusPath = "./data/simple/";
	public static final String corpusFileName = "corpus.tsv";
	public static HashMap<String,String> docMap = new HashMap<String,String>();  // key is the file name, value is the content
	
	public static void main(String[] args) throws Exception{
		generateFiles();
		System.out.println(docMap.size());
	}
	
	public static void generateFiles() throws Exception{
		BufferedReader reader = new BufferedReader(new FileReader(new File(corpusPath+corpusFileName)));
		String line = "";
		while((line = reader.readLine()) != null){
			String[] currentDoc = line.split("\t");
			docMap.put(currentDoc[0], currentDoc[1]);
			PrintWriter writer = new PrintWriter(corpusPath+"multiple files/"+currentDoc[0]);
			writer.print(currentDoc[1]);
			writer.close();
		}
		reader.close();
	}
}
