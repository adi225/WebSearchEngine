package edu.nyu.cs.cs2580;
import java.net.*;
import java.util.HashMap;
import java.util.Scanner;
import java.io.*;

public class GenerateCorpusFromAOL {

	public static HashMap<String,Integer> urlMapDocId = new HashMap<String,Integer>();
	
	public static final String aolFilePath = "D:/NYU/Courses/Web Search Engine/Fall 2014/Project/AOL-user-ct-collection/user-ct-test-collection-01.txt";
	public static final String outputFolderPath = "D:/NYU/Courses/Web Search Engine/Fall 2014/Project/AOL-corpus/";
	
	public static void main(String[] args) throws Exception{
		generateCorpus();
	}
	
	public static void generateMapping() throws Exception{
		
	}
	
	public static void generateCorpus() throws Exception{
		int docID = 0;
		
		File file = new File(aolFilePath);
		BufferedReader reader = new BufferedReader(new FileReader(file));
		String line = reader.readLine();
		while((line = reader.readLine()) != null){
			String[] components = line.split("\t");
			if(components.length == 5){
				String urlComponent = components[4];
				if(urlMapDocId.containsKey(urlComponent)){
					continue;
				}
				try{
					URL url = new URL(urlComponent);
			    BufferedReader in = new BufferedReader(new InputStreamReader(url.openStream()));
			    String inputLine = "";
			    PrintWriter writer = new PrintWriter(outputFolderPath + docID);
			    while ((inputLine = in.readLine()) != null){
			    	writer.println(inputLine);
			    }
			    writer.close();
			    in.close();
			    System.out.println("Processed docID: "+docID);
			    urlMapDocId.put(urlComponent, docID);
			    docID++;
				}
				catch(Exception e){
					continue;
				}
			}
		}
		reader.close();
	}
	
	public static boolean containsTitle(String line){
		return line.contains("<title>") && line.contains("</title>") ? true : false;
	}
	
	public static String getTitle(String line){
		line = line.trim();
		return line.substring(7,line.length() - 8);
	}

}
