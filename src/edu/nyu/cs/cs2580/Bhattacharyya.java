package edu.nyu.cs.cs2580;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Created by farah farhat on 11/14/14.
 *
 * This is the main entry class for the Pseudo-Relevance Feedback - Query Similarity
 *
 * Usage (must be running from the parent directory of src):
 *  0) Compiling
 *   javac src/edu/nyu/cs/cs2580/*.java
 *  1) Running
 *   java -cp src edu.nyu.cs.cs2580.Bhattacharyya <PATH-TO-PRF-OUTPUT> <PATH-TO-OUTPUT>
 */

public class Bhattacharyya
{
	static String pathToPrfOutput = null;
	static String pathToOutput = null;
	static BufferedReader br = null;
	static BufferedReader queryBr = null;
	
	public static class QueryPrf
	{
		String _term = null;
		double _probability = 0;
		
		public QueryPrf(String term, double probability)
		{
			_term = term;
			_probability = probability;
		}
		
		@Override
		public boolean equals(Object o) 
		{
		    if (this._term.equals(((QueryPrf)o)._term))
		    {
		      return true;
		    }
		    return false;
		}
	}
	
	public static void main(String[] args)
	{		
		if (args == null || args.length != 2)
		{
			System.err.println("Fatal error: needs 2 parameters");
		    System.exit(-1);
		}
		
		pathToPrfOutput = args[0];
		pathToOutput = args[1];
		
		Map<String, List<QueryPrf>> allQueries = new HashMap<String, List<QueryPrf>>();
		
		try
		{
			/* Loop over contents of the file
			 * Each line is
			 * query:filename
			 * where query is the name of the query and filename is the name of the file 
			 * that has the prf of the query
			 * File format is:
			 * <TERM-1><PROB-1>
			 * <TERM-2><PROB-2>
			 * ...
			 * <TERM-m><PROB-m>
			 * 
			 */
					
			br = new BufferedReader(new FileReader(pathToPrfOutput));  
			String line = null;  
			while ((line = br.readLine()) != null)  
			{  
			   //parse line on :
				String[] singleLine = line.split(":");
				if(singleLine == null || singleLine.length != 2)
					throw new Exception("Wrong file format");
				
				String singleQuery = singleLine[0];
				String singleFileName = singleLine[1];
				
				queryBr = new BufferedReader(new FileReader(singleFileName));  
				String queryLine = null;  
				List<QueryPrf> singleQueryTerms = new ArrayList<QueryPrf>();
				while ((queryLine = queryBr.readLine()) != null)  
				{  
					//Each line is <TERM-1><PROB-1> tab-separated
					singleLine = queryLine.split("\\t");
					if(singleLine == null || singleLine.length != 2)
						throw new Exception("Wrong file format: " + singleLine);
					
					QueryPrf newQueryPrf = new QueryPrf(singleLine[0], Double.parseDouble(singleLine[1]));
					singleQueryTerms.add(newQueryPrf);
					
				}
				allQueries.put(singleQuery, singleQueryTerms);
			} 
			
			br.close();
		
		
			File file = new File(pathToOutput);
			 
			// if file doesn't exists, then create it
			if (!file.exists()) 
			{
				file.createNewFile();
			}
	
			FileWriter fw = new FileWriter(file.getAbsoluteFile());
			BufferedWriter bw = new BufferedWriter(fw);
			DecimalFormat df = new DecimalFormat("#.##"); 
			
	        
			for (Entry<String, List<QueryPrf>> firstEntry : allQueries.entrySet())
	        {
		        List<QueryPrf> list1 = (List<QueryPrf>)firstEntry.getValue();
		        
		        List<String> localVocab = new ArrayList<String>();
		        
		        for(QueryPrf singleQueryPrf : list1)
		        {
		        	if(!localVocab.contains(singleQueryPrf._term))
		        		localVocab.add(singleQueryPrf._term);
		        }
		        		
		        for (Map.Entry<String, List<QueryPrf>> secondEntry : allQueries.entrySet())
		        {
		        	List<QueryPrf> list2 = (List<QueryPrf>)secondEntry.getValue();
		        	
		        	for(QueryPrf singleQueryPrf : list2)
			        {
			        	if(!localVocab.contains(singleQueryPrf._term))
			        		localVocab.add(singleQueryPrf._term);
			        }
		        	
		        	/*Compare 1 & 2*/
		        	if(!firstEntry.getKey().equals(secondEntry.getKey()))
		        	{
		        		double similarity = 0;
		        		for(String word : localVocab)
			        	{
			        		QueryPrf temp = new QueryPrf(word, 0);
			        		//if none or only 1 contain it, the value will be 0 - we can skip
			        		if(list1.contains(temp) && list2.contains(temp))
			        		{
			        			QueryPrf firstTerm = list1.get(list1.indexOf(temp));
			        			QueryPrf secondTerm = list2.get(list2.indexOf(temp));
			        			
			        			
			        			similarity += Math.sqrt(firstTerm._probability * secondTerm._probability);
			        		}
			        	}
			        	
			        	bw.write(firstEntry.getKey() + "\t" + secondEntry.getKey() + "\t" + df.format(similarity) + "\n");
		        	}
		        	
		        	
		        }
		    }
		    
			bw.close();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		    System.exit(-1);
		}
	}
}
