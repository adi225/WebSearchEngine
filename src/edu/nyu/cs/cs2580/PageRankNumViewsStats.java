package edu.nyu.cs.cs2580;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

public class PageRankNumViewsStats {

	public static String pageRankFilePath = "./data/index/pagerank";
	public static String numViewFilePath = "./data/index/numviews";
	
	public static void main(String[] args) throws Exception{
		printStats();
	}
	
	public static void printStats() throws Exception{
		countMinMaxPageRank();
		System.out.println();
		countMinMaxNumViews();
	}
	
	public static void countMinMaxPageRank() throws Exception{
		BufferedReader reader = new BufferedReader(new FileReader(new File(pageRankFilePath)));
		String line = "";
		double maxPageRank = Double.MIN_VALUE;
		double minPageRank = Double.MAX_VALUE;
		int numMinus2 = 0;
		int numMinus3 = 0;
		int numMinus4 = 0;
		int numMinus5 = 0;
		int numMinus6 = 0;
		int totalDoc = 0;
		while((line = reader.readLine()) != null){
			String[] strs = line.split(" ");
			double pageRank = Double.parseDouble(strs[1]);
			if(pageRank * Math.pow(10, 2) >= 1 && pageRank * Math.pow(10, 2) <=10){
				numMinus2++;
			}
			else if(pageRank * Math.pow(10, 3) >= 1 && pageRank * Math.pow(10, 3) <=10){
				numMinus3++;
			}
			else if(pageRank * Math.pow(10, 4) >= 1 && pageRank * Math.pow(10, 4) <=10){
				numMinus4++;
			}
			else if(pageRank * Math.pow(10, 5) >= 1 && pageRank * Math.pow(10, 5) <=10){
				numMinus5++;
			}
			else if(pageRank * Math.pow(10, 6) >= 1 && pageRank * Math.pow(10, 6) <=10){
				numMinus6++;
			}
			maxPageRank = Math.max(maxPageRank, pageRank);
			minPageRank = Math.min(minPageRank, pageRank);
			totalDoc++;
		}
		System.out.println("Max PageRank: "+maxPageRank);
		System.out.println("Min PageRank: "+minPageRank);
		System.out.println("Num of docs with PageRank in order of 10^-6: "+numMinus6 + "\t" +(double)numMinus6*100.0/totalDoc + "%");
		System.out.println("Num of docs with PageRank in order of 10^-5: "+numMinus5 + "\t" +(double)numMinus5*100.0/totalDoc + "%");
		System.out.println("Num of docs with PageRank in order of 10^-4: "+numMinus4 + "\t" +(double)numMinus4*100.0/totalDoc + "%");
		System.out.println("Num of docs with PageRank in order of 10^-3: "+numMinus3 + "\t" +(double)numMinus3*100.0/totalDoc + "%");
		System.out.println("Num of docs with PageRank in order of 10^-2: "+numMinus2 + "\t" +(double)numMinus2*100.0/totalDoc + "%");
		System.out.println("Total number of documents: "+totalDoc);
	}
	
	public static void countMinMaxNumViews() throws Exception{
		BufferedReader reader = new BufferedReader(new FileReader(new File(numViewFilePath)));
		String line = "";
		double maxNumViews = Double.MIN_VALUE;
		double minNumViews = Double.MAX_VALUE;
		int numPlus0 = 0;
		int numPlus1 = 0;
		int numPlus2 = 0;
		int numPlus3 = 0;
		int numPlus4 = 0;
		int numPlus5 = 0;
		int totalDoc = 0;
		while((line = reader.readLine()) != null){
			String[] strs = line.split(" ");
			double numViews = Double.parseDouble(strs[1]);
			if(numViews >= 0 && numViews < 10){
				numPlus0++;
			}
			else if(numViews >= 10 && numViews < 100){
				numPlus1++;
			}
			else if(numViews >= 100 && numViews < 1000){
				numPlus2++;
			}
			else if(numViews >= 1000 && numViews < 10000){
				numPlus3++;
			}
			else if(numViews >= 10000 && numViews < 100000){
				numPlus4++;
			}
			else if(numViews >= 100000 && numViews < 1000000){
				numPlus5++;
			}
			maxNumViews = Math.max(maxNumViews, numViews);
			minNumViews = Math.min(minNumViews, numViews);
			totalDoc++;
		}
		System.out.println("Max NumViews: "+maxNumViews);
		System.out.println("Min NumViews: "+minNumViews);
		System.out.println("Num of docs with NumViews in order of 10^0: "+numPlus0 + "\t" +(double)numPlus0*100.0/totalDoc + "%");
		System.out.println("Num of docs with NumViews in order of 10^1: "+numPlus1 + "\t" +(double)numPlus1*100.0/totalDoc + "%");
		System.out.println("Num of docs with NumViews in order of 10^2: "+numPlus2 + "\t" +(double)numPlus2*100.0/totalDoc + "%");
		System.out.println("Num of docs with NumViews in order of 10^3: "+numPlus3 + "\t" +(double)numPlus3*100.0/totalDoc + "%");
		System.out.println("Num of docs with NumViews in order of 10^4: "+numPlus4 + "\t" +(double)numPlus4*100.0/totalDoc + "%");
		System.out.println("Num of docs with NumViews in order of 10^5: "+numPlus5 + "\t" +(double)numPlus5*100.0/totalDoc + "%");
		System.out.println("Total number of documents: "+totalDoc);
	}
}
