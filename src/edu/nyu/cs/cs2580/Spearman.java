package edu.nyu.cs.cs2580;

import java.io.*;
import java.util.*;

/**
 * This is the main entry class for the Spearman coefficient
 *
 * Usage (must be running from the parent directory of src):
 *  0) Compiling
 *   javac src/edu/nyu/cs/cs2580/*.java
 *  1) Running
 *   java -cp src edu.nyu.cs.cs2580.Spearman <PATH-TO-PAGERANK> <PATH-TO-NUMVIEWS>
 */

public class Spearman
{
    static String pathToPagerank = null;
    static String pathToNumviews = null;
    static BufferedReader br = null;

    public static void main(String[] args) throws Exception {
        if (args == null || args.length != 2)
        {
            System.err.println("Fatal error: needs 2 parameters");
            System.exit(-1);
        }

        pathToPagerank = args[0];
        pathToNumviews = args[1];

        // count the number of documents we are considering
        br = new BufferedReader(new FileReader(pathToPagerank));
        String line = null;
        int n = 0;
        while ((line = br.readLine()) != null)
        {
            n++;
        }
        br.close();

        String[] singleLine;
        String doc;
        Double[] pageRank = new Double[n];
        Integer[] numView = new Integer[n];
        double[] x_k, y_k;
        List<String> docName = new ArrayList<String>();

        for (int i = 0; i < n; i++)
            numView[i] = 0;

        br = new BufferedReader(new FileReader(pathToPagerank));
        for (int i = 0; i < n; i++)
        {
            line = br.readLine();
            singleLine = line.split("\\s+");
            docName.add(singleLine[0]);
            pageRank[i] = Double.parseDouble(singleLine[1]);
        }
        br.close();

        br = new BufferedReader(new FileReader(pathToNumviews));
        while ((line = br.readLine()) != null)
        {
            singleLine = line.split("\\s+");
            doc = singleLine[0];
            if (docName.contains(doc))
            {
                numView[docName.indexOf(doc)] = Integer.parseInt(singleLine[1]);
            }
        }
        br.close();

        x_k = sortRank(pageRank);
        y_k = sortRank(numView);

        System.out.println(docName);
        System.out.println(Arrays.toString(pageRank));
        System.out.println(Arrays.toString(numView));
        System.out.println(Arrays.toString(x_k));
        System.out.println(Arrays.toString(y_k));

        int z = 0;
        for (int i = 0; i < x_k.length; i++)
        {
            z += x_k[i];
        }
        z /= x_k.length;

        double top = 0, bottom1 = 0, bottom2 = 0;
        for (int i = 0; i < x_k.length; i++)
        {
            top += (x_k[i]-z)*(y_k[i]-z);
            bottom1 += Math.pow((x_k[i]-z),2);
            bottom2 += Math.pow((y_k[i]-z),2);
        }
        double rho = top/(bottom1*bottom2);
        System.out.print(rho);
    }

    public static double[] sortRank(Double[] data) throws Exception {
        int n = data.length;
        int numDuplicates;
        double rankEle, rankNum = 1;
        double[] rank = new double[n];
        Double[] tempData= new Double[n];
        System.arraycopy(data, 0, tempData, 0, n);
        tempData = removeDuplicates(tempData);
        Arrays.sort(tempData, Collections.reverseOrder());

        for (int i = 0; i < tempData.length; i++)
        {
            numDuplicates = 0;
            for (int j = 0; j < n; j++)
                if (data[j]==tempData[i])
                    numDuplicates++;
            rankEle = rankNum + 0.5*(numDuplicates-1);
            for (int j = 0; j < n; j++)
            {
                if (data[j]==tempData[i])
                    rank[j] = rankEle;
            }
            rankNum = rankNum + numDuplicates;
        }
        return rank;
    }

    public static double[] sortRank(Integer[] data) throws Exception {
        int n = data.length;
        int numDuplicates;
        double rankEle, rankNum = 1;
        double[] rank = new double[n];
        Integer[] tempData= new Integer[n];
        System.arraycopy(data, 0, tempData, 0, n);
        tempData = removeDuplicates(tempData);
        Arrays.sort(tempData, Collections.reverseOrder());

        for (int i = 0; i < tempData.length; i++)
        {
            numDuplicates = 0;
            for (int j = 0; j < n; j++)
                if (data[j]==tempData[i])
                    numDuplicates++;
            rankEle = rankNum + 0.5*(numDuplicates-1);
            for (int j = 0; j < n; j++)
            {
                if (data[j]==tempData[i])
                    rank[j] = rankEle;
            }
            rankNum = rankNum + numDuplicates;
        }
        return rank;
    }

    public static Integer[] removeDuplicates(Integer[] arr) {
        return new HashSet<Integer>(Arrays.asList(arr)).toArray(new Integer[0]);
    }

    public static Double[] removeDuplicates(Double[] arr) {
        return new HashSet<Double>(Arrays.asList(arr)).toArray(new Double[0]);
    }
}
