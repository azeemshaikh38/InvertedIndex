import java.io.*;
import java.util.*;

public class QueryIndex {
	public static void main(String[] args) throws IOException {
		//Read in the 2nd argument which is the Inverted Index result file from the MapReduce job
		FileReader fpResult = new FileReader(args[1]);
		BufferedReader fpBufferedResult = new BufferedReader(fpResult);
		String lineResult;
		String[] splitLine;
		Hashtable<String, String> hashDoc = new Hashtable<String, String>();

		//Read the file and create a HashTable with word as key and the rest as value
		while ((lineResult = fpBufferedResult.readLine()) != null) {
			splitLine = lineResult.split("\\s+:\\s*");
			hashDoc.put(splitLine[0], splitLine[1]);
		}

		//Open the 3rd argument for writing
		PrintWriter fpOut = new PrintWriter(args[2]);

		//Parse the 1st argument which is the Query file 
		FileReader fpQuery = new FileReader(args[0]);
		BufferedReader fpBufferedQuery = new BufferedReader(fpQuery);
		String lineQuery;
		
		while ((lineQuery = fpBufferedQuery.readLine()) != null) {
			//For every query word, print using the HashTable created previously
			fpOut.println(lineQuery+"\t"+hashDoc.get(lineQuery));
		}
		fpOut.close();	
	}
}
