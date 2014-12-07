import java.io.*;
import java.util.*;
import java.util.regex.*;

public class QueryBooleanFull {

	private static Hashtable<String, String> hashDoc;
	private static PrintWriter fpOut;
	private static FileReader fpQuery;

	public static void main(String[] args) throws IOException {
		FileReader fpResult = new FileReader(args[1]);
		BufferedReader fpBufferedResult = new BufferedReader(fpResult);
		String lineResult;
		String[] splitLine;
		hashDoc = new Hashtable<String, String>();
		String[] result = null;
		String stringBuild = "";

		//Create HashTable 
		while ((lineResult = fpBufferedResult.readLine()) != null) {
			splitLine = lineResult.split("\\s+:\\s*");
			hashDoc.put(splitLine[0], splitLine[1]);
		}
		PrintWriter fpOut = new PrintWriter(args[2]);
		FileReader fpQuery = new FileReader(args[0]);
		BufferedReader fpBufferedQuery = new BufferedReader(fpQuery);
		String lineQuery;

		while ( (lineQuery = fpBufferedQuery.readLine()) != null ) {
			//Recursively evaluate the boolean expression and return array of document names
			result = evaluate(lineQuery);

			//Build the final string to be printed
			stringBuild = lineQuery+" -> ";
			for (int i = 0; i < result.length; i++) {
				stringBuild += result[i]+", ";
			}
			fpOut.println(stringBuild);
		}
		fpOut.close();
	}

	private static String[] evaluate(String expr) throws IOException {		
		
		String lineQuery = expr;
		String tmpLine;
		String[] lineQuerySplit;
		String[] lineDocs1;
		String[] lineDocs2;
		String[] lineDocSplit;
		List<String> returnString = new ArrayList<String>();
		int i1,i2;
	
		//If AND operator is found
		if (Pattern.compile("\\s+AND\\s+").matcher(lineQuery).find()) {
			lineQuerySplit = lineQuery.split("\\s+AND\\s+", 2);	
			//Resursive call
			lineDocs1 = evaluate(lineQuerySplit[0]);
			lineDocs2 = evaluate(lineQuerySplit[1]);
			i1 = 0; i2 = 0;
			//Merge both the document list to get final answer
			while ( (i1 < lineDocs1.length) && (i2 < lineDocs2.length) ) {
				if (lineDocs1[i1].compareTo(lineDocs2[i2]) < 0) {
					i1++;
				} else if (lineDocs2[i2].compareTo(lineDocs1[i1]) < 0) {
					i2++;
				} else {
					//Both documents match. So return this docId
					returnString.add(lineDocs1[i1]);
					i1++;i2++;
				}
			}
		} else if (Pattern.compile("\\s+OR\\s+").matcher(lineQuery).find()) {
		//If OR operator is found
			lineQuerySplit = lineQuery.split("\\s+OR\\s+");
                     	//Recursive call
			lineDocs1 = evaluate(lineQuerySplit[0]);
			lineDocs2 = evaluate(lineQuerySplit[1]);
                        i1 = 0; i2 = 0;

			//Merge all documents in both lists
                        while ( (i1 < lineDocs1.length) && (i2 < lineDocs2.length) ) {
                                if (lineDocs1[i1].compareTo(lineDocs2[i2]) < 0) {
					returnString.add(lineDocs1[i1]);
					i1++;
                                } else if (lineDocs2[i2].compareTo(lineDocs1[i1]) < 0) {
					returnString.add(lineDocs2[i2]);
					i2++;
                                } else {
					returnString.add(lineDocs1[i1]);
					i1++;i2++;
                                }
                        }

			//If any document list is yet to be traversed, add those too
			int i;
			if (i1 < lineDocs1.length) {
				for (i = i1; i < lineDocs1.length; i++) {
					//fpOut.println(lineDocs1[i]);
					returnString.add(lineDocs1[i]);
				}
			}
			if (i2 < lineDocs2.length) {
				for (i = i2; i < lineDocs2.length; i++) {
					//fpOut.println(lineDocs2[i]);
					returnString.add(lineDocs2[i]);
				}
			}
		} else if (Pattern.compile("\\s*NOT\\s+").matcher(lineQuery).find()) {
		//If NOT operator is found

				//Get all documents in which this word is present
				tmpLine = lineQuery.replaceAll("\\s*NOT\\s+", "");
				String[] docQuery = hashDoc.get(tmpLine.replaceAll("\\s+", "")).split(",\\s+");
	
				//Get all document names from the HashTable -> universal set
				Collection<String> docLineCollection = hashDoc.values();
				Iterator<String> hashValueItr = docLineCollection.iterator();
				List<String> printedList = new ArrayList<String>();

				//Iterate through entire universal set and save those which do not match current document list
				while (hashValueItr.hasNext()) {
					String[] docIdArray = hashValueItr.next().split(",\\s+");
					for (int i = 0; i< docIdArray.length; i++) {
						if (!(Arrays.asList(docQuery).contains(docIdArray[i]))) {
							//Avoid duplication of document names
							if ( returnString.isEmpty() || !(returnString.contains(docIdArray[i])) ) {
								returnString.add(docIdArray[i]);
							}
						}
					}
				}
		} else {
				//No boolean operator. Return default value
				String[] docQuery = hashDoc.get(lineQuery.replaceAll("\\s+", "")).split(",\\s+");
				for (int i = 0; i < docQuery.length; i++) {
					returnString.add(docQuery[i]);
				}
		}
		//Sort the ArrayList and return 
		Collections.sort(returnString);
		String[] returnArray = new String[returnString.size()];
		returnString.toArray(returnArray);
		return returnArray;	
	}
}
