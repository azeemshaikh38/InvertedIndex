import java.io.*;
import java.util.*;

public class QueryFullIndexTwoWord {
	public static void main(String[] args) throws IOException {
		FileReader fpResult = new FileReader(args[1]);
		BufferedReader fpBufferedResult = new BufferedReader(fpResult);
		String lineResult;
		String[] splitLine;
		Hashtable<String, String> hashDoc = new Hashtable<String, String>();
		//Create a HashTable using the full inverted index file from MapReduce program
		while ((lineResult = fpBufferedResult.readLine()) != null) {
			splitLine = lineResult.split("\\s+:\\s*");
			hashDoc.put(splitLine[0], splitLine[1]);
		}

		PrintWriter fpOut = new PrintWriter(args[2]);
		FileReader fpQuery = new FileReader(args[0]);
		BufferedReader fpBufferedQuery = new BufferedReader(fpQuery);
		String lineQuery;
		String[] lineDocs;
		String[] lineDocSplit;
		
		while ((lineQuery = fpBufferedQuery.readLine()) != null) {
			//Make sure only one whitespace exist between query words
			String[] twoWords = lineQuery.split("\\s+");
			if (twoWords.length >= 2) {
				lineDocs = hashDoc.get(twoWords[0]+" "+twoWords[1]).split(",\\s+");
				fpOut.println(lineQuery+":");
				//For every file@offset value do below
				for (int i = 0; i < lineDocs.length; i++) {
					lineDocSplit = lineDocs[i].split("@");	
					RandomAccessFile docSeeker = new RandomAccessFile(lineDocSplit[0], "r");
					docSeeker.seek(Long.parseLong(lineDocSplit[1]));
					fpOut.println(lineDocs[i]+" -> "+docSeeker.readLine());
					docSeeker.close();
				}
			}
		}
		fpOut.close();	
	}
}
