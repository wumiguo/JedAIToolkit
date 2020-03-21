package org.scify.jedai.similarityjoins.fuzzy_set_simjoin;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class InputReader {

	/** Reads input from a CSV file */
	public Map<String, List<Set<String>>> importCollectionFromFile(String file, int setCol, int tokenCol,
			String columnDelimiter, String tokenDelimiter, int header, int maxLines) {

		Map<String, List<Set<String>>> collection = new LinkedHashMap<String, List<Set<String>>>();

		BufferedReader br;
		int lines = 0;
		int errorLines = 0;
		try {
			br = new BufferedReader(new FileReader(file));

			String line, set;
			String[] columns;
			Set<String> tokens;
			List<Set<String>> elements;

			// if the file has header, ignore the first line
			if (header == 1) {
				br.readLine();
			}

			while ((line = br.readLine()) != null) {
				try {
					columns = line.split(columnDelimiter);

					set = columns[setCol];
					tokens = new HashSet<String>(Arrays.asList(columns[tokenCol].split(tokenDelimiter)));

					elements = collection.get(set);
					if (elements == null) {
						elements = new ArrayList<Set<String>>();
					}
					elements.add(tokens);
					collection.put(set, elements);
					lines++;
				} catch (Exception e) {
					errorLines++;
				}
				if (maxLines > -1 && lines >= maxLines) {
					break;
				}
			}

			br.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		double elementsPerSet = 0;
		for (String set : collection.keySet()) {
			elementsPerSet += collection.get(set).size();
		}
		elementsPerSet /= collection.size();

		System.out.println("Finished reading file. Lines read: " + lines + ". Lines skipped due to errors: "
				+ errorLines + ". Num of sets: " + collection.size() + ". Elements per set: " + elementsPerSet);

		return collection;
	}
}