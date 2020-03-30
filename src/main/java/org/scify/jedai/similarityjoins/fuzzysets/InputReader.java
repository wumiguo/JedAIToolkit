/*
* Copyright [2016-2020] [George Papadakis (gpapadis@yahoo.gr)]
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
 */

package org.scify.jedai.similarityjoins.fuzzysets;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class InputReader {

    /**
     * Reads input from a CSV file
     */
    public Map<String, List<Set<String>>> importCollectionFromFile(String file, int setCol, int tokenCol,
            String columnDelimiter, String tokenDelimiter, int header, int maxLines) {

        Map<String, List<Set<String>>> collection = new LinkedHashMap<>();

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
                    tokens = new HashSet<>(Arrays.asList(columns[tokenCol].split(tokenDelimiter)));

                    elements = collection.get(set);
                    if (elements == null) {
                        elements = new ArrayList<>();
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
