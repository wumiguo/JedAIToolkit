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

import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TObjectIntHashMap;

import java.util.*;

public class InputTransformer {

    /**
     * Computes the frequencies of tokens
     * @param collection
     * @return 
     */
    public LinkedHashMap<String, Integer> computeTokenFrequencies(Collection<List<Set<String>>> collection) {
        LinkedHashMap<String, Integer> tokFreq = new LinkedHashMap<>();

        Integer frequency;
        for (List<Set<String>> set : collection) {
            for (Set<String> element : set) {
                for (String token : element) {
                    frequency = tokFreq.get(token);
                    if (frequency == null) {
                        frequency = 0;
                    }
                    frequency++;
                    tokFreq.put(token, frequency);
                }
            }
        }

        tokFreq = Util.sortByValue(tokFreq);

        return tokFreq;
    }

    /**
     * Maps tokens to integer IDs
     */
    TObjectIntMap<String> mapTokensToInts(LinkedHashMap<String, Integer> tokenFrequencies) {

        TObjectIntMap<String> tokenDict = new TObjectIntHashMap<>();

        int counter = 0;

        for (String token : tokenFrequencies.keySet()) {
            tokenDict.put(token, counter);
            counter++;
        }

        return tokenDict;
    }

    /**
     * Creates a token dictionary with IDs ordered by frequency
     * @param sets
     * @return 
     */
    public TObjectIntMap<String> mapTokensToIntsByFrequency(Collection<List<Set<String>>> sets) {
        LinkedHashMap<String, Integer> tokenFreq = computeTokenFrequencies(sets);
        TObjectIntMap<String> tokenDict = mapTokensToInts(tokenFreq);
        return tokenDict;
    }

    /**
     * Replaces string tokens with integer IDs
     * @param input
     * @param tokenDictionary
     * @return 
     */
    public int[][][] transform(Map<String, List<Set<String>>> input, TObjectIntMap<String> tokenDictionary) {

        int[][][] collection = new int[input.size()][][];

        boolean existingDictionary = tokenDictionary.size() > 0;
        int unknownTokenCounter = 0;

        int i = 0, j, k;
        List<Set<String>> elements;
        for (String set : input.keySet()) {
            elements = input.get(set);
            collection[i] = new int[elements.size()][];
            j = 0;
            for (Set<String> element : elements) {
                collection[i][j] = new int[element.size()];
                k = 0;
                for (String token : element) {
                    if (!tokenDictionary.containsKey(token)) {
                        if (existingDictionary) {
                            unknownTokenCounter--;
                            tokenDictionary.put(token, unknownTokenCounter);
                        } else {
                            tokenDictionary.put(token, tokenDictionary.size());
                        }
                    }
                    collection[i][j][k] = tokenDictionary.get(token);
                    k++;
                }
                j++;
            }
            i++;
        }

        return collection;
    }
}
