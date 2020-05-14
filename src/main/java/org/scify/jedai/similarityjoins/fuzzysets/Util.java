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

import com.esotericsoftware.minlog.Log;
import gnu.trove.list.TIntList;
import gnu.trove.map.TIntFloatMap;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.TObjectIntMap;
import gnu.trove.set.TIntSet;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.*;
import java.util.Map.Entry;

public class Util {

    /**
     * Sorts a map by its values
     * @param <K>
     * @param <V>
     * @param map
     * @return 
     */
    public static <K extends Comparable<? super K>, V extends Comparable<? super V>> LinkedHashMap<K, V> sortByValue(
            Map<K, V> map) {
        List<Entry<K, V>> list = new ArrayList<>(map.entrySet());
        Comparator<Entry<K, V>> byValue = Entry.comparingByValue();
        Comparator<Entry<K, V>> byKey = Entry.comparingByKey();
        Comparator<Entry<K, V>> byValueThenByKey = byValue.thenComparing(byKey);
        list.sort(byValueThenByKey);

        LinkedHashMap<K, V> result = new LinkedHashMap<>();
        list.forEach((entry) -> {
            result.put(entry.getKey(), entry.getValue());
        });

        return result;
    }

    /**
     * Prints a collection with string tokens
     * @param collection
     * @param maxN
     */
    public static void printCollection(Map<String, List<Set<String>>> collection, int maxN) {

        int counter = 0, i, j;
        List<Set<String>> elements;

        for (String set : collection.keySet()) {
            if (counter >= maxN) {
                break;
            }
            counter++;
            System.out.print(set + "|");
            elements = collection.get(set);
            i = 1;
            for (Set<String> element : elements) {
                j = 1;
                for (String token : element) {
                    System.out.print(token);
                    if (j < element.size()) {
                        System.out.print(",");
                    }
                    j++;
                }
                if (i < elements.size()) {
                    System.out.print(";");
                }
                i++;
            }
            System.out.println();
        }
    }

    /**
     * Prints a collection with integer tokens
     * @param collection
     * @param maxN
     */
    public static void printCollection(int[][][] collection, int maxN) {

        for (int i = 0; i < collection.length; i++) {
            if (i >= maxN) {
                break;
            }
            System.out.print(i + "|");
            for (int j = 0; j < collection[i].length; j++) {
                for (int k = 0; k < collection[i][j].length; k++) {
                    System.out.print(collection[i][j][k]);
                    if (k < collection[i][j].length - 1) {
                        System.out.print(",");
                    }
                }
                if (j < collection[i].length - 1) {
                    System.out.print(";");
                }
            }
            System.out.println();
        }
    }

    /**
     * Prints a map of tokens
     * @param tokenMap
     * @param maxN
     */
    public static void printTokenMap(TObjectIntMap<String> tokenMap, int maxN) {

        int counter = 0;
        for (String token : tokenMap.keySet()) {
            counter++;
            if (counter > maxN) {
                break;
            }
            System.out.println(token + " : " + tokenMap.get(token));
        }
    }

    /**
     * Prints a map
     * @param <K>
     * @param <V>
     * @param map
     * @param maxN
     */
    public static <K, V> void printMap(Map<K, V> map, int maxN) {

        int counter = 0;
        for (Object key : map.keySet()) {
            counter++;
            if (counter > maxN) {
                break;
            }
//            System.out.println(key + " : " + map.get(key));
        }

    }

    /**
     * Prints the contents of an inverted index
     * @param idx
     * @param maxN
     */
    public static void printInvertedIndex(int[][][] idx, int maxN) {

        for (int i = 0; i < idx.length; i++) {
            if (i >= maxN) {
                break;
            }
            System.out.print(i + "|");
            for (int j = 0; j < idx[i].length; j++) {
                System.out.print(idx[i][j][0] + "," + idx[i][j][1]);
                if (j < idx[i].length - 1) {
                    System.out.print(";");
                }
            }
            System.out.println();
        }
    }

    /**
     * Prints the contents of an inverted index
     * @param idx
     * @param maxN
     */
    public static void printSetInvertedIndex(TIntObjectMap<TIntList>[] idx, int maxN) {
        for (int i = 0; i < idx.length; i++) {
            if (i >= maxN) {
                break;
            }
            System.out.print(i + "|");
            for (int s : idx[i].keys()) {
                System.out.print(" " + s + ":");
                for (int e : idx[i].get(s).toArray()) {
                    System.out.print(e + ",");
                }
            }
            System.out.println();
        }
    }

    /**
     * Prints a list of matching pairs
     * @param matchingPairs
     * @param maxN
     */
    public static void printMatchingPairs(List<int[]> matchingPairs, int maxN) {

        for (int i = 0; i < matchingPairs.size(); i++) {
            if (i >= maxN) {
                break;
            }
            System.out.println(matchingPairs.get(i)[0] + "," + matchingPairs.get(i)[1]);
        }
    }

    /**
     * Prints the unflattened signature of a set
     * @param unflattenedSignature
     */
    public static void printUnflattenedSignature(TIntSet[] unflattenedSignature) {

        int tokenCount, elementCount = 0;
        for (TIntSet elementSignature : unflattenedSignature) {
            tokenCount = 0;
            for (int token : elementSignature.toArray()) {
                System.out.print(token);
                if (tokenCount < elementSignature.size() - 1) {
                    System.out.print(",");
                }
                tokenCount++;
            }
            if (elementCount < unflattenedSignature.length - 1) {
                System.out.print(";");
            }
            elementCount++;
        }
        System.out.println();
    }

    public static void printCheckFilterCandidates(TIntObjectMap<TIntFloatMap> checkFilterCandidates) {
        for (int s : checkFilterCandidates.keys()) {
            System.out.print(s + "|");
            for (int e : checkFilterCandidates.get(s).keys()) {
                System.out.print(" " + e + ":" + checkFilterCandidates.get(s).get(e));
            }
            System.out.println();
        }
    }

    //public static void writeMatchingPairs(List<int[]> matchingPairs, String outFile, String stats, String statsFile,
    public static void writeMatchingPairs(HashMap<String, Float> matchingPairs, String outFile, String stats, String statsFile,
            String[] keys1, String[] keys2) {
        try {
            PrintWriter writer;

            //System.out.println(Arrays.toString(keys1));
//			System.out.println(Arrays.asList(keys1).indexOf("67194633@N00")+" "+Arrays.asList(keys1).indexOf("32706865@N07"));
            if (outFile != null && outFile.length() > 0) {
                writer = new PrintWriter(outFile);
//				for (int[] pair : matchingPairs) {
                for (String key : matchingPairs.keySet()) {
                    String[] pair = key.split("_");
//					writer.println(key+": "+matchingPairs.get(key));
//					writer.println(pair[0] + "_" + pair[1]);
                    writer.println(keys1[Integer.parseInt(pair[0])] + "," + keys2[Integer.parseInt(pair[1])] + "," + matchingPairs.get(key));
                }
                writer.close();
            }

            if (statsFile != null && statsFile.length() > 0) {
                writer = new PrintWriter(statsFile);
                writer.println(stats);
                writer.close();
            }
        } catch (FileNotFoundException e) {
            Log.error("Error while writing matching pairs", e);
        }
    }
}
