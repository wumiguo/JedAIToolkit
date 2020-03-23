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

import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;

import java.util.ArrayList;

public class IndexConstructor {

    /**
     * Constructs an inverted index from a given collection
     */
    public int[][][] buildInvertedIndex(int[][][] collection, int numTokens) {

        @SuppressWarnings("unchecked")
        ArrayList<int[]>[] tmpIdx = new ArrayList[numTokens];
        int[] tuple;

        for (int i = 0; i < tmpIdx.length; i++) {
            tmpIdx[i] = new ArrayList<int[]>();
        }

        for (int i = 0; i < collection.length; i++) {
            for (int j = 0; j < collection[i].length; j++) {
                for (int k = 0; k < collection[i][j].length; k++) {
                    tuple = new int[]{i, j};
                    tmpIdx[collection[i][j][k]].add(tuple);
                }
            }
        }

        int[][][] idx = new int[numTokens][][];
        for (int i = 0; i < tmpIdx.length; i++) {
            idx[i] = new int[tmpIdx[i].size()][];
            for (int j = 0; j < tmpIdx[i].size(); j++) {
                idx[i][j] = new int[]{tmpIdx[i].get(j)[0], tmpIdx[i].get(j)[1]};
            }
        }

        return idx;
    }

    public TIntObjectMap<TIntList>[] buildSetInvertedIndex(int[][][] collection, int numTokens) {

        // initialize the index
        @SuppressWarnings("unchecked")
        TIntObjectMap<TIntList>[] idx = new TIntObjectHashMap[numTokens];
        for (int i = 0; i < idx.length; i++) {
            idx[i] = new TIntObjectHashMap<TIntList>();
        }

        // populate the index
        TIntList invList;
        int token;
        for (int i = 0; i < collection.length; i++) {
            for (int j = 0; j < collection[i].length; j++) {
                for (int k = 0; k < collection[i][j].length; k++) {
                    token = collection[i][j][k];
                    if (idx[token].containsKey(i)) {
                        invList = idx[token].get(i);
                    } else {
                        invList = new TIntArrayList();
                    }
                    invList.add(j);
                    idx[token].put(i, invList);
                }
            }
        }

        return idx;
    }
}
