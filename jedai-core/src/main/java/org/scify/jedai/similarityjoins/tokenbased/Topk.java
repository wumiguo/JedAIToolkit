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

package org.scify.jedai.similarityjoins.tokenbased;

import gnu.trove.iterator.TIntIntIterator;
import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.hash.TIntIntHashMap;
import javafx.util.Pair;
import org.scify.jedai.datamodel.Comparison;
import org.scify.jedai.datamodel.EntityProfile;
import org.scify.jedai.datamodel.SimilarityPairs;
import org.scify.jedai.datamodel.joins.IntPair;
import org.scify.jedai.similarityjoins.tokenbased.topk.SetSimJoin;

import java.util.*;

/**
 *
 * @author mthanos
 */
public class Topk extends AbstractTokenBasedJoin {

    private int k;//defines how many top pairs will be found

    private int[] originalId;
    private final List<String> attributeValues;
    private TIntList[] records;

    public Topk(double thr, int k) {
        super(thr);
        this.k = k;
        attributeValues = new ArrayList<>();
    }

    @Override
    public SimilarityPairs applyJoin(String attributeName1, String attributeName2, List<EntityProfile> dataset1, List<EntityProfile> dataset2) {
        init();

        final List<Comparison> comparisons = performJoin();
        return getSimilarityPairs(comparisons);
    }

    private void init() {

        int counter = 0;
        final List<Pair<String, Integer>> idIdentifier = new ArrayList<>();
        for (EntityProfile profile : profilesD1) {
            final String nextValue = getAttributeValue(attributeNameD1, profile);
            idIdentifier.add(new Pair<>(nextValue, counter++));
        }

        if (isCleanCleanER) {
            for (EntityProfile profile : profilesD2) {
                final String nextValue = getAttributeValue(attributeNameD2, profile);
                idIdentifier.add(new Pair<>(nextValue, counter++));
            }
        }

        idIdentifier.sort((s1, s2) -> s1.getKey().split(" ").length - s2.getKey().split(" ").length);

        attributeValues.clear();
        originalId = new int[noOfEntities];
        records = new TIntList[noOfEntities];
        for (int i = 0; i < noOfEntities; i++) {
            final Pair<String, Integer> currentPair = idIdentifier.get(i);
            attributeValues.add(currentPair.getKey());
            originalId[i] = currentPair.getValue();
            records[i] = new TIntArrayList();
        }
        TIntIntMap freqMap = new TIntIntHashMap();
        for (int sIndex = 0; sIndex < noOfEntities; sIndex++) {

            final String s = attributeValues.get(sIndex).trim();
            if (s.length() < 1) {
                continue;
            }

            String[] split = s.split(" ");
            for (int sp = 0; sp < split.length; sp++) {
                int token = djbHash(split[sp]);
                records[sIndex].add(token);
                int freq = freqMap.get(token);
                freqMap.put(token, (freq + 1));
            }

        }

        List<IntPair> tokenFreq = new ArrayList<>();
        for (TIntIntIterator iterator = freqMap.iterator(); iterator.hasNext();) {
            iterator.advance();
            tokenFreq.add(new IntPair(iterator.key(), iterator.value()));
        }
        tokenFreq.sort((p1, p2) -> p2.getValue() - p1.getValue());
        freqMap.clear();
        for (int i = 0; i < tokenFreq.size(); i++) {
            freqMap.put(tokenFreq.get(i).getKey(), i);
        }

        for (int sIndex = 0; sIndex < noOfEntities; sIndex++) {

            for (int iii = 0; iii < records[sIndex].size(); iii++) {
                int idfid = freqMap.get(records[sIndex].get(iii)) * 2 + 2;
                records[sIndex].set(iii, idfid);
            }
            records[sIndex].sort();
        }

    }

    private List<Comparison> performJoin() {

        SetSimJoin ssj = new SetSimJoin();
        LinkedHashMap<String, ArrayList<Integer>> recordsForTopk = new LinkedHashMap<>();
        int cnter = 0;
        for (TIntList rec : records) {
            ArrayList<Integer> tokens = new ArrayList<>();
            String id = ("" + originalId[cnter++]);
            for (int inrec = 0; inrec < rec.size(); inrec++) {
                tokens.add(rec.get(inrec));
            }
            recordsForTopk.put(id, tokens);
        }
        ssj.setCleanCleanER(isCleanCleanER);
        ssj.setNoOfEntities(noOfEntities);
        ssj.setDatasetDelimiter(datasetDelimiter);
        ssj.setRecords(recordsForTopk);
        ssj.topkGlobal(k);
        ArrayList<Object[]> results = ssj.getResults();
        final List<Comparison> executedComparisons = new ArrayList<>();

        for (Object[] res : results) {
            int id1 = Integer.parseInt((String) res[0]);
            int id2 = Integer.parseInt((String) res[1]);
            if (id1 == id2) {
                continue;
            }
            if (isCleanCleanER) {
                if (((id1 < datasetDelimiter) && (id2 < datasetDelimiter)) || ((id1 >= datasetDelimiter) && (id2 >= datasetDelimiter))) {
                    continue;
                }
            }
            double jaccardSim = (double) res[2];
            if (jaccardSim >= threshold) {
                final Comparison currentComp = getComparison(id1, id2);
                currentComp.setUtilityMeasure(jaccardSim);
                executedComparisons.add(currentComp);
            }
        }

        return executedComparisons;
    }
}
