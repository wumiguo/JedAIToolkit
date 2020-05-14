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

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.scify.jedai.datamodel.Comparison;
import org.scify.jedai.datamodel.EntityProfile;
import org.scify.jedai.datamodel.SimilarityPairs;
import org.scify.jedai.similarityjoins.fuzzysets.FuzzySetSimJoin;

import java.util.*;

/**
 *
 * @author mthanos
 */
public class FuzzySetJoin extends AbstractTokenBasedJoin {

    private int[] originalId;
    private final List<String> attributeValues;
    private List<String>[] records1;
    private final Map<String, List<Set<String>>> collection1;
    private final Map<String, List<Set<String>>> collection2;

    public FuzzySetJoin(float thr) {
        super(thr);
        collection1 = new LinkedHashMap<>();
        collection2 = new LinkedHashMap<>();
        attributeValues = new ArrayList<>();
    }

    @Override
    public SimilarityPairs applyJoin(String attributeName1, String attributeName2, List<EntityProfile> dataset1, List<EntityProfile> dataset2) {
        init();

        final List<Comparison> comparisons = performJoin();
        return getSimilarityPairs(comparisons);
    }

    @Override
    public String getMethodInfo() {
        return getMethodName() + ": it implements silkmoth similarity join algorithm";
    }

    @Override
    public String getMethodName() {
        return "FuzzySetJoin";
    }

    private void init() {
        int counter = 0;
        final List<Pair<String, Integer>> idIdentifier = new ArrayList<>();
        for (EntityProfile profile : profilesD1) {
            final String nextValue = getAttributeValue(attributeNameD1, profile);
            idIdentifier.add(new ImmutablePair<>(nextValue, counter++));
        }

        if (isCleanCleanER) {
            for (EntityProfile profile : profilesD2) {
                final String nextValue = getAttributeValue(attributeNameD2, profile);
                idIdentifier.add(new ImmutablePair<>(nextValue, counter++));
            }
        }

        //idIdentifier.sort((s1, s2) -> s1.getKey().split(" ").length - s2.getKey().split(" ").length);
        attributeValues.clear();
        originalId = new int[noOfEntities];
        records1 = new List[noOfEntities];
//        records2 = new List[noOfEntities - datasetDelimiter];
        for (int i = 0; i < noOfEntities; i++) {
            final Pair<String, Integer> currentPair = idIdentifier.get(i);
            attributeValues.add(currentPair.getKey());
            originalId[i] = currentPair.getValue();
            records1[i] = new ArrayList<>();
            final String s = attributeValues.get(i).trim();
            if (s.length() < 1) {
                continue;
            }

            String[] split = s.split(" ");
            records1[i].addAll(Arrays.asList(split));
            Set<String> asSet = new HashSet<>(records1[i]);
            List<Set<String>> asList = new ArrayList<>();
            asList.add(asSet);
            if (isCleanCleanER) {
                if (i < datasetDelimiter) {
                    collection1.put(originalId[i] + "", asList);
                } else {
                    collection2.put(originalId[i] + "", asList);
                }
            } else {
                collection1.put(originalId[i] + "", asList);
                collection2.put(originalId[i] + "", asList);
            }
        }
    }

    private List<Comparison> performJoin() {
        FuzzySetSimJoin fssj = new FuzzySetSimJoin();
//		List<int[]> matchingPairs = fssj.join(input1, input2, simThreshold);
        HashMap<String, Float> matchingPairs = fssj.join(collection1, collection2, threshold);
        LinkedHashMap<String, ArrayList<String>> recordsForTopk = new LinkedHashMap<>();
        int cnter = 0;
        final List<Comparison> executedComparisons = new ArrayList<>();

        for (String allres : matchingPairs.keySet()) {
            String[] res = allres.split("_");
            int id1 = Integer.parseInt(res[0]);
            int id2 = Integer.parseInt(res[1]);
            float jaccardSim = matchingPairs.get(allres);
            //System.out.println(id1+" "+id2+" "+jaccardSim+" "+isCleanCleanER+" "+datasetDelimiter);

            if (isCleanCleanER) {
                id2 += datasetDelimiter;
            } else if (id1 == id2) {
                continue;
            }
            if (jaccardSim >= threshold) {
                final Comparison currentComp = getComparison(originalId[id1], originalId[id2]);
                currentComp.setUtilityMeasure(jaccardSim);
                executedComparisons.add(currentComp);
            }
        }

        return executedComparisons;
    }
}
