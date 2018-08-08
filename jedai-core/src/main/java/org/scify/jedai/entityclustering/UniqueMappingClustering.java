/*
* Copyright [2016-2018] [George Papadakis (gpapadis@yahoo.gr)]
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
package org.scify.jedai.entityclustering;

import org.scify.jedai.datamodel.Comparison;
import org.scify.jedai.datamodel.EquivalenceCluster;
import org.scify.jedai.datamodel.SimilarityEdge;
import org.scify.jedai.datamodel.SimilarityPairs;
import org.scify.jedai.utilities.comparators.DecSimilarityEdgeComparator;

import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;

import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.Queue;

/**
 *
 * @author vefthym
 */
public class UniqueMappingClustering extends AbstractEntityClustering {

    private final TIntSet matchedIds; //the ids of entities that have been already matched

    public UniqueMappingClustering() {
        this(0.5);
    }

    public UniqueMappingClustering(double simTh) {
        super(simTh);

        matchedIds = new TIntHashSet();
    }

    @Override
    public EquivalenceCluster[] getDuplicates(SimilarityPairs simPairs) {
        if (simPairs.getNoOfComparisons() == 0) {
            return new EquivalenceCluster[0];
        }

        initializeData(simPairs);
        if (!isCleanCleanER) {
            return null; //the method is only applicable to Clean-Clean ER
        }

        final Queue<SimilarityEdge> SEqueue = new PriorityQueue<>(simPairs.getNoOfComparisons(), new DecSimilarityEdgeComparator());

        final Iterator<Comparison> iterator = simPairs.getPairIterator();
        while (iterator.hasNext()) { // add a similarity edge to the queue, for every pair of entities with a weight higher than the threshold
            Comparison comparison = iterator.next();
            if (threshold < comparison.getUtilityMeasure()) {
                SEqueue.add(new SimilarityEdge(comparison.getEntityId1(), comparison.getEntityId2() + datasetLimit, comparison.getUtilityMeasure()));
            }
        }
        simPairs = null;

        while (!SEqueue.isEmpty()) {
            final SimilarityEdge se = SEqueue.remove();
            int e1 = se.getModel1Pos();
            int e2 = se.getModel2Pos();

            //skip already matched entities (unique mapping contraint for clean-clean ER)
            if (matchedIds.contains(e1) || matchedIds.contains(e2)) {
                continue;
            }

            similarityGraph.addEdge(e1, e2);
            matchedIds.add(e1);
            matchedIds.add(e2);
        }

        return getConnectedComponents();
    }

    @Override
    public String getMethodInfo() {
        return getMethodName() + ": it create a cluster for each pair of entities, none of which has been matched previously. ";
    }

    @Override
    public String getMethodName() {
        return "Unique Mapping Clustering";
    }

    @Override
    public void setNextRandomConfiguration() {
        matchedIds.clear();
        super.setNextRandomConfiguration();
    }

    @Override
    public void setNumberedGridConfiguration(int iterationNumber) {
        matchedIds.clear();
        super.setNumberedGridConfiguration(iterationNumber);
    }
    
    @Override
    public void setNumberedRandomConfiguration(int iterationNumber) {
        matchedIds.clear();
        super.setNumberedRandomConfiguration(iterationNumber);
    }
}
