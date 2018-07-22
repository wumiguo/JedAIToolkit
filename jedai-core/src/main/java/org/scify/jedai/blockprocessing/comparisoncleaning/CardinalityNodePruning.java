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
package org.scify.jedai.blockprocessing.comparisoncleaning;

import org.scify.jedai.datamodel.AbstractBlock;
import org.scify.jedai.datamodel.Comparison;
import org.scify.jedai.utilities.comparators.IncComparisonWeightComparator;
import org.scify.jedai.utilities.enumerations.WeightingScheme;

import com.esotericsoftware.minlog.Log;

import gnu.trove.iterator.TIntIterator;
import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;

import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;

/**
 *
 * @author gap2
 */
public class CardinalityNodePruning extends CardinalityEdgePruning {

    protected int firstId;
    protected int lastId;
    protected TIntList[] nearestEntities;

    public CardinalityNodePruning() {
        this(WeightingScheme.ARCS);
    }

    public CardinalityNodePruning(WeightingScheme scheme) {
        super(scheme);
        threshold = -1;
        nodeCentric = true;
    }

    @Override
    public String getMethodInfo() {
        return getMethodName() + ": a Meta-blocking method that retains for every entity, "
                + "the comparisons that correspond to its top-k weighted edges in the blocking graph.";
    }

    @Override
    public String getMethodName() {
        return "Cardinality Node Pruning";
    }

    protected boolean isValidComparison(int entityId, int neighborId) {
        if (nearestEntities[neighborId] == null) {
            return true;
        }

        if (nearestEntities[neighborId].contains(entityId)) {
            return entityId < neighborId;
        }

        return true;
    }

    @Override
    protected List<AbstractBlock> pruneEdges() {
        nearestEntities = new TIntList[noOfEntities];
        topKEdges = new PriorityQueue<>((int) (2 * threshold), new IncComparisonWeightComparator());
        if (weightingScheme.equals(WeightingScheme.ARCS)) {
            for (int i = 0; i < noOfEntities; i++) {
                processArcsEntity(i);
                verifyValidEntities(i);
            }
        } else {
            for (int i = 0; i < noOfEntities; i++) {
                processEntity(i);
                verifyValidEntities(i);
            }
        }

        final List<AbstractBlock> newBlocks = new ArrayList<>();
        retainValidComparisons(newBlocks);
        return newBlocks;
    }

    protected void retainValidComparisons(List<AbstractBlock> newBlocks) {
        final List<Comparison> retainedComparisons = new ArrayList<>();
        for (int i = 0; i < noOfEntities; i++) {
            if (nearestEntities[i] != null) {
                retainedComparisons.clear();
                TIntIterator intIterator = nearestEntities[i].iterator();
                while (intIterator.hasNext()) {
                    int neighborId = intIterator.next();
                    if (isValidComparison(i, neighborId)) {
                        retainedComparisons.add(getComparison(i, neighborId));
                    }
                }
                addDecomposedBlock(retainedComparisons, newBlocks);
            }
        }
    }

    protected void setLimits() {
        firstId = 0;
        lastId = noOfEntities;
    }

    @Override
    protected void setThreshold() {
        threshold = Math.max(1, blockAssingments / noOfEntities);
        Log.info(getMethodName() + " Threshold \t:\t" + threshold);
    }

    @Override
    protected void verifyValidEntities(int entityId) {
        if (validEntities.isEmpty()) {
            return;
        }

        topKEdges.clear();
        minimumWeight = Double.MIN_VALUE;
        for (TIntIterator iterator = validEntities.iterator(); iterator.hasNext();) {
            int neighborId = iterator.next();
            double weight = getWeight(entityId, neighborId);
            if (!(weight < minimumWeight)) {
                final Comparison comparison = new Comparison(cleanCleanER, -1, neighborId);
                comparison.setUtilityMeasure(weight);
                topKEdges.add(comparison);
                if (threshold < topKEdges.size()) {
                    Comparison lastComparison = topKEdges.poll();
                    minimumWeight = lastComparison.getUtilityMeasure();
                }
            }
        }

        nearestEntities[entityId] = new TIntArrayList();
        for (Comparison comparison : topKEdges) {
            nearestEntities[entityId].add(comparison.getEntityId2());
        }
    }
}
