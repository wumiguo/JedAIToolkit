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
import org.scify.jedai.utilities.enumerations.WeightingScheme;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 *
 * @author gap2
 */
public class CanopyMetaClustering extends WeightedEdgePruning {

    protected int firstId;
    protected int lastId;

    private final double inclusionThreshold;
    private final double removalThreshold;

    protected final TIntList shuffledEntities;
    protected final TIntSet removedEntities;

    public CanopyMetaClustering() {
        this(0.25, 0.50, WeightingScheme.ARCS);
    }

    public CanopyMetaClustering(double it, double rt, WeightingScheme scheme) {
        super(scheme);
        nodeCentric = true;
        inclusionThreshold = it;
        removalThreshold = rt;
        removedEntities = new TIntHashSet();
        shuffledEntities = new TIntArrayList();
    }

    @Override
    public String getMethodInfo() {
        return getMethodName() + ": a Meta-blocking method that retains for every entity, the comparisons "
                + "that correspond to edges in the blocking graph that are exceed the average edge weight "
                + "in the respective node neighborhood.";
    }

    @Override
    public String getMethodName() {
        return "Canopy Meta-Clustering";
    }

    @Override
    protected List<AbstractBlock> pruneEdges() {
        final List<AbstractBlock> newBlocks = new ArrayList<>();
        shuffleEntities();
        if (weightingScheme.equals(WeightingScheme.ARCS)) {
            for (int i = 0; i < noOfEntities; i++) {
                processArcsEntity(i);
                verifyValidEntities(i, newBlocks);
            }
        } else {
            for (int i = 0; i < noOfEntities; i++) {
                processEntity(i);
                verifyValidEntities(i, newBlocks);
            }
        }
        return newBlocks;
    }

    protected void setLimits() {
        firstId = 0;
        lastId = noOfEntities;
    }

    @Override
    protected void setThreshold() {
        if (weightingScheme.equals(WeightingScheme.ARCS)) {
            for (int i = 0; i < noOfEntities; i++) {
                processArcsEntity(i);
                setThreshold(i);
            }
        } else {
            for (int i = 0; i < noOfEntities; i++) {
                processEntity(i);
                setThreshold(i);
            }
        }
    }

    protected void setThreshold(int entityId) {
        threshold = 0;
        for (TIntIterator iterator = validEntities.iterator(); iterator.hasNext();) {
            threshold += getWeight(entityId, iterator.next());
        }
        threshold /= validEntities.size();
    }

    protected void shuffleEntities() {
        if (cleanCleanER) {
            for (int i = 0; i < datasetLimit; i++) {
                shuffledEntities.add(i);
            }
        } else {
            for (int i = 0; i < datasetLimit; i++) {
                shuffledEntities.add(i);
            }
        }
        shuffledEntities.shuffle(new Random());
    }

    @Override
    protected void verifyValidEntities(int entityId, List<AbstractBlock> newBlocks) {
        retainedNeighbors.clear();
        if (!cleanCleanER) {
            for (TIntIterator tIterator = validEntities.iterator(); tIterator.hasNext();) {
                int neighborId = tIterator.next();
//                if (isValidComparison(entityId, neighborId)) {
//                    retainedNeighbors.add(neighborId);
//                }
            }
            addDecomposedBlock(entityId, retainedNeighbors, newBlocks);
        } else {
            if (entityId < datasetLimit) {
                for (TIntIterator tIterator = validEntities.iterator(); tIterator.hasNext();) {
                    int neighborId = tIterator.next();
//                    if (isValidComparison(entityId, neighborId)) {
//                        retainedNeighbors.add(neighborId - datasetLimit);
//                    }
                }
                addDecomposedBlock(entityId, retainedNeighbors, newBlocks);
            } else {
                for (TIntIterator tIterator = validEntities.iterator(); tIterator.hasNext();) {
                    int neighborId = tIterator.next();
//                    if (isValidComparison(entityId, neighborId)) {
//                        retainedNeighbors.add(neighborId);
//                    }
                }
                addReversedDecomposedBlock(entityId - datasetLimit, retainedNeighbors, newBlocks);
            }
        }
    }
}
