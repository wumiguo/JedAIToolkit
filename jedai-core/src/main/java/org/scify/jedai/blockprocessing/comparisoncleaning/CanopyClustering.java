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

import com.esotericsoftware.minlog.Log;
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
public class CanopyClustering extends WeightedNodePruning {

    protected final double inclusiveThreshold;
    protected final double exclusiveThreshold;
    protected TIntSet excludedEntities;

    public CanopyClustering(double inThr, double outThr) {
        this(inThr, outThr, WeightingScheme.ARCS);
    }

    public CanopyClustering(double inThr, double outThr, WeightingScheme scheme) {
        super(scheme);
        nodeCentric = true;
        exclusiveThreshold = outThr;
        inclusiveThreshold = inThr;
        if (exclusiveThreshold < inclusiveThreshold) {
            Log.error(getMethodName(), "The Exclusive Threshold cannot be smaller than the Inclusive one.");
            System.exit(-1);
        }
    }

    @Override
    public String getMethodInfo() {
        return getMethodName() + ": a Meta-blocking method that retains for every entity, the comparisons "
                + "that correspond to edges in the blocking graph that are exceed the average edge weight "
                + "in the respective node neighborhood.";
    }

    @Override
    public String getMethodName() {
        return "Canopy Clustering";
    }

    @Override
    protected List<AbstractBlock> pruneEdges() {
        final TIntList entityIds = new TIntArrayList();
        for (int i = 0; i < noOfEntities; i++) {
            entityIds.add(i);
        }
        entityIds.shuffle(new Random());
        final TIntIterator iterator = entityIds.iterator();

        excludedEntities = new TIntHashSet();
        final List<AbstractBlock> newBlocks = new ArrayList<>();
        if (weightingScheme.equals(WeightingScheme.ARCS)) {
            while (iterator.hasNext()) {
                int currentId = iterator.next();
                processArcsEntity(currentId);
                verifyValidEntities(currentId, newBlocks);
            }
        } else {
            while (iterator.hasNext()) {
                int currentId = iterator.next();
                processEntity(currentId);
                verifyValidEntities(currentId, newBlocks);
            }
        }
        return newBlocks;
    }

    protected void processIndividualPair(boolean normalizeId, int entityId, int neighborId) {
        if (excludedEntities.contains(neighborId)) {
            return;
        }

        double weight = getWeight(entityId, neighborId);
        if (inclusiveThreshold < weight) {
            if (exclusiveThreshold < weight) {
                excludedEntities.add(neighborId);
            }
            
            if (normalizeId) {
                retainedNeighbors.add(neighborId - datasetLimit);
            } else {
                retainedNeighbors.add(neighborId);
            }
        }
    }

    @Override
    protected void setThreshold() {
    }

    @Override
    protected void verifyValidEntities(int entityId, List<AbstractBlock> newBlocks) {
        retainedNeighbors.clear();
        if (!cleanCleanER) { // Dirty ER
            for (TIntIterator tIterator = validEntities.iterator(); tIterator.hasNext();) {
                processIndividualPair(false, entityId, tIterator.next());
            }
            addDecomposedBlock(entityId, retainedNeighbors, newBlocks);
        } else { // Clean-Clean ER
            if (entityId < datasetLimit) { // Dataset 1
                for (TIntIterator tIterator = validEntities.iterator(); tIterator.hasNext();) {
                    processIndividualPair(true, entityId, tIterator.next());
                }
                addDecomposedBlock(entityId, retainedNeighbors, newBlocks);
            } else { // Dataset 2
                for (TIntIterator tIterator = validEntities.iterator(); tIterator.hasNext();) {
                    processIndividualPair(false, entityId, tIterator.next());
                }
                addReversedDecomposedBlock(entityId - datasetLimit, retainedNeighbors, newBlocks);
            }
        }
    }
}
