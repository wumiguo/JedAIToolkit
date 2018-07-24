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

import java.util.List;
import java.util.Random;

/**
 *
 * @author gap2
 */
public class CanopyClustering extends CardinalityNodePruning {

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
        nearestEntities = new TIntSet[noOfEntities];
        if (weightingScheme.equals(WeightingScheme.ARCS)) {
            while (iterator.hasNext()) {
                int currentId = iterator.next();
                processArcsEntity(currentId);
                verifyValidEntities(currentId);
            }
        } else {
            while (iterator.hasNext()) {
                int currentId = iterator.next();
                processEntity(currentId);
                verifyValidEntities(currentId);
            }
        }
        
        return retainValidComparisons();
    }
    
    @Override
    protected void setThreshold() {
    }

    @Override
    protected void verifyValidEntities(int entityId) {
        nearestEntities[entityId] = new TIntHashSet();
        for (TIntIterator tIterator = validEntities.iterator(); tIterator.hasNext();) {
            int neighborId = tIterator.next();
            if (excludedEntities.contains(neighborId)) {
                System.out.println("Excluded!!!");
                continue;
            }

            double weight = getWeight(entityId, neighborId);
            if (inclusiveThreshold < weight) {
                if (exclusiveThreshold < weight) {
                    System.out.println(weight);
                    excludedEntities.add(neighborId);
                }
                nearestEntities[entityId].add(neighborId);
            }
        }
    }
}
