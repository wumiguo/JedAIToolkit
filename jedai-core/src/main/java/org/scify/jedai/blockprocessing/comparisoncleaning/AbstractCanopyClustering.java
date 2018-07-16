/*
 *    This program is free software; you can redistribute it and/or modify
 *    it under the terms of the GNU General Public License as published by
 *    the Free Software Foundation; either version 2 of the License, or
 *    (at your option) any later version.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU General Public License for more details.
 *
 *    Copyright (C) 2015 George Antony Papadakis (gpapadis@yahoo.gr)
 */
package org.scify.jedai.blockprocessing.comparisoncleaning;

import gnu.trove.iterator.TIntIterator;
import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import org.scify.jedai.utilities.enumerations.WeightingScheme;

/**
 *
 * @author gap2
 */
public abstract class AbstractCanopyClustering extends AbstractMetablocking {

    protected final TIntSet removedEntities;
    
    public AbstractCanopyClustering(WeightingScheme wScheme) {
        super(wScheme);
        
        removedEntities = new TIntHashSet();
    }

    protected void getBilateralBlocks() {
        final TIntList entityIds1 = new TIntArrayList();
        for (int i = 0; i < datasetLimit; i++) {
            entityIds1.add(i);
        }
        entityIds1.shuffle(new Random());

        removedEntities.clear();
        int d2Entities = noOfEntities - datasetLimit;   
        final TIntIterator iterator = entityIds1.iterator();
        while (iterator.hasNext() && removedEntities.size() < d2Entities) {
            // Get current element:
            int currentId = iterator.next();

            // Start a new cluster:
            retainedNeighbors.clear();
//            setNormalizedNeighborEntities(currentId);
            final TIntIterator innerIterator = validEntities.iterator();
            while (innerIterator.hasNext()) {
                int neighborId = innerIterator.next();
                double jaccardSim = counters[neighborId] / (entityIndex.getNoOfEntityBlocks(currentId, 0) + entityIndex.getNoOfEntityBlocks(neighborId, 1) - counters[neighborId]);

                // Inclusion threshold:
//                if (t1 <= jaccardSim) {
//                    retainedNeighbors.add(neighborId);
//                }

                // Removal threshold:
//                if (t2 <= jaccardSim) {
//                    removedEntities.add(neighborId);
//                }
            }
            
//            addBilateralBlock(currentId);
        }
    }

    protected void getUnilateralBlocks() {
        final List<Integer> entityIds = new ArrayList<>();
        for (int i = 0; i < noOfEntities; i++) {
            entityIds.add(i);
        }
        Collections.shuffle(entityIds);

        removedEntities.clear();
        final Iterator iter = entityIds.iterator();
        while (removedEntities.size() < entityIds.size()) {
            // Get next element:
            int currentId = (Integer) iter.next();
            
            // Remove first element:
            removedEntities.add(currentId);
            
            // Start a new cluster:
            retainedNeighbors.clear();
//            setUnilateralValidEntities(currentId);
//            for (int neighborId : validEntities) {
//                double jaccardSim = counters[neighborId] / (entityIndex.getNoOfEntityBlocks(currentId, 0) + entityIndex.getNoOfEntityBlocks(neighborId, 0) - counters[neighborId]);
//                
//                // Inclusion threshold:
//                if (t1 <= jaccardSim) {
//                    retainedNeighbors.add(neighborId);
//                }
//
//                // Removal threshold:
//                if (t2 <= jaccardSim) {
//                    removedEntities.add(neighborId);
//                }
//            }
//            
//            addUnilateralBlock(currentId);
        }
    }
}
