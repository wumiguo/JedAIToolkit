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

package org.scify.jedai.prioritization;

import gnu.trove.iterator.TIntIterator;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
import org.apache.jena.atlas.json.JsonArray;
import org.scify.jedai.datamodel.Comparison;
import org.scify.jedai.datamodel.EntityProfile;
import org.scify.jedai.utilities.comparators.DecComparisonWeightComparator;
import org.scify.jedai.utilities.comparators.IncComparisonWeightComparator;
import org.scify.jedai.utilities.enumerations.ProgressiveWeightingScheme;

/**
 * @author gap2
 * @author giovanni
 */
public class GlobalProgressiveSortedNeighborhood extends AbstractSimilarityBasedPrioritization {

    protected int maxWindow;

    protected final Queue<Comparison> topComps;

    public GlobalProgressiveSortedNeighborhood(int budget, ProgressiveWeightingScheme pwScheme) {
        super(budget, pwScheme);

        topComps = new PriorityQueue<>((int) (2 * comparisonsBudget), new IncComparisonWeightComparator());
    }

    @Override
    public void developEntityBasedSchedule(List<EntityProfile> profilesD1, List<EntityProfile> profilesD2) {
        super.developEntityBasedSchedule(profilesD1, profilesD2);
        maxWindow = noOfEntities <= 100 ? 2 : (int) Math.round(Math.pow(2, Math.log10(noOfEntities) + 1)) + 1;
        getComparisons();
    }

    private void getComparisons() { //TODO: comparison propagation
        int limit = isCleanCleanER ? datasetLimit : noOfEntities;
        for (int entityId = 0; entityId < limit; entityId++) {
            final int[] entityPositions = positionIndex.getEntityPositions(entityId);
            for (int currentWindow = 1; currentWindow < maxWindow; currentWindow++) {
                for (int position : entityPositions) {
                    if (position + currentWindow < sortedEntityIds.length) {
                        if (isCleanCleanER && datasetLimit <= sortedEntityIds[position + currentWindow]
                                || !isCleanCleanER && sortedEntityIds[position + currentWindow] < entityId) {
                            updateLocalWeight(currentWindow, entityId, sortedEntityIds[position + currentWindow]);
                        }
                    }

                    if (0 <= position - currentWindow) {
                        if (isCleanCleanER && datasetLimit <= sortedEntityIds[position - currentWindow]
                                || !isCleanCleanER && sortedEntityIds[position - currentWindow] < entityId) {
                            updateLocalWeight(currentWindow, entityId, sortedEntityIds[position - currentWindow]);
                        }
                    }
                }
            }

            double minimumWeight = -1;
            for (TIntIterator iterator = distinctNeighbors.iterator(); iterator.hasNext(); ) {
                int neighborId = iterator.next();
                flags[neighborId] = -1;

                double weight = getWeight(entityId, neighborId);
                if (weight < minimumWeight) {
                    continue;
                }

                int entityId2 = isCleanCleanER ? neighborId - datasetLimit : neighborId;
                final Comparison c = new Comparison(isCleanCleanER, entityId, entityId2);
                c.setUtilityMeasure(weight);
                topComps.add(c);
                if (comparisonsBudget < topComps.size()) {
                    Comparison lastComparison = topComps.poll();
                    minimumWeight = lastComparison.getUtilityMeasure();
                }
            }
        }

        final List<Comparison> topComparisons = new ArrayList<>(topComps);
        Collections.sort(topComparisons, new DecComparisonWeightComparator());
        compIterator = topComparisons.iterator();
    }

    protected double getWeight(int entityId1, int entityId2) {
        switch (pwScheme) {
            case NCF:
                double denominator = positionIndex.getEntityPositions(entityId1).length + positionIndex.getEntityPositions(entityId2).length - counters[entityId2];
                return counters[entityId2] / denominator;
            default: //ACF, ID
                return counters[entityId2];
        }
    }

    @Override
    public boolean hasNext() {
        return compIterator.hasNext();
    }

    @Override
    public Comparison next() {
        return compIterator.next();
    }

    protected void updateLocalWeight(int currentWindow, int entityId, int neighborId) {
        if (flags[neighborId] != entityId) {
            counters[neighborId] = 0;
            flags[neighborId] = entityId;
        }

        switch (pwScheme) {
            case ID:
                counters[neighborId] += 1.0 / currentWindow;
                break;
            default: // ACF, NCF
                counters[neighborId]++;
                break;
        }
        distinctNeighbors.add(neighborId);
    }

    @Override
    public int getNumberOfGridConfigurations() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void setNextRandomConfiguration() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void setNumberedGridConfiguration(int iterationNumber) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void setNumberedRandomConfiguration(int iterationNumber) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public String getMethodConfiguration() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public String getMethodInfo() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public String getMethodName() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public String getMethodParameters() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public JsonArray getParameterConfiguration() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public String getParameterDescription(int parameterId) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public String getParameterName(int parameterId) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
}
