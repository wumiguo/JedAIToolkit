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
import org.apache.jena.atlas.json.JsonArray;
import org.scify.jedai.datamodel.Comparison;
import org.scify.jedai.utilities.comparators.DecComparisonWeightComparator;
import org.scify.jedai.utilities.enumerations.ProgressiveWeightingScheme;

/**
 *
 * @author gap2
 */
public class LocalProgressiveSortedNeighborhood extends AbstractSimilarityBasedPrioritization {

    protected int currentWindow;

    public LocalProgressiveSortedNeighborhood(int budget, ProgressiveWeightingScheme pwScheme) {
        super(budget, pwScheme);

        currentWindow = 0;
    }

    protected void getWindowComparisons() {
        currentWindow++;
        int limit = isCleanCleanER ? datasetLimit : noOfEntities;
        final List<Comparison> windowComparisons = new ArrayList<>();
        for (int entityId = 0; entityId < limit; entityId++) {
            distinctNeighbors.clear();

            for (int position : positionIndex.getEntityPositions(entityId)) {
                if (position + currentWindow < sortedEntityIds.length) {
                    if (isCleanCleanER && datasetLimit <= sortedEntityIds[position + currentWindow]
                            || !isCleanCleanER && sortedEntityIds[position + currentWindow] < entityId) {
                        updateCounters(entityId, sortedEntityIds[position + currentWindow]);
                    }
                }

                if (0 <= position - currentWindow) {
                    if (isCleanCleanER && datasetLimit <= sortedEntityIds[position - currentWindow]
                            || !isCleanCleanER && sortedEntityIds[position - currentWindow] < entityId) {
                        updateCounters(entityId, sortedEntityIds[position - currentWindow]);
                    }
                }
            }

            for (TIntIterator iterator = distinctNeighbors.iterator(); iterator.hasNext();) {
                int neighborId = iterator.next();
                flags[neighborId] = -1;

                int entityId2 = isCleanCleanER ? neighborId - datasetLimit : neighborId;
                final Comparison c = new Comparison(isCleanCleanER, entityId, entityId2);
                c.setUtilityMeasure(getWeight(entityId, neighborId));
                windowComparisons.add(c);
            }
        }

        Collections.sort(windowComparisons, new DecComparisonWeightComparator());
        compIterator = windowComparisons.iterator();
    }

    protected double getWeight(int entityId1, int entityId2) {
        switch (pwScheme) {
            case ACF:
                return counters[entityId2];
            case NCF:
                double denominator = positionIndex.getEntityPositions(entityId1).length + positionIndex.getEntityPositions(entityId2).length - counters[entityId2];
                return counters[entityId2] / denominator;
        }
        return -1;
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

    @Override
    public boolean hasNext() {
        return emittedComparisons < comparisonsBudget && currentWindow < sortedEntityIds.length;
    }

    @Override
    public Comparison next() {
        if (compIterator == null || !compIterator.hasNext()) {
            getWindowComparisons();
        }

        emittedComparisons++;
        return compIterator.next();
    }

    private void updateCounters(int entityId, int neighborId) {
        if (flags[neighborId] != entityId) {
            counters[neighborId] = 0;
            flags[neighborId] = entityId;
        }

        counters[neighborId]++;
        distinctNeighbors.add(neighborId);
    }
}
