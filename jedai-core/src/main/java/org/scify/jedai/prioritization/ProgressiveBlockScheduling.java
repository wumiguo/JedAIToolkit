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

import com.esotericsoftware.minlog.Log;
import org.scify.jedai.prioritization.utilities.BlockcentricEntityIndex;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.jena.atlas.json.JsonArray;
import org.scify.jedai.datamodel.AbstractBlock;
import org.scify.jedai.datamodel.Comparison;
import org.scify.jedai.datamodel.ComparisonIterator;
import org.scify.jedai.datamodel.DecomposedBlock;
import org.scify.jedai.utilities.comparators.DecComparisonWeightComparator;
import org.scify.jedai.utilities.comparators.IncBlockCardinalityComparator;
import org.scify.jedai.utilities.enumerations.WeightingScheme;

/**
 *
 * @author gap2
 */
public class ProgressiveBlockScheduling extends AbstractHashBasedPrioritization {

    protected boolean isDecomposedBlock;

    private int blockCounter;
    private int comparisonCounter;

    private AbstractBlock[] blocksArray;
    private BlockcentricEntityIndex entityIndex;

    public ProgressiveBlockScheduling(int budget, WeightingScheme wScheme) {
        super(budget, wScheme);
    }

    @Override
    public void developBlockBasedSchedule(List<AbstractBlock> blocks) {
        if (blocks == null || blocks.isEmpty()) {
            Log.error("No blocks were given as input!");
            System.exit(-1);
        }
        
        Collections.sort(blocks, new IncBlockCardinalityComparator());
        blocksArray = blocks.toArray(new AbstractBlock[blocks.size()]);
        isDecomposedBlock = blocksArray[0] instanceof DecomposedBlock;
        if (!isDecomposedBlock) {
            entityIndex = new BlockcentricEntityIndex(blocks, wScheme);
        }

        blockCounter = 0;
        comparisonCounter = 0;
        filterComparisons();
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

    private void filterComparisons() {
        final List<Comparison> topComparisons = new ArrayList<>();
        final ComparisonIterator iterator = blocksArray[blockCounter].getComparisonIterator();
        while (iterator.hasNext()) {
            final Comparison comparison = iterator.next();
            if (!isDecomposedBlock) {
                double weight = entityIndex.getWeight(blockCounter, comparison);
                if (weight < 0) {
                    continue;
                }
                comparison.setUtilityMeasure(weight);
            }
            topComparisons.add(comparison);
        }
        Collections.sort(topComparisons, new DecComparisonWeightComparator());
        compIterator = topComparisons.iterator();
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
        if (comparisonCounter < comparisonsBudget) {
            while (!compIterator.hasNext() && blockCounter < blocksArray.length - 1) {
                blockCounter++;
                filterComparisons();
            }
            return compIterator.hasNext();
        }
        return false;
    }

    @Override
    public Comparison next() {
        comparisonCounter++;
        return compIterator.next();
    }
}
