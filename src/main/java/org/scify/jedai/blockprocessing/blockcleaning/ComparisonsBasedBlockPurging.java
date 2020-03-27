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
package org.scify.jedai.blockprocessing.blockcleaning;

import org.scify.jedai.datamodel.AbstractBlock;
import org.scify.jedai.utilities.comparators.IncBlockCardinalityComparator;

import com.esotericsoftware.minlog.Log;

import gnu.trove.set.TDoubleSet;
import gnu.trove.set.hash.TDoubleHashSet;

import java.util.Collections;
import java.util.List;

import org.apache.jena.atlas.json.JsonArray;
import org.apache.jena.atlas.json.JsonObject;
import org.scify.jedai.configuration.gridsearch.DblGridSearchConfiguration;
import org.scify.jedai.configuration.randomsearch.DblRandomSearchConfiguration;

/**
 *
 * @author gap2
 */
public class ComparisonsBasedBlockPurging extends AbstractBlockPurging {

    private double smoothingFactor;
    private double maxComparisonsPerBlock;

    protected final DblGridSearchConfiguration gridSFactor;
    protected final DblRandomSearchConfiguration randomSFactor;

    public ComparisonsBasedBlockPurging(boolean isCleanCleanER) {
        this(isCleanCleanER ? 1.00 : 1.025);
    }

    public ComparisonsBasedBlockPurging(double sf) {
        smoothingFactor = sf;

        gridSFactor = new DblGridSearchConfiguration(2.0, 1.0, 0.02);
        randomSFactor = new DblRandomSearchConfiguration(2.0, 1.0);
    }

    @Override
    public String getMethodConfiguration() {
        return getParameterName(0) + "=" + smoothingFactor;
    }

    @Override
    public String getMethodInfo() {
        return getMethodName() + ": it discards the blocks exceeding a certain number of comparisons.";
    }

    @Override
    public String getMethodName() {
        return "Comparison-based Block Purging";
    }

    @Override
    public String getMethodParameters() {
        return getMethodName() + " involves a single parameter:\n"
                + "1)" + getParameterDescription(0) + ".\n";
    }

    @Override
    public int getNumberOfGridConfigurations() {
        return gridSFactor.getNumberOfConfigurations();
    }

    @Override
    public JsonArray getParameterConfiguration() {
        final JsonObject obj = new JsonObject();
        obj.put("class", "java.lang.Double");
        obj.put("name", getParameterName(0));
        obj.put("defaultValue", "1.0");
        obj.put("minValue", "1.0");
        obj.put("maxValue", "2.0");
        obj.put("stepValue", "0.01");
        obj.put("description", getParameterDescription(0));

        final JsonArray array = new JsonArray();
        array.add(obj);

        return array;
    }

    @Override
    public String getParameterDescription(int parameterId) {
        switch (parameterId) {
            case 0:
                return "The " + getParameterName(0) + " determines the termination criterion for automatically estimating the "
                        + "maximum number of comparisons per block.";
            default:
                return "invalid parameter id";
        }
    }

    @Override
    public String getParameterName(int parameterId) {
        switch (parameterId) {
            case 0:
                return "Smoothing Factor";
            default:
                return "invalid parameter id";
        }
    }

    @Override
    protected boolean satisfiesThreshold(AbstractBlock block) {
        return block.getNoOfComparisons() <= maxComparisonsPerBlock;
    }

    @Override
    public void setNextRandomConfiguration() {
        smoothingFactor = (Double) randomSFactor.getNextRandomValue();
    }

    @Override
    public void setNumberedGridConfiguration(int iterationNumber) {
        smoothingFactor = (Double) gridSFactor.getNumberedValue(iterationNumber);
    }

    @Override
    public void setNumberedRandomConfiguration(int iterationNumber) {
        smoothingFactor = (Double) randomSFactor.getNumberedRandom(iterationNumber);
    }

    @Override
    protected void setThreshold(List<AbstractBlock> blocks) {
        Collections.sort(blocks, new IncBlockCardinalityComparator());
        final TDoubleSet distinctComparisonsLevel = new TDoubleHashSet();
        blocks.forEach((block) -> {
            distinctComparisonsLevel.add(block.getNoOfComparisons());
        });

        int index = -1;
        double[] blockAssignments = new double[distinctComparisonsLevel.size()];
        double[] comparisonsLevel = new double[distinctComparisonsLevel.size()];
        double[] totalComparisonsPerLevel = new double[distinctComparisonsLevel.size()];
        for (AbstractBlock block : blocks) {
            if (index == -1) {
                index++;
                comparisonsLevel[index] = block.getNoOfComparisons();
                blockAssignments[index] = 0;
                totalComparisonsPerLevel[index] = 0;
            } else if (block.getNoOfComparisons() != comparisonsLevel[index]) {
                index++;
                comparisonsLevel[index] = block.getNoOfComparisons();
                blockAssignments[index] = blockAssignments[index - 1];
                totalComparisonsPerLevel[index] = totalComparisonsPerLevel[index - 1];
            }

            blockAssignments[index] += block.getTotalBlockAssignments();
            totalComparisonsPerLevel[index] += block.getNoOfComparisons();
        }

        double currentBC = 0;
        double currentCC = 0;
        double currentSize = 0;
        double previousBC = 0;
        double previousCC = 0;
        double previousSize = 0;
        int arraySize = blockAssignments.length;
        for (int i = arraySize - 1; 0 <= i; i--) {
            previousSize = currentSize;
            previousBC = currentBC;
            previousCC = currentCC;

            currentSize = comparisonsLevel[i];
            currentBC = blockAssignments[i];
            currentCC = totalComparisonsPerLevel[i];

            if (currentBC * previousCC < smoothingFactor * currentCC * previousBC) {
                break;
            }
        }

        maxComparisonsPerBlock = previousSize;
        Log.info("Maximum comparisons per block\t:\t" + maxComparisonsPerBlock);
    }
}
