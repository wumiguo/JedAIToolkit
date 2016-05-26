/*
* Copyright [2016] [George Papadakis (gpapadis@yahoo.gr)]
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

package BlockProcessing.BlockRefinement;

import BlockBuilding.AbstractBlockBuilding;
import DataModel.AbstractBlock;
import Utilities.BlockCardinalityComparator;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author gap2
 */
public class ComparisonsBasedBlockPurging extends AbstractBlockPurging {

    private static final Logger LOGGER = Logger.getLogger(AbstractBlockBuilding.class.getName());
    
    private double SMOOTHING_FACTOR;
    private double maxComparisonsPerBlock;
    
    public ComparisonsBasedBlockPurging() {
        this(1.025);//default value of smooting factor
    }

    public ComparisonsBasedBlockPurging(double smoothingFactor) {
        SMOOTHING_FACTOR = smoothingFactor;
        LOGGER.log(Level.INFO, "Smoothing factor\t:\t{0}", SMOOTHING_FACTOR);
    }

    @Override
    public String getMethodInfo() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public String getMethodParameters() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    protected boolean satisfiesThreshold(AbstractBlock block) {
        return block.getNoOfComparisons() <= maxComparisonsPerBlock;
    }

    @Override
    protected void setThreshold(List<AbstractBlock> blocks) {
        Collections.sort(blocks, new BlockCardinalityComparator());
        final Set<Double> distinctComparisonsLevel = new HashSet<Double>();
        for (AbstractBlock block : blocks) {
            distinctComparisonsLevel.add(block.getNoOfComparisons());
        }

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

            if (currentBC * previousCC < SMOOTHING_FACTOR * currentCC * previousBC) {
                break;
            }
        }

        maxComparisonsPerBlock = previousSize;
        LOGGER.log(Level.INFO, "Maximum comparisons per block\t:\t{0}", maxComparisonsPerBlock);
    }
}
