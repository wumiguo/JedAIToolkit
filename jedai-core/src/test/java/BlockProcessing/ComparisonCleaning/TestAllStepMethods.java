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
package BlockProcessing.ComparisonCleaning;

import BlockBuilding.IBlockBuilding;
import Utilities.DataStructures.AbstractDuplicatePropagation;
import BlockProcessing.IBlockProcessing;
import Utilities.DataStructures.UnilateralDuplicatePropagation;
import DataModel.AbstractBlock;
import DataModel.EntityProfile;
import DataReader.EntityReader.IEntityReader;
import DataReader.EntityReader.EntitySerializationReader;
import DataReader.GroundTruthReader.GtSerializationReader;
import DataReader.GroundTruthReader.IGroundTruthReader;
import Utilities.Enumerations.BlockBuildingMethod;
import Utilities.BlocksPerformance;
import Utilities.Enumerations.BlockCleaningMethod;
import Utilities.Enumerations.ComparisonCleaningMethod;
import java.util.List;

/**
 *
 * @author G.A.P. II
 */
public class TestAllStepMethods {

    public static void main(String[] args) {
        String entitiesFilePath = "C:\\Users\\GAP2\\Downloads\\cddbProfiles";
        String groundTruthFilePath = "C:\\Users\\GAP2\\Downloads\\cddbIdDuplicates";

        IEntityReader eReader = new EntitySerializationReader(entitiesFilePath);
        List<EntityProfile> profiles = eReader.getEntityProfiles();
        System.out.println("Input Entity Profiles\t:\t" + profiles.size());

        IGroundTruthReader gtReader = new GtSerializationReader(groundTruthFilePath);
        final AbstractDuplicatePropagation duplicatePropagation = new UnilateralDuplicatePropagation(gtReader.getDuplicatePairs(eReader.getEntityProfiles()));
        System.out.println("Existing Duplicates\t:\t" + duplicatePropagation.getDuplicates().size());

        System.out.println("\n\nCurrent blocking metohd\t:\t" + BlockBuildingMethod.STANDARD_BLOCKING);
            
        double time1 = System.currentTimeMillis();
        IBlockBuilding blockBuildingMethod = BlockBuildingMethod.getDefaultConfiguration(BlockBuildingMethod.STANDARD_BLOCKING);
        List<AbstractBlock> blocks = blockBuildingMethod.getBlocks(profiles, null);

        IBlockProcessing blockCleaningMethod = BlockCleaningMethod.getDefaultConfiguration(BlockCleaningMethod.BLOCK_FILTERING);
        List<AbstractBlock> newBlocks = blockCleaningMethod.refineBlocks(blocks);
        
        double time2 = System.currentTimeMillis();
        BlocksPerformance blStats = new BlocksPerformance(blocks, duplicatePropagation);
        blStats.setStatistics();
        blStats.printStatistics(time2 - time1, blockBuildingMethod.getMethodConfiguration(), blockBuildingMethod.getMethodName());

        for (ComparisonCleaningMethod coclMethod : ComparisonCleaningMethod.values()) {
            double time3 = System.currentTimeMillis();

            IBlockProcessing comparisonCleaningMethod = ComparisonCleaningMethod.getDefaultConfiguration(coclMethod);
            List<AbstractBlock> localBlocks = comparisonCleaningMethod.refineBlocks(newBlocks);
            
            double time4 = System.currentTimeMillis();
            
            StringBuilder workflowConf = new StringBuilder(blockBuildingMethod.getMethodConfiguration());
            StringBuilder workflowName = new StringBuilder(blockBuildingMethod.getMethodName());
            workflowConf.append("\n").append(blockCleaningMethod.getMethodConfiguration());
            workflowName.append("->").append(blockCleaningMethod.getMethodName());
            workflowConf.append("\n").append(comparisonCleaningMethod.getMethodConfiguration());
            workflowName.append("->").append(comparisonCleaningMethod.getMethodName());

            blStats = new BlocksPerformance(localBlocks, duplicatePropagation);
            blStats.setStatistics();
            blStats.printStatistics(time4 - time3, workflowConf.toString(), workflowName.toString());
        }
    }
}
