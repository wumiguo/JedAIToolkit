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
package DemoWorkflows;

import BlockBuilding.IBlockBuilding;
import BlockBuilding.StandardBlocking;
import BlockProcessing.BlockCleaning.BlockFiltering;
import BlockProcessing.ComparisonCleaning.CardinalityNodePruning;
import BlockProcessing.IBlockProcessing;
import DataModel.AbstractBlock;
import DataModel.EntityProfile;
import DataReader.EntityReader.EntitySerializationReader;
import DataReader.EntityReader.IEntityReader;
import DataReader.GroundTruthReader.GtSerializationReader;
import DataReader.GroundTruthReader.IGroundTruthReader;
import Utilities.BlocksPerformance;
import Utilities.DataStructures.AbstractDuplicatePropagation;
import Utilities.DataStructures.BilateralDuplicatePropagation;
import java.util.List;

/**
 *
 * @author gap2
 */
public class SerializedDblpAcm {

    public static void main(String[] args) {
        String mainDirectory = "C:\\Users\\gap2\\Downloads\\";

        IEntityReader serializedEntityReader = new EntitySerializationReader(mainDirectory + "dblpProfiles");
        List<EntityProfile> serializedDBLP = serializedEntityReader.getEntityProfiles();
        System.out.println("Serialized DBLP Entity Profiles\t:\t" + serializedDBLP.size());

        serializedEntityReader = new EntitySerializationReader(mainDirectory + "acmProfiles");
        List<EntityProfile> serializedACM = serializedEntityReader.getEntityProfiles();
        System.out.println("Serialized ACM Entity Profiles\t:\t" + serializedACM.size());

        IGroundTruthReader serializedGtReader = new GtSerializationReader(mainDirectory + "dblpAcmIdDuplicates");
        final AbstractDuplicatePropagation duplicatePropagation = new BilateralDuplicatePropagation(serializedGtReader.getDuplicatePairs(null));
        System.out.println("Existing Duplicates\t:\t" + duplicatePropagation.getDuplicates().size());

        StringBuilder workflowConf = new StringBuilder();
        StringBuilder workflowName = new StringBuilder();

        double time1 = System.currentTimeMillis();

        IBlockBuilding tokenBlocking = new StandardBlocking();
        List<AbstractBlock> blocks = tokenBlocking.getBlocks(serializedACM, serializedDBLP);
        workflowConf.append(tokenBlocking.getMethodConfiguration());
        workflowName.append(tokenBlocking.getMethodName());

        IBlockProcessing blockCleaning = new BlockFiltering();
        List<AbstractBlock> bcBlocks = blockCleaning.refineBlocks(blocks);
        workflowConf.append(blockCleaning.getMethodConfiguration());
        workflowName.append(blockCleaning.getMethodName());
        
        IBlockProcessing comparisonCleaning = new CardinalityNodePruning();
        List<AbstractBlock> ccBlocks = comparisonCleaning.refineBlocks(bcBlocks);
        workflowConf.append(comparisonCleaning.getMethodConfiguration());
        workflowName.append(comparisonCleaning.getMethodName());
                
        double time2 = System.currentTimeMillis();

        BlocksPerformance blStats = new BlocksPerformance(ccBlocks, duplicatePropagation);
        blStats.setStatistics();
        blStats.printStatistics(time2 - time1, workflowConf.toString(), workflowName.toString());
    }
}
