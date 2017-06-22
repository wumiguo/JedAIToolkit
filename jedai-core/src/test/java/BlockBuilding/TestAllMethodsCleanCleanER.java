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
package BlockBuilding;

import Utilities.DataStructures.AbstractDuplicatePropagation;
import DataModel.AbstractBlock;
import DataModel.EntityProfile;
import DataReader.EntityReader.IEntityReader;
import DataReader.EntityReader.EntitySerializationReader;
import DataReader.GroundTruthReader.GtSerializationReader;
import DataReader.GroundTruthReader.IGroundTruthReader;
import Utilities.Enumerations.BlockBuildingMethod;
import Utilities.BlocksPerformance;
import Utilities.DataStructures.BilateralDuplicatePropagation;
import java.util.List;

/**
 *
 * @author G.A.P. II
 */
public class TestAllMethodsCleanCleanER {

    public static void main(String[] args) {
        String[] entitiesFilePath = {"E:\\Data\\CCERdata\\abt-buy\\abtProfiles",
            "E:\\Data\\CCERdata\\abt-buy\\buyProfiles",
            "E:\\Data\\CCERdata\\amazon-gp\\amazonProfiles",
            "E:\\Data\\CCERdata\\amazon-gp\\gpProfiles",
            "E:\\Data\\CCERdata\\dblp-acm\\dblpProfiles",
            "E:\\Data\\CCERdata\\dblp-acm\\acmProfiles",
            "E:\\Data\\CCERdata\\dblp-scholar\\dblpProfiles2",
            "E:\\Data\\CCERdata\\dblp-scholar\\scholarProfiles",
            "E:\\Data\\CCERdata\\movies\\imdbProfiles",
            "E:\\Data\\CCERdata\\movies\\dbpediaProfiles"
        };
        String[] groundTruthFilePath = {"E:\\Data\\CCERdata\\abt-buy\\abtBuyIdDuplicates",
            "E:\\Data\\CCERdata\\amazon-gp\\amazonGpIdDuplicates",
            "E:\\Data\\CCERdata\\dblp-acm\\dblpAcmIdDuplicates",
            "E:\\Data\\CCERdata\\dblp-scholar\\dblpScholarIdDuplicates",
            "E:\\Data\\CCERdata\\movies\\moviesIdDuplicates"
        };

        for (int i = 0; i < groundTruthFilePath.length; i++) {
            IEntityReader eReader1 = new EntitySerializationReader(entitiesFilePath[i*2]);
            List<EntityProfile> profiles1 = eReader1.getEntityProfiles();
            System.out.println("Input Entity Profiles\t:\t" + profiles1.size());
            
            IEntityReader eReader2 = new EntitySerializationReader(entitiesFilePath[i*2+1]);
            List<EntityProfile> profiles2 = eReader2.getEntityProfiles();
            System.out.println("Input Entity Profiles\t:\t" + profiles2.size());

            IGroundTruthReader gtReader = new GtSerializationReader(groundTruthFilePath[i]);
            final AbstractDuplicatePropagation duplicatePropagation = new BilateralDuplicatePropagation(gtReader.getDuplicatePairs(null));
            System.out.println("Existing Duplicates\t:\t" + duplicatePropagation.getDuplicates().size());

            for (BlockBuildingMethod blbuMethod : BlockBuildingMethod.values()) {
                if (!blbuMethod.equals(BlockBuildingMethod.STANDARD_BLOCKING)) {
                    continue;
                }

                double time1 = System.currentTimeMillis();

                System.out.println("\n\nCurrent blocking metohd\t:\t" + blbuMethod);
                IBlockBuilding blockBuildingMethod = BlockBuildingMethod.getDefaultConfiguration(blbuMethod);

                System.out.println("Block Building...");
                List<AbstractBlock> blocks = blockBuildingMethod.getBlocks(profiles1, profiles2);
                double time2 = System.currentTimeMillis();

                BlocksPerformance blStats = new BlocksPerformance(blocks, duplicatePropagation);
                blStats.setStatistics();
                blStats.printStatistics(time2 - time1, blockBuildingMethod.getMethodConfiguration(), blockBuildingMethod.getMethodName());
            }
        }
    }
}
