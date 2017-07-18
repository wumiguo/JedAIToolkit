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
        String[] entitiesFilePath = {"D:\\Data\\newDatasetFormat\\cleanCleanERfiles\\abtProfiles",
            "D:\\Data\\newDatasetFormat\\cleanCleanERfiles\\buyProfiles",
            "D:\\Data\\newDatasetFormat\\cleanCleanERfiles\\amazonProfiles",
            "D:\\Data\\newDatasetFormat\\cleanCleanERfiles\\gpProfiles",
            "D:\\Data\\newDatasetFormat\\cleanCleanERfiles\\dblpProfiles",
            "D:\\Data\\newDatasetFormat\\cleanCleanERfiles\\acmProfiles",
            "D:\\Data\\newDatasetFormat\\cleanCleanERfiles\\dblpProfiles2",
            "D:\\Data\\newDatasetFormat\\cleanCleanERfiles\\scholarProfiles",
            "D:\\Data\\newDatasetFormat\\cleanCleanERfiles\\imdbProfiles",
            "D:\\Data\\newDatasetFormat\\cleanCleanERfiles\\dbpediaProfiles"
        };
        String[] groundTruthFilePath = {"D:\\Data\\newDatasetFormat\\cleanCleanERfiles\\abtBuyIdDuplicates",
            "D:\\Data\\newDatasetFormat\\cleanCleanERfiles\\amazonGpIdDuplicates",
            "D:\\Data\\newDatasetFormat\\cleanCleanERfiles\\dblpAcmIdDuplicates",
            "D:\\Data\\newDatasetFormat\\cleanCleanERfiles\\dblpScholarIdDuplicates",
            "D:\\Data\\newDatasetFormat\\cleanCleanERfiles\\moviesIdDuplicates"
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
