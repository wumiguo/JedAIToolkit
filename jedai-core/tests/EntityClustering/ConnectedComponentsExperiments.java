package EntityClustering;

import BlockBuilding.IBlockBuilding;
import BlockProcessing.IBlockProcessing;
import DataModel.AbstractBlock;
import DataModel.EntityProfile;
import DataModel.EquivalenceCluster;
import DataModel.SimilarityPairs;
import DataReader.EntityReader.EntitySerializationReader;
import DataReader.EntityReader.IEntityReader;
import DataReader.GroundTruthReader.GtSerializationReader;
import DataReader.GroundTruthReader.IGroundTruthReader;
import EntityMatching.IEntityMatching;
import EntityMatching.ProfileMatcher;
import Utilities.BlocksPerformance;
import Utilities.ClustersPerformance;
import Utilities.DataStructures.AbstractDuplicatePropagation;
import Utilities.DataStructures.UnilateralDuplicatePropagation;
import Utilities.Enumerations.BlockBuildingMethod;
import Utilities.Enumerations.RepresentationModel;
import Utilities.Enumerations.SimilarityMetric;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author G.A.P. II
 */
public class ConnectedComponentsExperiments {

    public static void main(String[] args) {
        BlockBuildingMethod blockingWorkflow = BlockBuildingMethod.STANDARD_BLOCKING;

        String[] datasetProfiles = {
            "E:\\Data\\csvProfiles\\restaurantProfiles",
            "E:\\Data\\csvProfiles\\coraProfiles",
            "E:\\Data\\csvProfiles\\cddbProfiles",
            "E:\\Data\\csvProfiles\\abtBuyProfiles",
            "E:\\Data\\csvProfiles\\amazonGpProfiles",
            "E:\\Data\\csvProfiles\\dblpAcmProfiles",
            "E:\\Data\\csvProfiles\\dblpScholarProfiles",
            "E:\\Data\\csvProfiles\\moviesProfiles"
        };

        String[] datasetGroundtruth = {
            "E:\\Data\\csvProfiles\\restaurantIdDuplicates",
            "E:\\Data\\csvProfiles\\coraIdDuplicates",
            "E:\\Data\\csvProfiles\\cddbIdDuplicates",
            "E:\\Data\\csvProfiles\\abtBuyIdDuplicates",
            "E:\\Data\\csvProfiles\\amazonGpIdDuplicates",
            "E:\\Data\\csvProfiles\\dblpAcmIdDuplicates",
            "E:\\Data\\csvProfiles\\dblpScholarIdDuplicates",
            "E:\\Data\\csvProfiles\\moviesIdDuplicates"
        };

        for (int datasetId = 0; datasetId < datasetProfiles.length; datasetId++) {
            System.out.println("\n\n\n\n\nCurrent dataset id\t:\t" + datasetId);;

            IEntityReader eReader = new EntitySerializationReader(datasetProfiles[datasetId]);
            List<EntityProfile> profiles = eReader.getEntityProfiles();
            System.out.println("Input Entity Profiles\t:\t" + profiles.size());

            IGroundTruthReader gtReader = new GtSerializationReader(datasetGroundtruth[datasetId]);
            final AbstractDuplicatePropagation duplicatePropagation = new UnilateralDuplicatePropagation(gtReader.getDuplicatePairs(eReader.getEntityProfiles()));
            System.out.println("Existing Duplicates\t:\t" + duplicatePropagation.getDuplicates().size());

            IBlockBuilding blockBuildingMethod = BlockBuildingMethod.getDefaultConfiguration(blockingWorkflow);
            List<AbstractBlock> blocks = blockBuildingMethod.getBlocks(profiles, null);
            System.out.println("Original blocks\t:\t" + blocks.size());

            IBlockProcessing blockCleaningMethod = BlockBuildingMethod.getDefaultBlockCleaning(blockingWorkflow);
            if (blockCleaningMethod != null) {
                blocks = blockCleaningMethod.refineBlocks(blocks);
            }

            IBlockProcessing comparisonCleaningMethod = BlockBuildingMethod.getDefaultComparisonCleaning(blockingWorkflow);
            if (comparisonCleaningMethod != null) {
                blocks = comparisonCleaningMethod.refineBlocks(blocks);
            }

            BlocksPerformance blp = new BlocksPerformance(blocks, duplicatePropagation);
            blp.setStatistics();
            blp.printStatistics();

            int bestStDevMulti = Integer.MIN_VALUE;
            double bestFMeasure = Double.MIN_VALUE;
            RepresentationModel bestRepModel = null;
            SimilarityMetric bestSimMetric = null;
            for (RepresentationModel repModel : RepresentationModel.values()) {
                System.out.println("\n\nCurrent model\t:\t" + repModel.toString());
                
                List<SimilarityMetric> simMetrics = SimilarityMetric.getModelCompatibleSimMetrics(repModel);
                for (SimilarityMetric simMetric : simMetrics) {
                    System.out.println("Current similarity metric\t:\t" + simMetric);

                    final List<AbstractBlock> copyOfBlocks = new ArrayList<>(blocks);

                    IEntityMatching em = new ProfileMatcher(repModel, SimilarityMetric.getModelDefaultSimMetric(repModel));
                    SimilarityPairs simPairs = em.executeComparisons(copyOfBlocks, profiles);

                    for (int stDevMulti = -3; stDevMulti < 4; stDevMulti++) {
                        IEntityClustering ec = new ConnectedComponentsClustering();
                        ec.setMultiplier(stDevMulti);
                        
                        List<EquivalenceCluster> entityClusters = ec.getDuplicates(simPairs);

                        ClustersPerformance clp = new ClustersPerformance(entityClusters, duplicatePropagation);
                        clp.setStatistics();
                        clp.printStatistics();
                        
                        double currentFMeasure = clp.getFMeasure();
                        if (bestFMeasure < currentFMeasure) {
                            bestFMeasure = currentFMeasure;
                            bestRepModel = repModel;
                            bestSimMetric = simMetric;
                            bestStDevMulti = stDevMulti;
                        }
                    }
                }
            }
            
            System.out.println("\n\n\n\n\nBest configuration...");
            System.out.println("Best F-Measure\t:\t" + bestFMeasure);
            System.out.println("Best Representation Model\t:\t" + bestRepModel);
            System.out.println("Best Similarity Metric\t:\t" + bestSimMetric);
            System.out.println("Best St. Dev. Multiplier\t:\t" + bestStDevMulti);
        }
    }
}
