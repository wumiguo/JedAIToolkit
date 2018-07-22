package org.scify.jedai.datareader;

import java.util.List;
import java.util.Set;
import org.apache.jena.ext.com.google.common.base.Equivalence;
import org.scify.jedai.blockbuilding.IBlockBuilding;
import org.scify.jedai.blockprocessing.IBlockProcessing;
import org.scify.jedai.datamodel.AbstractBlock;
import org.scify.jedai.datamodel.EntityProfile;
import org.scify.jedai.datamodel.EquivalenceCluster;
import org.scify.jedai.datamodel.IdDuplicates;
import org.scify.jedai.datamodel.SimilarityPairs;
import org.scify.jedai.datareader.entityreader.EntityRDFReader;
import org.scify.jedai.datareader.groundtruthreader.GtRDFReader;
import org.scify.jedai.entityclustering.IEntityClustering;
import org.scify.jedai.entityclustering.UniqueMappingClustering;
import org.scify.jedai.entitymatching.IEntityMatching;
import org.scify.jedai.entitymatching.ProfileMatcher;
import org.scify.jedai.utilities.BlocksPerformance;
import org.scify.jedai.utilities.ClustersPerformance;
import org.scify.jedai.utilities.datastructures.AbstractDuplicatePropagation;
import org.scify.jedai.utilities.datastructures.BilateralDuplicatePropagation;
import org.scify.jedai.utilities.enumerations.BlockBuildingMethod;
import org.scify.jedai.utilities.enumerations.RepresentationModel;
import org.scify.jedai.utilities.enumerations.SimilarityMetric;

/**
 *
 * @author GAP2
 */
public class TestSilkData {
    public static void main(String[] args) {
        String mainDir = "C:\\Users\\GAP2\\Downloads\\classAndResult\\";
        String datasetD1Path = mainDir + "source.nt";
        String datasetD2Path = mainDir + "target.nt";
        String gtFilePath = mainDir + "source.nt";

        EntityRDFReader n3reader = new EntityRDFReader(datasetD1Path);
        List<EntityProfile> profilesD1 = n3reader.getEntityProfiles();
        System.out.println("Profiles D1\t:\t" + profilesD1.size());

        n3reader = new EntityRDFReader(datasetD2Path);
        List<EntityProfile> profilesD2 = n3reader.getEntityProfiles();
        System.out.println("Profiles D2\t:\t" + profilesD2.size());

        GtRDFReader gtrdfReader = new GtRDFReader(gtFilePath);
        Set<IdDuplicates> duplicates = gtrdfReader.getDuplicatePairs(profilesD1, profilesD2);
        System.out.println("Duplicate pairs\t:\t" + duplicates.size());

        double bestFMeasure = -1;
        String bestWorkflow = "";
        String bestConfiguration = "";
        AbstractDuplicatePropagation adp = new BilateralDuplicatePropagation(duplicates);

        for (BlockBuildingMethod blbuMethod : BlockBuildingMethod.values()) {
            final StringBuilder workflowConf = new StringBuilder();
            final StringBuilder workflowName = new StringBuilder();

            double time1 = System.currentTimeMillis();

            System.out.println("\n\nCurrent blocking metohd\t:\t" + blbuMethod);
            IBlockBuilding blockBuildingMethod = BlockBuildingMethod.getDefaultConfiguration(blbuMethod);
            List<AbstractBlock> blocks = blockBuildingMethod.getBlocks(profilesD1, profilesD2);
            System.out.println("Original blocks\t:\t" + blocks.size());

            workflowConf.append(blockBuildingMethod.getMethodConfiguration());
            workflowName.append(blockBuildingMethod.getMethodName());

            IBlockProcessing blockCleaningMethod = BlockBuildingMethod.getDefaultBlockCleaning(blbuMethod);
            if (blockCleaningMethod != null) {
                blocks = blockCleaningMethod.refineBlocks(blocks);

                workflowConf.append("\n").append(blockCleaningMethod.getMethodConfiguration());
                workflowName.append("->").append(blockCleaningMethod.getMethodName());
            }

            IBlockProcessing comparisonCleaningMethod = BlockBuildingMethod.getDefaultComparisonCleaning(blbuMethod);
            if (comparisonCleaningMethod != null) {
                blocks = comparisonCleaningMethod.refineBlocks(blocks);

                workflowConf.append("\n").append(comparisonCleaningMethod.getMethodConfiguration());
                workflowName.append("->").append(comparisonCleaningMethod.getMethodName());
            }

            double time2 = System.currentTimeMillis();

            BlocksPerformance bp = new BlocksPerformance(blocks, adp);
            bp.setStatistics();
            bp.printStatistics(time2 - time1, workflowConf.toString(), workflowName.toString());

            for (RepresentationModel model : RepresentationModel.values()) {
                double time3 = System.currentTimeMillis();

                IEntityMatching pm = new ProfileMatcher(model, SimilarityMetric.getModelDefaultSimMetric(model));
                SimilarityPairs simPairs = pm.executeComparisons(blocks, profilesD1, profilesD2);
                
                double time4 = System.currentTimeMillis();

                for (double simThreshold = 0.1; simThreshold < 1.0; simThreshold += 0.1) {
                    double time5 = System.currentTimeMillis();

                    IEntityClustering clustering = new UniqueMappingClustering(simThreshold);
                    EquivalenceCluster[] matches = clustering.getDuplicates(simPairs);

                    double time6 = System.currentTimeMillis();

                    final StringBuilder localWorkflowConf = new StringBuilder(workflowConf);
                    localWorkflowConf.append("\n").append(pm.getMethodConfiguration());
                    localWorkflowConf.append("\n").append(clustering.getMethodConfiguration());
                    
                    final StringBuilder localWorkflowName = new StringBuilder(workflowName);
                    localWorkflowName.append("->").append(pm.getMethodName());
                    localWorkflowName.append("->").append(clustering.getMethodName());
                    
                    double overheadTime = (time6 - time5) + (time4 - time3) + (time2 - time1);
                    ClustersPerformance cp = new ClustersPerformance(matches, adp);
                    cp.setStatistics();
                    cp.printStatistics(overheadTime, localWorkflowName.toString(), localWorkflowConf.toString());
                    
                    if (bestFMeasure < cp.getFMeasure()) {
                        bestFMeasure = cp.getFMeasure();
                        bestWorkflow = localWorkflowName.toString();
                        bestConfiguration = localWorkflowConf.toString();
                    }
                }
            }
            
            System.out.println("Best F-Measure\t:\t" + bestFMeasure);
            System.out.println("Best Workflow\t:\t" + bestWorkflow);
            System.out.println("Best Configuration\t:\t" + bestConfiguration);
        }
    }
}
