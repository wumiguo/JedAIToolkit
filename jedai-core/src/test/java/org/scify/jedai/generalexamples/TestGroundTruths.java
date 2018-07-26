/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.scify.jedai.generalexamples;

import java.util.List;
import java.util.Set;
import org.apache.log4j.BasicConfigurator;
import org.scify.jedai.blockbuilding.StandardBlocking;
import org.scify.jedai.datamodel.AbstractBlock;
import org.scify.jedai.datamodel.Comparison;
import org.scify.jedai.datamodel.ComparisonIterator;
import org.scify.jedai.datamodel.EntityProfile;
import org.scify.jedai.datamodel.EquivalenceCluster;
import org.scify.jedai.datamodel.IdDuplicates;
import org.scify.jedai.datareader.AbstractReader;
import org.scify.jedai.datareader.entityreader.EntitySerializationReader;
import org.scify.jedai.datareader.entityreader.IEntityReader;
import org.scify.jedai.utilities.datastructures.BilateralDuplicatePropagation;
import org.scify.jedai.utilities.datastructures.UnilateralDuplicatePropagation;
import org.scify.jedai.utilities.enumerations.BlockBuildingMethod;

/**
 *
 * @author gap2
 */
public class TestGroundTruths {

    public static void main(String[] args) {
        BasicConfigurator.configure();
        
        String mainDir = "C:\\Users\\gap2\\Downloads\\";
        String entitiesFilePath = mainDir + "coraProfiles";
        String uniGT = mainDir + "coraIdDuplicates";

        final Set<IdDuplicates> derDuplicates = (Set<IdDuplicates>) AbstractReader.loadSerializedObject(uniGT);
        System.out.println("Duplicates DER\t:\t" + derDuplicates.size());

        final UnilateralDuplicatePropagation udp = new UnilateralDuplicatePropagation(derDuplicates);
        List<EquivalenceCluster> clusters = udp.getRealEquivalenceClusters();
        for (EquivalenceCluster c : clusters) {
            System.out.println(c.getEntityIdsD1());
        }

        IEntityReader eReader = new EntitySerializationReader(entitiesFilePath);
        List<EntityProfile> profiles = eReader.getEntityProfiles();
        System.out.println("Input Entity Profiles\t:\t" + profiles.size());

        StandardBlocking sb = (StandardBlocking) BlockBuildingMethod.getDefaultConfiguration(BlockBuildingMethod.STANDARD_BLOCKING);
        List<AbstractBlock> blocks = sb.getBlocks(profiles);

        for (AbstractBlock block : blocks) {
            final ComparisonIterator ci = block.getComparisonIterator();
            while (ci.hasNext()) {
                Comparison comp = ci.next();
                udp.isSuperfluous(comp.getEntityId1(), comp.getEntityId2());
            }
        }

        clusters = udp.getDetectedEquivalenceClusters();
        for (EquivalenceCluster c : clusters) {
            System.out.println(c.getEntityIdsD1());
        }
        
        String biGT = mainDir + "abtBuyIdDuplicatesCCER";

        final Set<IdDuplicates> ccerDuplicates = (Set<IdDuplicates>) AbstractReader.loadSerializedObject(biGT);
        System.out.println("Duplicates DER\t:\t" + derDuplicates.size());

        final BilateralDuplicatePropagation bdp = new BilateralDuplicatePropagation(ccerDuplicates);
        clusters = bdp.getRealEquivalenceClusters();
        for (EquivalenceCluster c : clusters) {
            System.out.println(c.getEntityIdsD1() + "\t" + c.getEntityIdsD2());
        }
        
        String entitiesFilePathD1 = mainDir + "abtProfiles";
        eReader = new EntitySerializationReader(entitiesFilePathD1);
        List<EntityProfile> profilesD1 = eReader.getEntityProfiles();
        System.out.println("Input Entity Profiles D1\t:\t" + profilesD1.size());
        
        String entitiesFilePathD2 = mainDir + "buyProfiles";
        eReader = new EntitySerializationReader(entitiesFilePathD2);
        List<EntityProfile> profilesD2 = eReader.getEntityProfiles();
        System.out.println("Input Entity Profiles D2\t:\t" + profilesD2.size());
        
        sb = (StandardBlocking) BlockBuildingMethod.getDefaultConfiguration(BlockBuildingMethod.STANDARD_BLOCKING);
        blocks = sb.getBlocks(profilesD1, profilesD2);

        for (AbstractBlock block : blocks) {
            final ComparisonIterator ci = block.getComparisonIterator();
            while (ci.hasNext()) {
                Comparison comp = ci.next();
                bdp.isSuperfluous(comp.getEntityId1(), comp.getEntityId2());
            }
        }
        
        clusters = bdp.getDetectedEquivalenceClusters();
        for (EquivalenceCluster c : clusters) {
            System.out.println(c.getEntityIdsD1() + "\t" + c.getEntityIdsD2());
        }
    }
}
