package org.scify.jedai.datareader;

import org.scify.jedai.datamodel.IdDuplicates;
import org.scify.jedai.datareader.entityreader.EntitySerializationReader;
import org.scify.jedai.datareader.groundtruthreader.GtCSVReader;
import java.util.Set;

/**
 *
 * @author G.A.P. II
 */

public class TestGtCSVReader {
    public static void main(String[] args) {
        String entityFilePath = "C:\\Users\\G.A.P. II\\Downloads\\cddbProfiles";
        String gtFilePath = "C:\\Users\\G.A.P. II\\Downloads\\cd_gold.csv";
        EntitySerializationReader esr = new EntitySerializationReader(entityFilePath);
        GtCSVReader csvReader = new GtCSVReader(gtFilePath);
        csvReader.setIgnoreFirstRow(true);
        csvReader.setSeparator(';');
        Set<IdDuplicates> duplicates = csvReader.getDuplicatePairs(esr.getEntityProfiles());
//        for (EntityProfile profile : profiles) {
//            System.out.println("\n\n" + profile.getEntityUrl());
//            for (Attribute attribute : profile.getAttributes()) {
//                System.out.println(attribute.toString());
//            }
//        }
        csvReader.storeSerializedObject(duplicates, "C:\\Users\\G.A.P. II\\Downloads\\cddbDuplicates");
    }
}