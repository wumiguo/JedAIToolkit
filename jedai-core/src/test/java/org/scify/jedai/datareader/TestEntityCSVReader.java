package org.scify.jedai.datareader;

import org.scify.jedai.datamodel.Attribute;
import org.scify.jedai.datamodel.EntityProfile;
import org.scify.jedai.datareader.entityreader.EntityCSVReader;
import java.util.List;

/**
 *
 * @author G.A.P. II
 */

public class TestEntityCSVReader {
    public static void main(String[] args) {
        String filePath = "C:/Users/Manos/workspaceMars/JedAIgitFinal/JedAIgitFinal/datasets/converter/DBLP2.csv";
        EntityCSVReader csvReader = new EntityCSVReader(filePath);
        csvReader.setAttributeNamesInFirstRow(true);
        csvReader.setSeparator(';');
        csvReader.setAttributesToExclude(new int[]{1});
        csvReader.setIdIndex(0);
        List<EntityProfile> profiles = csvReader.getEntityProfiles();
        for (EntityProfile profile : profiles) {
            System.out.println("\n\n" + profile.getEntityUrl());
            for (Attribute attribute : profile.getAttributes()) {
                System.out.println(attribute.toString());
            }
        }
        csvReader.storeSerializedObject(profiles, "C:/Users/Manos/workspaceMars/JedAIgitFinal/JedAIgitFinal/datasets/converter/DBLP2prof");
        csvReader.convertToRDFfile(profiles, "C:/Users/Manos/workspaceMars/JedAIgitFinal/JedAIgitFinal/datasets/converter/DBLP2toRDFxml.xml");

    }
}
