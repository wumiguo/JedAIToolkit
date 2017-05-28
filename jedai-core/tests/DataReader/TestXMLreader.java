package DataReader;

import DataModel.Attribute;
import DataModel.EntityProfile;
import DataReader.EntityReader.XMLreader;
import java.util.List;

/**
 *
 * @author G.A.P. II
 */

public class TestXMLreader {
    public static void main(String[] args) {
        String filePath = "C:/Users/Manos/workspaceMars/JedAIToolkit-master/datasets/exam.xml";
        XMLreader n3reader = new XMLreader(filePath);
        List<EntityProfile> profiles = n3reader.getEntityProfiles();
        for (EntityProfile profile : profiles) {
            System.out.println("\n\n" + profile.getEntityUrl());
            for (Attribute attribute : profile.getAttributes()) {
                System.out.print(attribute.toString());
                System.out.println();
            }
        }
        n3reader.storeSerializedObject(profiles, (filePath+"PROFILES"));
    }
}
