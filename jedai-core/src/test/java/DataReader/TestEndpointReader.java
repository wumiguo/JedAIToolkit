package DataReader;

import DataModel.Attribute;
import DataModel.EntityProfile;
import DataReader.EntityReader.EndpointReader;
import java.util.List;


public class TestEndpointReader {
    public static void main(String[] args) {
        String endpointURL = "http://linkedgeodata.org/sparql";
        String filePath = "C:/Users/Manos/workspaceMars/JedAIToolkit-master/datasets/endpoint";
        EndpointReader n3reader = new EndpointReader(endpointURL);
        n3reader.setAttributesToExclude(new String[]{"http://www.w3.org/2000/01/rdf-schema#label", "http://www.w3.org/2000/01/rdf-schema#label"});
        List<EntityProfile> profiles = n3reader.getEntityProfiles();
        for (EntityProfile profile : profiles) {
            System.out.println("\n\n" + profile.getEntityUrl());
            for (Attribute attribute : profile.getAttributes()) {
                System.out.print(attribute.toString());
                System.out.println();
            }
        }
        n3reader.storeSerializedObject(profiles, filePath+"profilesEndpoint");
    }
}
