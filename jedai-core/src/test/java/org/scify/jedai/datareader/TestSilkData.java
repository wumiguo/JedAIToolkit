package org.scify.jedai.datareader;

import java.util.List;
import java.util.Set;
import org.scify.jedai.datamodel.EntityProfile;
import org.scify.jedai.datamodel.IdDuplicates;
import org.scify.jedai.datareader.entityreader.EntityRDFReader;
import org.scify.jedai.datareader.entityreader.EntitySerializationReader;
import org.scify.jedai.datareader.groundtruthreader.GtCSVReader;
import org.scify.jedai.datareader.groundtruthreader.GtRDFReader;

/**
 *
 * @author GAP2
 */
public class TestSilkData {
    public static void main(String[] args) {
        String mainDir = "C:\\Users\\GAP2\\Downloads\\datasets\\";
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
    }
}
