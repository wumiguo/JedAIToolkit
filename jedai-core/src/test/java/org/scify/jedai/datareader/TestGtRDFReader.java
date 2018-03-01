package org.scify.jedai.datareader;

import org.scify.jedai.datamodel.EntityProfile;
import org.scify.jedai.datamodel.IdDuplicates;
import org.scify.jedai.datareader.entityreader.EntityRDFReader;
import org.scify.jedai.datareader.groundtruthreader.GtRDFReader;

import java.util.List;
import java.util.Set;

/**
 *
 * @author G.A.P. II
 */

public class TestGtRDFReader {
    public static void main(String[] args) {
        String FilePath = "/home/ethanos/Downloads/dbpedia_2015-10.nt";
        EntityRDFReader rdfReader = new EntityRDFReader(FilePath);
        GtRDFReader gtrdfReader = new GtRDFReader(FilePath);
        rdfReader.setAttributesToExclude(new String[]{"http://www.w3.org/2002/07/owl#sameAs"});
        List<EntityProfile> profiles = rdfReader.getEntityProfiles();
        Set<IdDuplicates> duplicates = gtrdfReader.getDuplicatePairs(profiles);
        for (IdDuplicates duplicate : duplicates) {
        	int id1 = duplicate.getEntityId1();
            System.out.println(id1 + " " + duplicate.getEntityId2());

        }
    }
}