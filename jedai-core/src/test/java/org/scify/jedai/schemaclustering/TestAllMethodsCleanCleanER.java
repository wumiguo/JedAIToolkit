/*
* Copyright [2016-2018] [George Papadakis (gpapadis@yahoo.gr)]
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
package org.scify.jedai.schemaclustering;

import gnu.trove.iterator.TObjectIntIterator;
import gnu.trove.map.TObjectIntMap;
import java.util.List;
import org.apache.log4j.BasicConfigurator;
import org.scify.jedai.datamodel.EntityProfile;
import org.scify.jedai.datareader.entityreader.EntitySerializationReader;
import org.scify.jedai.utilities.enumerations.RepresentationModel;
import org.scify.jedai.utilities.enumerations.SimilarityMetric;

/**
 *
 * @author GAP2
 */
public class TestAllMethodsCleanCleanER {

    public static void main(String[] args) {
        BasicConfigurator.configure();

        String mainDir = "D:\\Data\\JedAIdata\\datasets\\cleanCleanErDatasets\\";
        final String[] datasetName = {"abtBuy", "amazonGp", "dblpAcm", "dblpScholar", "movies", "restaurants"};
        final String[] datasetsD1 = {"abt", "amazon", "dblp", "dblp2", "imdb", "restaurant1"};
        final String[] datasetsD2 = {"buy", "gp", "acm", "scholar", "dbpedia", "restaurant2"};

        for (int i = 0; i < datasetsD1.length; i++) {
            System.out.println("\n\n\n\n\nCurrent dataset\t:\t" + datasetName[i]);
            final EntitySerializationReader inReaderD1 = new EntitySerializationReader(mainDir + datasetsD1[i] + "Profiles");
            final List<EntityProfile> profilesD1 = inReaderD1.getEntityProfiles();
            System.out.println("Profiles D1\t:\t" + profilesD1.size());

            final EntitySerializationReader inReaderD2 = new EntitySerializationReader(mainDir + datasetsD2[i] + "Profiles");
            final List<EntityProfile> profilesD2 = inReaderD2.getEntityProfiles();
            System.out.println("Profiles D2\t:\t" + profilesD2.size());

            final AttributeValueClustering avc = new AttributeValueClustering(RepresentationModel.CHARACTER_TRIGRAMS, SimilarityMetric.ENHANCED_JACCARD_SIMILARITY);
            final TObjectIntMap<String>[] clusters = avc.getClusters(profilesD1, profilesD2);
            for (int j = 0; j < clusters.length; j++) {
                System.out.println("\nDataset\t:\t" + (j + 1));
                final TObjectIntIterator<String> it = clusters[j].iterator();
                while (it.hasNext()) {
                    it.advance();
                    System.out.println(it.key() + "\t" + it.value());
                }
            }
        }
    }
}
