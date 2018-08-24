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
        csvReader.convertToRDFfile(profiles, "C:/Users/Manos/workspaceMars/JedAIgitFinal/JedAIgitFinal/datasets/converter/DBLP2toRDFxml.xml", "http://w3/");

    }
}
