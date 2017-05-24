/*
* Copyright [2017] [George Papadakis (gpapadis@yahoo.gr)]
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
package DataReader.EntityReader;

import org.jdom2.Attribute;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;

import DataModel.EntityProfile;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author G.A.P. II
 */
public class XMLreader extends AbstractEntityReader {

    private static final Logger LOGGER = Logger.getLogger(XMLreader.class.getName());

    private final Set<String> attributesToExclude;
    private final Map<String, EntityProfile> urlToEntity;

    public XMLreader(String filePath) {
        super(filePath);
        attributesToExclude = new HashSet<>();
        urlToEntity = new HashMap<>();
    }

    @Override
    public List<EntityProfile> getEntityProfiles() {
        if (!entityProfiles.isEmpty()) {
            return entityProfiles;
        }

        if (inputFilePath == null) {
            LOGGER.log(Level.SEVERE, "Input file path has not been set!");
            return null;
        }

        //load the rdf model from the input file
        	SAXBuilder saxBuilder = new SAXBuilder();
            try {
				Document document = saxBuilder.build(inputFilePath);
				readXMLdoc(document);
			} catch (JDOMException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}


        return entityProfiles;
    }

    @Override
    public String getMethodInfo() {
        return "RDF Reader: converts an rdf file of any format into a set of entity profiles.";
    }

    @Override
    public String getMethodParameters() {
        return "The RDF Reader involves 1 parameter, in addition to the absolute file path:\n"
             + "attributesToExclude: String[], default value: owl:sameAs.\n"
             + "The names of the predicates that will be ignored during the creation of entity profiles.\n";
    }

    private void readXMLdoc(Document document) throws IOException {
        //read each ntriples
        //get spo, create a separate profile for each separate subject,
        //with Attribute=predicate and Value=object
    	Element classElement = document.getRootElement();

        List<Element> dblpRoot = classElement.getChildren();        
        for (int profCounter = 0; profCounter < dblpRoot.size(); profCounter++) {    
            Element profile = dblpRoot.get(profCounter);
            String profName = profile.getName();
            EntityProfile entityProfile = urlToEntity.get(profName);
            entityProfile = new EntityProfile(profName);
            entityProfiles.add(entityProfile);
            urlToEntity.put(profName, entityProfile);
            List<Element> profAttributes = profile.getChildren();
            for (int attCounter = 0; attCounter < profAttributes.size(); attCounter++) {
                Element attr = profAttributes.get(attCounter);
                String attName = attr.getName();
                if (attributesToExclude.contains(attName)) continue;
                String attValue = attr.getValue();
                entityProfile.addAttribute(attName, attValue);
            //if already exists a profile for the subject, simply add po as <Att>-<Value>
            }
        }
    }

    public void setAttributesToExclude(String[] attributesNamesToExclude) {
        attributesToExclude.addAll(Arrays.asList(attributesNamesToExclude));
    }
}
