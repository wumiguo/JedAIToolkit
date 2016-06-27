/*
* Copyright [2016] [George Papadakis (gpapadis@yahoo.gr)]
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

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.riot.RDFDataMgr;

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

public class EntityRDFReader extends AbstractEntityReader {

    private static final Logger LOGGER = Logger.getLogger(EntityRDFReader.class.getName());
    
    private final Set<String> attributesToExclude;
    private final Map<String, EntityProfile> urlToEntity;
    
    public EntityRDFReader(String filePath) {
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
        try {
            Model model = RDFDataMgr.loadModel(inputFilePath);
            readModel(model);
        } catch (IOException ex) {
            LOGGER.log(Level.SEVERE, null, ex);
            return null;
        }

        return entityProfiles;
    }

    @Override
    public String getMethodInfo() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public String getMethodParameters() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    private void readModel(Model m) throws IOException {
        StmtIterator iter = m.listStatements();
        Statement stmt;
        //read each ntriples
        //get spo, create a separate profile for each separate subject,
        //with Attribute=predicate and Value=object
        while (iter.hasNext()) {
            stmt = iter.nextStatement();
            
            Property predicate = stmt.getPredicate();
            String pred = predicate.toString();
            if (attributesToExclude.contains(pred)) {
                continue;
            }
            
            Resource subject = stmt.getSubject();
            String sub = subject.toString();
            
            RDFNode object = stmt.getObject();
            String obj = object.toString();

            //if already exists a profile for the subject, simply add po as <Att>-<Value>
            EntityProfile existingProfile = urlToEntity.get(sub);
            if (existingProfile == null) {
            	
                EntityProfile newProfile = new EntityProfile(sub);
                if (!obj.isEmpty()) newProfile.addAttribute(pred, obj);
                entityProfiles.add(newProfile);
                urlToEntity.put(sub, newProfile);
                
            } else {
            	if (!obj.isEmpty()) existingProfile.addAttribute(pred, obj);
            	
            }
        }
    }

    public void setAttributesToExclude(String[] attributesNamesToExclude) {
        attributesToExclude.addAll(Arrays.asList(attributesNamesToExclude));
    }
}
