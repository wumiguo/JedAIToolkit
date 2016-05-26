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

import DataReader.AbstractReader;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.riot.RDFDataMgr;

import DataModel.Attribute;
import DataModel.EntityProfile;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 *
 * @author G.A.P. II
 */

public class N3Reader extends AbstractReader implements IEntityReader {
	
    private Set<String> attributesToExclude;
    
    public N3Reader(String filePath) {
        super(filePath);
        attributesToExclude = new HashSet<>();
    }
    
    public List<EntityProfile> getEntityProfiles() {
        if (inputFilePath == null) {
            System.err.println("Input file path has not been set!");
            return null;
        }
        
        //load the rdf model from the input file
        

        try {
        	Model model = RDFDataMgr.loadModel(inputFilePath);
        	readModel(model);
        	

        } catch (IOException ex) {
            ex.printStackTrace();
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
    	Statement stmt = null;
    	EntityProfile newProfile = null;
    	//read each ntriples
    	//get spo, create a separate profile for each separate subject,
    	//with Attribute=predicate and Value=object
    	while (iter.hasNext()) {
    		stmt = iter.nextStatement();
    		Resource subject = stmt.getSubject();
    		String sub =subject.toString();
        	Property predicate = stmt.getPredicate();
        	String pred =predicate.toString();
        	if (attributesToExclude.contains(pred)) {
                continue;
            }
        	RDFNode object = stmt.getObject();
        	String obj =object.toString();
        	
        	//if already exists a profile for the subject, simply add po as <Att>-<Value>
        	boolean in = ContainsUrl(entityProfiles, sub);
        	if (!in) {       	
        		newProfile = new EntityProfile(sub);
                entityProfiles.add(newProfile);
                newProfile.addAttribute(pred, obj);
        	}
        	else {
        		
        		newProfile.addAttribute(pred, obj);
        	}
        	

        }
    }
    
    public void setAttributesToExclude(String[] attributesNamesToExclude) {
        for (String attributeName : attributesNamesToExclude) {
            attributesToExclude.add(attributeName);
        }
    }
    
    public boolean ContainsUrl(List<EntityProfile> entityProfiles, String url) {
    	
    	for (EntityProfile ep : entityProfiles) {
	        if (ep.getEntityUrl().equals(url)) {
	            return true;
	        }
	    }
	    return false;
    }
    
    
    
    

    
    
	public static void main(String[] args) {
		String filePath = "/home/ethanos/Downloads/dbpedia_2015-10.nt";
		N3Reader n3reader = new N3Reader(filePath);
		n3reader.setAttributesToExclude(new String[]{"http://www.w3.org/2000/01/rdf-schema#label", "http://www.w3.org/2000/01/rdf-schema#label"});
		List<EntityProfile> profiles = n3reader.getEntityProfiles();
		for (EntityProfile profile : profiles) {
            System.out.println("\n\n" + profile.getEntityUrl());
            for (Attribute attribute : profile.getAttributes()) {
                System.out.print(attribute.toString());
                System.out.println();
            }
        }
	

	}
    
}