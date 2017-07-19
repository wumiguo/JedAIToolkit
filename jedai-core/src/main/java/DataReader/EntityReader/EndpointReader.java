package DataReader.EntityReader;

import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;


import DataModel.EntityProfile;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

public class EndpointReader extends AbstractEntityReader {

    private static final Logger LOGGER = Logger.getLogger(EndpointReader.class.getName());

    private final Set<String> attributesToExclude;
    private final Map<String, EntityProfile> urlToEntity;

    public EndpointReader(String filePath) {
        super(filePath);
        attributesToExclude = new HashSet<>();
        attributesToExclude.add("owl:sameAs");
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
            readEndpoint(inputFilePath);
        } catch (IOException ex) {
            LOGGER.log(Level.SEVERE, null, ex);
            return null;
        }

        return entityProfiles;
    }

    @Override
    public String getMethodInfo() {
        return "SPARQL Endpoint Reader: converts a SPARQL endpoint into a set of entity profiles.";
    }

    @Override
    public String getMethodParameters() {
        return "The Endpoint Reader involves 1 parameter, in addition to the absolute endpoint url:\n"
             + "attributesToExclude: String[], default value: owl:sameAs.\n"
             + "The names of the predicates that will be ignored during the creation of entity profiles.\n";
    }

    private void readEndpoint(String endpointUrl) throws IOException {
        //read each ntriples
        //get spo, create a separate profile for each separate subject,
        //with Attribute=predicate and Value=object
    	String sparqlQueryString1 ="select ?a ?b ?c where {?a ?b ?c}";

    	Query query = QueryFactory.create(sparqlQueryString1);
    	QueryExecution qexec = QueryExecutionFactory.sparqlService(endpointUrl, query);
    	ResultSet results = qexec.execSelect();
    	//ResultSetFormatter.out(System.out, results, query);       
    	//results = qexec.execSelect();

    	
        while (results.hasNext()) {
            QuerySolution qs = results.next();


            String sub = qs.get("a").toString();
            String pred = qs.get("b").toString();
            String obj = qs.get("c").toString();
            if (attributesToExclude.contains(pred)) {
                continue;
            }




            //if already exists a profile for the subject, simply add po as <Att>-<Value>
            EntityProfile entityProfile = urlToEntity.get(sub);
            if (entityProfile == null) {
                entityProfile = new EntityProfile(sub);
                entityProfiles.add(entityProfile);
                urlToEntity.put(sub, entityProfile);
            } 
            
            if (!obj.isEmpty()) {
                entityProfile.addAttribute(pred, obj);
            }
        }
        qexec.close() ;
    }

    public void setAttributesToExclude(String[] attributesNamesToExclude) {
        attributesToExclude.addAll(Arrays.asList(attributesNamesToExclude));
    }
}
