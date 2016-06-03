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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import DataModel.EntityProfile;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author G.A.P. II
 */

public class EntityDBReader extends AbstractEntityReader {

    private static final Logger LOGGER = Logger.getLogger(EntityDBReader.class.getName());
    
    private String table;
    private String password;
    private String user;
    private String role;
    private Boolean ssl;
    private final Set<String> attributesToExclude;
    private final Map<String, EntityProfile> urlToEntity;
    
    public EntityDBReader(String dbURL) {
        super(dbURL);
        ssl = true;
        attributesToExclude = new HashSet<>();
        urlToEntity = new HashMap<>();
    }

    @Override
    public List<EntityProfile> getEntityProfiles() {
        if (!entityProfiles.isEmpty()) {
            return entityProfiles;
        }

        
        if (inputFilePath == null) {
            LOGGER.log(Level.SEVERE, "Database url has not been set!");
            return null;
        }

        //inputFilePath is assigned the Database URL
        try {
        	Connection conn = null;
        	if (inputFilePath.contains("mysql"))
        	{
        		conn = getMySQLconnection(inputFilePath);
        	}
        	else if (inputFilePath.contains("postgresql"))
        	{
        		conn = getPostgreSQLconnection(inputFilePath);
        	}
        	java.sql.Statement stmt = null;
        	stmt = conn.createStatement();
  	        String sql = "SELECT * FROM "+table; //retrieve the appropriate table
  	        java.sql.ResultSet rs = stmt.executeQuery(sql);
  	        java.sql.ResultSetMetaData rsmd = rs.getMetaData();
  	        int columnsNum = rsmd.getColumnCount();
  	        String[] columns = new String[columnsNum];
  	        for (int i=0; i < columnsNum; i++)
  	        {
  	        	columns[i]=rsmd.getColumnName(i+1);//get attribute names
  	        }
  	        EntityProfile newProfile = null;
  	        //Extract data from result set
  	        while(rs.next()){ 	
  	           //Retrieve by column name
  	           String id  = rs.getString(columns[0]);
  	           newProfile = new EntityProfile(id);//create a new profile for each record
 	            entityProfiles.add(newProfile);
  	           for (int i=1; i < columnsNum; i++) {
  	        	   String attributeName = columns[i];
  	        	 if (attributesToExclude.contains(attributeName)) {
   	                continue;
   	            }
  	        	String value = rs.getString(columns[i]);
  	        	newProfile.addAttribute(attributeName, value);  	        	 
      	           
  	           }
  	        }
  	        rs.close();
        }catch(Exception e){
	        //Handle errors for Class.forName
	        e.printStackTrace();
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

    private Connection getMySQLconnection(String dbURL) throws IOException {

    	Connection conn = null;
    	   try{
    		   conn = DriverManager.getConnection("jdbc:"+dbURL + "?user="+user + "&password="+password);
     	      Class.forName("com.mysql.jdbc.Driver");

    	     }catch(Exception e){
    	        //Handle errors for Class.forName
    	        e.printStackTrace();
    	     }
    	
    	   return conn;
    }
    
    private Connection getPostgreSQLconnection(String url) throws IOException {
    	Connection conn = null;
    	try{
 		   Properties props = new Properties();
 		   if (!(user==null)) props.setProperty("user",user);
 		   if (!(password==null)) props.setProperty("password",password);
 		   if (ssl) props.setProperty("ssl","true");   
 		   conn = DriverManager.getConnection("jdbc:"+url, props);
 	     }catch(Exception e){
 	        //Handle errors for Class.forName
 	        e.printStackTrace();
 	     }
    	return conn;
     
    }
    public void setTable(String table) {
        this.table = table;
    }
    
    public void setPassword(String password) {
        this.password = password;
    }
    
    public void setUser(String user) {
        this.user = user;
    }
    
    public void setRole(String role) {
        this.role = role;
    }
    public void setSSL(Boolean ssl) {
        this.ssl = ssl;
    }
    public void setAttributesToExclude(String[] attributesNamesToExclude) {
        attributesToExclude.addAll(Arrays.asList(attributesNamesToExclude));
    }
}
