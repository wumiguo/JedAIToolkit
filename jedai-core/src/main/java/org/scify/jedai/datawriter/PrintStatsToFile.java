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
package org.scify.jedai.datawriter;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.List;

import org.scify.jedai.datamodel.EntityProfile;
import org.scify.jedai.datamodel.EquivalenceCluster;

import com.esotericsoftware.minlog.Log;

import gnu.trove.iterator.TIntIterator;

public class PrintStatsToFile {

	private List<EntityProfile> profilesD1;
	private List<EntityProfile> profilesD2;
	private EquivalenceCluster[] entityClusters;

	public PrintStatsToFile(List<EntityProfile> profilesD1, List<EntityProfile> profilesD2, EquivalenceCluster[] entityClusters) {
        this.profilesD1=profilesD1;
        this.profilesD2=profilesD2;
        this.entityClusters=entityClusters;
    }
    

	public void printToCSV(String filename) throws FileNotFoundException {
        final PrintWriter pw = new PrintWriter(new File(filename));
        final StringBuilder sb = new StringBuilder();

        sb.append("cluster_id,dataset,entity_url\n");
        int counter = 0;
        for (EquivalenceCluster eqc : entityClusters) {
            if (eqc.getEntityIdsD1().isEmpty()) {
                continue;
            }
            counter++;
            for (TIntIterator iterator = eqc.getEntityIdsD1().iterator(); iterator.hasNext();) {
                sb.append(counter).append(",").append(1).append(",")
                        .append(profilesD1.get(iterator.next()).getEntityUrl()).append("\n");
            }
            if (eqc.getEntityIdsD2().isEmpty()) {
                continue;
            }
            if (profilesD2 == null) {
                Log.error("The entity profiles of Dataset 2 are missing!");
                continue;
            }
            for (TIntIterator iterator = eqc.getEntityIdsD2().iterator(); iterator.hasNext();) {
                sb.append(counter).append(",").append(2).append(",")
                        .append(profilesD2.get(iterator.next()).getEntityUrl()).append("\n");
            }

        }
        pw.write(sb.toString());
        pw.close();
    }
    
	public void printToRDF(String filename) throws FileNotFoundException {
        final PrintWriter printWriter = new PrintWriter(new File(filename));

        printWriter.println("<?xml version=\"1.0\"?>");
	    printWriter.println();
	    printWriter.println("<rdf:RDF");
	    printWriter.println("xmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\"");
	    printWriter.println("xmlns:obj=\"https://www.w3schools.com/rdf/\">");
	    
        int counter = 0;
        for (EquivalenceCluster eqc : entityClusters) {
            if (eqc.getEntityIdsD1().isEmpty()) {
                continue;
            }
            counter++;
            for (TIntIterator iterator = eqc.getEntityIdsD1().iterator(); iterator.hasNext();) {
        	    printWriter.println();

            	printWriter.println("<rdf:Description rdf:about=\""+counter+"\">");
            	
            	printWriter.print("<obj:"+"cluster_id"+">");
            	printWriter.print(counter+"");
            	printWriter.println("</obj:"+"cluster_id>");
            	
            	printWriter.print("<obj:"+"dataset"+">");
            	printWriter.print("1");
            	printWriter.println("</obj:"+"dataset>");
            	
            	printWriter.print("<obj:"+"entity_url"+">");
            	printWriter.print(profilesD1.get(iterator.next()).getEntityUrl().replace("&", "")+"");
            	printWriter.println("</obj:"+"entity_url"+">");
                
          	  printWriter.println("</rdf:Description>");

            }
            if (eqc.getEntityIdsD2().isEmpty()) {
                continue;
            }
            if (profilesD2 == null) {
                Log.error("The entity profiles of Dataset 2 are missing!");
                continue;
            }
            for (TIntIterator iterator = eqc.getEntityIdsD2().iterator(); iterator.hasNext();) {
                                
        	    printWriter.println();

                printWriter.println("<rdf:Description rdf:about=\""+counter+"\">");
            	
                printWriter.print("<obj:"+"cluster_id"+">");
            	printWriter.print(counter+"");
            	printWriter.println("</obj:"+"cluster_id>");
            	
            	printWriter.print("<obj:"+"dataset"+">");
            	printWriter.print("2");
            	printWriter.println("</obj:"+"dataset>");
            	
            	printWriter.print("<obj:"+"entity_url"+">");
            	printWriter.print(profilesD2.get(iterator.next()).getEntityUrl().replace("&", "")+"");
            	printWriter.println("</obj:"+"entity_url"+">");
                
          	  printWriter.println("</rdf:Description>");
                
            }

        }
	    printWriter.println("</rdf:RDF>");

        printWriter.close();
    }
	
	public void printToXML(String filename) throws FileNotFoundException {
        final PrintWriter printWriter = new PrintWriter(new File(filename));

        printWriter.println("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
	    printWriter.println();

	    printWriter.println("<general>");

	    
        int counter = 0;
        for (EquivalenceCluster eqc : entityClusters) {
            if (eqc.getEntityIdsD1().isEmpty()) {
                continue;
            }
            counter++;
            for (TIntIterator iterator = eqc.getEntityIdsD1().iterator(); iterator.hasNext();) {
        	    printWriter.println();
            	printWriter.println("<entity"+counter+">");
            	
            	printWriter.print("<cluster_id"+">");
            	printWriter.print(counter+"");
            	printWriter.println("</cluster_id>");
            	
            	printWriter.print("<dataset"+">");
            	printWriter.print("1");
            	printWriter.println("</dataset>");
            	
            	printWriter.print("<entity_url"+">");
            	printWriter.print(profilesD1.get(iterator.next()).getEntityUrl().replace("&", "")+"");
            	printWriter.println("</entity_url"+">");
                
            	printWriter.println("</entity"+counter+">");

            }
            if (eqc.getEntityIdsD2().isEmpty()) {
                continue;
            }
            if (profilesD2 == null) {
                Log.error("The entity profiles of Dataset 2 are missing!");
                continue;
            }
            for (TIntIterator iterator = eqc.getEntityIdsD2().iterator(); iterator.hasNext();) {
                                
        	    printWriter.println();

            	printWriter.println("<entity"+counter+">");
            	
                printWriter.print("<cluster_id"+">");
            	printWriter.print(counter+"");
            	printWriter.println("</cluster_id>");
            	
            	printWriter.print("<dataset"+">");
            	printWriter.print("2");
            	printWriter.println("</dataset>");
            	
            	printWriter.print("<entity_url"+">");
            	printWriter.print(profilesD2.get(iterator.next()).getEntityUrl().replace("&", "")+"");
            	printWriter.println("</entity_url"+">");
                
            	printWriter.println("</entity"+counter+">");
                
            }

        }
	    printWriter.println("</general>");

        printWriter.close();
    }
	
    
}
