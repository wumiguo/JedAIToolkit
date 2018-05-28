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
package org.scify.jedai.utilities;

import org.scify.jedai.utilities.datastructures.BilateralDuplicatePropagation;
import org.scify.jedai.utilities.datastructures.AbstractDuplicatePropagation;
import org.scify.jedai.datamodel.Comparison;
import org.scify.jedai.datamodel.EquivalenceCluster;

import com.esotericsoftware.minlog.Log;
import gnu.trove.iterator.TIntIterator;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.List;
import org.scify.jedai.datamodel.EntityProfile;
import org.scify.jedai.datamodel.IdDuplicates;

/**
 *
 * @author gap2
 */
public class ClustersPerformance {

    private double fMeasure;
    private double precision;
    private double recall;
    private double totalMatches;

    private final AbstractDuplicatePropagation abstractDP;
    private final List<EquivalenceCluster> entityClusters;

    public ClustersPerformance(List<EquivalenceCluster> clusters, AbstractDuplicatePropagation adp) {
        abstractDP = adp;
        abstractDP.resetDuplicates();
        entityClusters = clusters;
    }

    public int getDetectedDuplicates() {
        return abstractDP.getNoOfDuplicates();
    }

    public int getEntityClusters() {
        return entityClusters.size();
    }

    public int getExistingDuplicates() {
        return abstractDP.getExistingDuplicates();
    }

    public double getFMeasure() {
        return fMeasure;
    }

    public double getPrecision() {
        return precision;
    }

    public double getRecall() {
        return recall;
    }

    public double getTotalMatches() {
        return totalMatches;
    }

    public void printStatistics(double overheadTime, String methodName, String methodConfiguration) {
        System.out.println("\n\n\n**************************************************");
        System.out.println("Performance of : " + methodName);
        System.out.println("Configuration : " + methodConfiguration);
        System.out.println("**************************************************");
        System.out.println("No of clusters\t:\t" + entityClusters.size());
        System.out.println("Detected duplicates\t:\t" + abstractDP.getNoOfDuplicates());
        System.out.println("Existing duplicates\t:\t" + abstractDP.getExistingDuplicates());
        System.out.println("Total matches\t:\t" + totalMatches);
        System.out.println("Precision\t:\t" + precision);
        System.out.println("Recall\t:\t" + recall);
        System.out.println("F-Measure\t:\t" + fMeasure);
        System.out.println("Overhead time\t:\t" + overheadTime);
    }

    public void printDetailedResults(List<EntityProfile> profilesD1, List<EntityProfile> profilesD2, String outputFile) throws FileNotFoundException {
        if (entityClusters.isEmpty()) {
            Log.warn("Empty set of equivalence clusters given as input!");
            return;
        }

        totalMatches = 0;
        final PrintWriter pw = new PrintWriter(new File(outputFile));
        final StringBuilder sb = new StringBuilder();

        if (abstractDP instanceof BilateralDuplicatePropagation) { // Clean-Clean ER
            for (EquivalenceCluster cluster : entityClusters) {
                for (TIntIterator outIterator = cluster.getEntityIdsD1().iterator(); outIterator.hasNext();) {
                    if (cluster.getEntityIdsD2().isEmpty()) {
                        continue;
                    }
                    
                    final int entityId1 = outIterator.next();
                    final EntityProfile profile1 = profilesD1.get(entityId1);

                    for (TIntIterator inIterator = cluster.getEntityIdsD2().iterator(); inIterator.hasNext();) {
                        totalMatches++;

                        final int entityId2 = inIterator.next();
                        final EntityProfile profile2 = profilesD2.get(entityId2);

                        final int originalDuplicates = abstractDP.getNoOfDuplicates();
                        abstractDP.isSuperfluous(new Comparison(true, entityId1, entityId2));
                        final int newDuplicates = abstractDP.getNoOfDuplicates();

                        sb.append(profile1.getEntityUrl()).append(",");
                        sb.append(profile2.getEntityUrl()).append(",");
                        if (originalDuplicates == newDuplicates) {
                            sb.append("FP,"); //false positive
                        } else { // originalDuplicates < newDuplicates
                            sb.append("TP,"); // true positive
                        }
                        sb.append("Profile 1:[").append(profile1).append("]");
                        sb.append("Profile 2:[").append(profile2).append("]").append("\n");
                    }
                }

                for (IdDuplicates duplicatesPair : abstractDP.getFalseNegatives()) {
                    final EntityProfile profile1 = profilesD1.get(duplicatesPair.getEntityId1());
                    final EntityProfile profile2 = profilesD2.get(duplicatesPair.getEntityId2());

                    sb.append(profile1.getEntityUrl()).append(",");
                    sb.append(profile2.getEntityUrl()).append(",");
                    sb.append("FN,"); // false negative
                    sb.append("Profile 1:[").append(profile1).append("]");
                    sb.append("Profile 2:[").append(profile2).append("]").append("\n");
                }
            }
        } else { // Dirty ER
            for (EquivalenceCluster cluster : entityClusters) {
                final int[] duplicatesArray = cluster.getEntityIdsD1().toArray();

                for (int i = 0; i < duplicatesArray.length; i++) {
                    for (int j = i + 1; j < duplicatesArray.length; j++) {
                        totalMatches++;

                        final EntityProfile profile1 = profilesD1.get(duplicatesArray[i]);
                        final EntityProfile profile2 = profilesD1.get(duplicatesArray[j]);

                        final int originalDuplicates = abstractDP.getNoOfDuplicates();
                        abstractDP.isSuperfluous(new Comparison(false, duplicatesArray[i], duplicatesArray[j]));
                        final int newDuplicates = abstractDP.getNoOfDuplicates();

                        sb.append(profile1.getEntityUrl()).append(",");
                        sb.append(profile2.getEntityUrl()).append(",");
                        if (originalDuplicates == newDuplicates) {
                            sb.append("FP,"); //false positive
                        } else { // originalDuplicates < newDuplicates
                            sb.append("TP,"); // true positive
                        }
                        sb.append("Profile 1:[").append(profile1).append("]");
                        sb.append("Profile 2:[").append(profile2).append("]").append("\n");
                    }
                }
            }

            for (IdDuplicates duplicatesPair : abstractDP.getFalseNegatives()) {
                final EntityProfile profile1 = profilesD1.get(duplicatesPair.getEntityId1());
                final EntityProfile profile2 = profilesD1.get(duplicatesPair.getEntityId2());

                sb.append(profile1.getEntityUrl()).append(",");
                sb.append(profile2.getEntityUrl()).append(",");
                sb.append("FN,"); // false negative
                sb.append("Profile 1:[").append(profile1).append("]");
                sb.append("Profile 2:[").append(profile2).append("]").append("\n");
            }
        }

        if (0 < totalMatches) {
            precision = abstractDP.getNoOfDuplicates() / totalMatches;
        } else {
            precision = 0;
        }
        recall = ((double) abstractDP.getNoOfDuplicates()) / abstractDP.getExistingDuplicates();
        if (0 < precision && 0 < recall) {
            fMeasure = 2 * precision * recall / (precision + recall);
        } else {
            fMeasure = 0;
        }

        pw.write("Precision\t:\t" + precision + "\n");
        pw.write("Recall\t:\t" + recall + "\n");
        pw.write("F-Measure\t:\t" + fMeasure + "\n");
        pw.write(sb.toString());
        pw.close();
    }

    public void setStatistics() {
        if (entityClusters.isEmpty()) {
            Log.warn("Empty set of equivalence clusters given as input!");
            return;
        }

        totalMatches = 0;
        if (abstractDP instanceof BilateralDuplicatePropagation) { // Clean-Clean ER
            for (EquivalenceCluster cluster : entityClusters) {
                for (TIntIterator outIterator = cluster.getEntityIdsD1().iterator(); outIterator.hasNext();) {
                    int entityId1 = outIterator.next();
                    for (TIntIterator inIterator = cluster.getEntityIdsD2().iterator(); inIterator.hasNext();) {
                        totalMatches++;
                        Comparison comparison = new Comparison(true, entityId1, inIterator.next());
                        abstractDP.isSuperfluous(comparison);
                    }
                }
            }
        } else { // Dirty ER
            for (EquivalenceCluster cluster : entityClusters) {
                final int[] duplicatesArray = cluster.getEntityIdsD1().toArray();

                for (int i = 0; i < duplicatesArray.length; i++) {
                    for (int j = i + 1; j < duplicatesArray.length; j++) {
                        totalMatches++;
                        Comparison comparison = new Comparison(false, duplicatesArray[i], duplicatesArray[j]);
                        abstractDP.isSuperfluous(comparison);
                    }
                }
            }
        }

        if (0 < totalMatches) {
            precision = abstractDP.getNoOfDuplicates() / totalMatches;
        } else {
            precision = 0;
        }
        recall = ((double) abstractDP.getNoOfDuplicates()) / abstractDP.getExistingDuplicates();
        if (0 < precision && 0 < recall) {
            fMeasure = 2 * precision * recall / (precision + recall);
        } else {
            fMeasure = 0;
        }
    }
}
