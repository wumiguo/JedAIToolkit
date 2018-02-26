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
import java.util.List;

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
