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
package EntityMatching;

import DataModel.AbstractBlock;
import DataModel.Attribute;
import DataModel.Comparison;
import DataModel.EntityProfile;
import DataModel.SimilarityPairs;
import Utilities.Enumerations.RepresentationModel;
import Utilities.TextModels.AbstractModel;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.jgrapht.*;
import org.jgrapht.graph.*;

/**
 *
 * @author G.A.P. II
 */
public class GroupLinkageWithMatrix extends AbstractEntityMatching {

    private static final Logger LOGGER = Logger.getLogger(GroupLinkageWithMatrix.class.getName());

    private boolean isCleanCleanER;

    protected List<AbstractModel[]> entityModelsD1;
    protected List<AbstractModel[]> entityModelsD2;
    protected RepresentationModel representationModel;
    protected double similarityThreshold;

    public GroupLinkageWithMatrix(RepresentationModel model) {
        representationModel = model;
        similarityThreshold = 0.1;
        LOGGER.log(Level.INFO, "Initializing profile matcher with : {0}", model);
    }

    @Override
    public SimilarityPairs executeComparisons(List<AbstractBlock> blocks,
            List<EntityProfile> profilesD1, List<EntityProfile> profilesD2) {
        if (profilesD1 == null) {
            LOGGER.log(Level.SEVERE, "First list of entity profiles is null! "
                    + "The first argument should always contain entities.");
            System.exit(-1);
        }

        boolean isCleanCleanER = false;
        entityModelsD1 = getModels(profilesD1);
        if (profilesD2 != null) {

            isCleanCleanER = true;
            entityModelsD2 = getModels(profilesD2);
        }
        this.isCleanCleanER =isCleanCleanER; 

        SimilarityPairs simPairs = new SimilarityPairs(isCleanCleanER, blocks);
        for (AbstractBlock block : blocks) {
            final Iterator<Comparison> iterator = block.getComparisonIterator();
            while (iterator.hasNext()) {
                Comparison currentComparison = iterator.next();
                double[][] simMatrix = getSimilarityMatrix(currentComparison);
                currentComparison.setUtilityMeasure(getSimilarity(simMatrix));
                simPairs.addComparison(currentComparison);
            }
        }
        return simPairs;
    }

    //Every element of the getModels list is an AbstractModel[] array, corresponding to 
    //a profile. Every element of these arrays is a text-model corresponding to an attribute.
    private List<AbstractModel[]> getModels(List<EntityProfile> profiles) {

        List<AbstractModel[]> ModelsList = new ArrayList<AbstractModel[]>();
        for (EntityProfile profile : profiles) {
            int counter = 0;
            AbstractModel[] model = new AbstractModel[profile.getProfileSize()];
            for (Attribute attribute : profile.getAttributes()) {
                if (!attribute.getValue().isEmpty()) {
                    model[counter] = RepresentationModel.getModel(representationModel, attribute.getName());
                    model[counter].updateModel(attribute.getValue());
                    counter++;
                }
            }
            model = Arrays.copyOf(model, counter);
            ModelsList.add(model);

        }
        return ModelsList;
    }

    @Override
    public String getMethodInfo() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public String getMethodParameters() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    public double[][] getSimilarityMatrix(Comparison comparison) {
        AbstractModel[] model1 = entityModelsD1.get(comparison.getEntityId1());
        AbstractModel[] model2;
        if (isCleanCleanER) {
            model2 = entityModelsD2.get(comparison.getEntityId2());
        } else {
            model2 = entityModelsD1.get(comparison.getEntityId2());
        }

        int s1 = model1.length;
        int s2 = model2.length;
        double[][] modelsim = new double[s1][s2];
        for (int i = 0; i < s1; i++) {
            for (int j = 0; j < s2; j++) {
                modelsim[i][j] = model1[i].getSimilarity(model2[j]);
            }
        }
        return modelsim;
    }

    public double getSimilarity(double[][] simMatrix) {
        double similarity = 0;
        int rows = simMatrix.length;
        int columns = simMatrix[0].length;
        WeightedGraph<String, DefaultWeightedEdge> graph
                = new SimpleDirectedWeightedGraph<String, DefaultWeightedEdge>(DefaultWeightedEdge.class);
        for (int i = 0; i < rows; i++) {
            graph.addVertex("a" + i);
        }
        for (int j = 0; j < columns; j++) {
            graph.addVertex("b" + j);
        }
        int[][] sortEdgePosHelp = new int[2][rows * columns];
        double[] sortEdgeHelp = new double[rows * columns];
        int edgeCounter = 0;
        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < columns; j++) {
                double sim = simMatrix[i][j];
                if (sim > similarityThreshold) {
                    DefaultWeightedEdge e = graph.addEdge("a" + i, "b" + j);
                    graph.setEdgeWeight(e, sim);
                    sortEdgePosHelp[0][edgeCounter] = i;//keep row
                    sortEdgePosHelp[1][edgeCounter] = j;//keep columns
                    sortEdgeHelp[edgeCounter] = sim;//put similarity
                    edgeCounter++;
                }

            }
        }

        QuicksortEdges(sortEdgeHelp, sortEdgePosHelp, 0, edgeCounter - 1);
    	//System.out.println("sorted");
        double nominator = 0;
        double denominator = graph.vertexSet().size(); //m1+m2
        for (int j = 0; j < edgeCounter; j++) {
            if (graph.containsEdge("a" + sortEdgePosHelp[0][j], "b" + sortEdgePosHelp[1][j]))//if edge is not
            {																			//already removed
                graph.removeVertex("a" + sortEdgePosHelp[0][j]);//remove vertices so all adjacent edges
                graph.removeVertex("b" + sortEdgePosHelp[1][j]);//are removed
                graph.addVertex("a" + sortEdgePosHelp[0][j]);//add them again
                graph.addVertex("b" + sortEdgePosHelp[1][j]);//and put only the current edge
                DefaultWeightedEdge e = graph.addEdge("a" + sortEdgePosHelp[0][j], "b" + sortEdgePosHelp[1][j]);
                graph.setEdgeWeight(e, sortEdgeHelp[j]);
                nominator += sortEdgeHelp[j];
                denominator-=1.0;
            }
            

        }

        
        similarity = nominator / denominator;

        return similarity;
    }

    public void QuicksortEdges(double array[], int pos[][], int start, int end) {
        int i = start;                          // index of left-to-right scan
        int k = end;                            // index of right-to-left scan

        if (end - start >= 1) // check that there are at least two elements to sort
        {
            double pivot = array[start];       // set the pivot as the first element in the partition

            while (k > i) // while the scan indices from left and right have not met,
            {
                while (array[i] >= pivot && i <= end && k > i) // from the left, look for the first
                {
                    i++;                                    // element greater than the pivot
                }
                while (array[k] < pivot && k >= start && k >= i) // from the right, look for the first
                {
                    k--;                                        // element not greater than the pivot
                }
                if (k > i) // if the left seekindex is still smaller than
                {
                    swap(array, pos, i, k);                      // the right index, swap the corresponding elements
                }
            }
            swap(array, pos, start, k);          // after the indices have crossed, swap the last element in
            // the left partition with the pivot 
            QuicksortEdges(array, pos, start, k - 1); // quicksort the left partition
            QuicksortEdges(array, pos, k + 1, end);   // quicksort the right partition
        } else // if there is only one element in the partition, do not do any sorting
        {
            return;                     // the array is sorted, so exit
        }
    }

    public void swap(double array[], int pos[][], int index1, int index2) // pre: array is full and index1, index2 < array.length
    // post: the values at indices 1 and 2 have been swapped
    {
        double temp = array[index1];           // store the first value in a temp
        array[index1] = array[index2];      // copy the value of the second into the first
        array[index2] = temp;
        int temp0 = pos[0][index1];           // store the first value in a temp
        pos[0][index1] = pos[0][index2];      // copy the value of the second into the first
        pos[0][index2] = temp0;
        int temp1 = pos[1][index1];           // store the first value in a temp
        pos[1][index1] = pos[1][index2];      // copy the value of the second into the first
        pos[1][index2] = temp1;// copy the value of the temp into the second
    }

    public void setSimilarityThreshold(double p) {
        this.similarityThreshold = p;
    }

}
