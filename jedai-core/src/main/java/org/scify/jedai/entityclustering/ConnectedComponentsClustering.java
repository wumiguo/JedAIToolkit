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
package org.scify.jedai.entityclustering;

import org.scify.jedai.datamodel.Comparison;
import org.scify.jedai.datamodel.EquivalenceCluster;
import org.scify.jedai.datamodel.SimilarityPairs;
import java.util.Iterator;
import java.util.List;

/**
 *
 * @author G.A.P. II
 */
public class ConnectedComponentsClustering extends AbstractEntityClustering {

    public ConnectedComponentsClustering() {
        this(0.5);
    }
    
    public ConnectedComponentsClustering(double simTh) {
        super(simTh);
    }

    @Override
    public List<EquivalenceCluster> getDuplicates(SimilarityPairs simPairs) {
        initializeData(simPairs);
        initializeGraph();
        
        // add an edge for every pair of entities with a weight higher than the thrshold
        final Iterator<Comparison> iterator = simPairs.getPairIterator();
        while (iterator.hasNext()) {
            final Comparison comparison = iterator.next();
            if (threshold < comparison.getUtilityMeasure()) {
                similarityGraph.addEdge(comparison.getEntityId1(), comparison.getEntityId2() + datasetLimit);
            }
        }

        return getConnectedComponents();
    }

    @Override
    public String getMethodInfo() {
        return getMethodName() + ": it gets equivalence clsuters from the transitive closure of the similarity graph.";
    }

    @Override
    public String getMethodName() {
        return "Connected Components Clustering";
    }
}
