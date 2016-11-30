package EntityClustering;

import DataModel.Comparison;
import DataModel.EquivalenceCluster;
import DataModel.SimilarityPairs;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.jgrapht.alg.ConnectivityInspector;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.SimpleGraph;

/**
 *
 * @author G.A.P. II
 */
public abstract class AbstractEntityClustering implements IEntityClustering {

    private static final Logger LOGGER = Logger.getLogger(AbstractEntityClustering.class.getName());

    protected boolean isCleanCleanER;
    
    protected double threshold;
    
    protected int noOfEntities;
    protected int datasetLimit;
    protected int stDevMultiplier;

    protected SimpleGraph similarityGraph;

    public AbstractEntityClustering() {
        stDevMultiplier = 0;
    }

    protected List<EquivalenceCluster> getConnectedComponents() {
        // get connected components
        final ConnectivityInspector ci = new ConnectivityInspector(similarityGraph);
        final List<Set<Integer>> connectedComponents = ci.connectedSets();

        // prepare output
        final List<EquivalenceCluster> equivalenceClusters = new ArrayList<>();
        for (Set<Integer> componentIds : connectedComponents) {
            EquivalenceCluster newCluster = new EquivalenceCluster();
            equivalenceClusters.add(newCluster);

            if (!isCleanCleanER) {
                newCluster.loadBulkEntityIdsD1(componentIds);
                continue;
            }

            for (Integer entityId : componentIds) {
                if (entityId < datasetLimit) {
                    newCluster.addEntityIdD1(entityId);
                } else {
                    newCluster.addEntityIdD2(entityId - datasetLimit);
                }
            }
        }
        
        return equivalenceClusters;
    }

    protected int getMaxEntityId(int[] entityIds) {
        int maxId = Integer.MIN_VALUE;
        for (int i = 0; i < entityIds.length; i++) {
            if (maxId < entityIds[i]) {
                maxId = entityIds[i];
            }
        }
        return maxId;
    }

    protected void initializeData(SimilarityPairs simPairs) {
        isCleanCleanER = simPairs.isCleanCleanER();

        int maxEntity1 = getMaxEntityId(simPairs.getEntityIds1());
        int maxEntity2 = getMaxEntityId(simPairs.getEntityIds2());
        if (simPairs.isCleanCleanER()) {
            datasetLimit = maxEntity1 + 1;
            noOfEntities = maxEntity1 + maxEntity2 + 2;
        } else {
            datasetLimit = 0;
            noOfEntities = Math.max(maxEntity1, maxEntity2) + 1;
        }
        
        setSimilarityThreshold(simPairs);
    }
    
    protected void initializeGraph() {
        similarityGraph = new SimpleGraph(DefaultEdge.class);
        for (int i = 0; i < noOfEntities; i++) {
            similarityGraph.addVertex(i);
        }
        LOGGER.log(Level.INFO, "Added {0} nodes in the graph", noOfEntities);
    }
    
    protected String explainMultiplierParameter() {
        return "1) standard deviation (StDev) multiplier : integer in [-3, 3], default value: 0.\n"
             + "It determines the contribution of edge weights StDev to the cut-off similarity threshold,\n" 
             + "for connecting two entities with an edge in the (initial) similarity graph, as derived from the following formula:\n"
             + "sim. threshold = av. edge weight + multiplier * StDev.";
    }
    
    public void setMultiplier(int sdMulti) {
        stDevMultiplier = sdMulti;
    }
    
    protected void setSimilarityThreshold(SimilarityPairs simPairs) {
        double averageSimilarity = 0;
        Iterator<Comparison> iterator = simPairs.getPairIterator();
        while (iterator.hasNext()) {
            Comparison comparison = iterator.next();
            averageSimilarity += comparison.getUtilityMeasure();
        }
        averageSimilarity /= simPairs.getNoOfComparisons();

        double standardDeviation = 0;
        iterator = simPairs.getPairIterator();
        while (iterator.hasNext()) {
            Comparison comparison = iterator.next();
            standardDeviation += Math.pow(comparison.getUtilityMeasure() - averageSimilarity, 2.0);
        }
        standardDeviation = Math.sqrt(standardDeviation / simPairs.getNoOfComparisons());

        threshold = averageSimilarity + stDevMultiplier * standardDeviation;
        LOGGER.log(Level.INFO, "Similarity threshold : {0}", threshold);
    }
}
