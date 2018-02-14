/*
* Copyright [2016-2017] [George Papadakis (gpapadis@yahoo.gr)]
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

package Utilities.Enumerations;

import BlockProcessing.ComparisonCleaning.CardinalityEdgePruning;
import BlockProcessing.ComparisonCleaning.CardinalityNodePruning;
import BlockProcessing.ComparisonCleaning.ComparisonPropagation;
import BlockProcessing.ComparisonCleaning.ReciprocalCardinalityNodePruning;
import BlockProcessing.ComparisonCleaning.ReciprocalWeightedNodePruning;
import BlockProcessing.ComparisonCleaning.WeightedEdgePruning;
import BlockProcessing.ComparisonCleaning.WeightedNodePruning;
import BlockProcessing.IBlockProcessing;

/**
 *
 * @author GAP2
 */

public enum ComparisonCleaningMethod {
    CARDINALITY_EDGE_PRUNING,
    CARDINALITY_NODE_PRUNING,
    COMPARISON_PROPAGATION,
    RECIPROCAL_CARDINALITY_NODE_PRUNING,
    RECIPROCAL_WEIGHTING_NODE_PRUNING,
    WEIGHTED_EDGE_PRUNING,
    WEIGHTED_NODE_PRUNING;

    public static IBlockProcessing getDefaultConfiguration(ComparisonCleaningMethod coclMethod) {
        switch (coclMethod) {
            case CARDINALITY_EDGE_PRUNING:
                return new CardinalityEdgePruning();
            case CARDINALITY_NODE_PRUNING:
                return new CardinalityNodePruning();
            case COMPARISON_PROPAGATION:
                return new ComparisonPropagation();
            case RECIPROCAL_CARDINALITY_NODE_PRUNING:
                return new ReciprocalCardinalityNodePruning();
            case RECIPROCAL_WEIGHTING_NODE_PRUNING:
                return new ReciprocalWeightedNodePruning();
            case WEIGHTED_EDGE_PRUNING:
                return new WeightedEdgePruning();
            case WEIGHTED_NODE_PRUNING:
                return new WeightedNodePruning();
            default:
                return new ComparisonPropagation();
        }
    }
}
