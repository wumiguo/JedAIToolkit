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
package Utilities.Enumerations;

/**
 *
 * @author G.A.P. II
 */
public enum SimilarityMetric {
    COSINE_SIMILARITY,
    ENHANCED_JACCARD_SIMILARITY,
    GENERALIZED_JACCARD_SIMILARITY,
    GRAPH_CONTAINMENT_SIMILARITY,
    GRAPH_NORMALIZED_VALUE_SIMILARITY,
    GRAPH_VALUE_SIMILARITY,
    GRAPH_OVERALL_SIMILARITY,
    JACCARD_SIMILARITY;

    public static SimilarityMetric getModelDefaultSimMetric(RepresentationModel model) {
        switch (model) {
            case CHARACTER_BIGRAMS:
                return JACCARD_SIMILARITY;
            case CHARACTER_BIGRAM_GRAPHS:
                return GRAPH_VALUE_SIMILARITY;
            case CHARACTER_FOURGRAMS:
                return JACCARD_SIMILARITY;
            case CHARACTER_FOURGRAM_GRAPHS:
                return GRAPH_VALUE_SIMILARITY;
            case CHARACTER_TRIGRAMS:
                return JACCARD_SIMILARITY;
            case CHARACTER_TRIGRAM_GRAPHS:
                return GRAPH_VALUE_SIMILARITY;
            case TOKEN_BIGRAMS:
                return COSINE_SIMILARITY;
            case TOKEN_BIGRAM_GRAPHS:
                return GRAPH_VALUE_SIMILARITY;
            case TOKEN_TRIGRAMS:
                return COSINE_SIMILARITY;
            case TOKEN_TRIGRAM_GRAPHS:
                return GRAPH_VALUE_SIMILARITY;
            case TOKEN_UNIGRAMS:
                return COSINE_SIMILARITY;
            case TOKEN_UNIGRAM_GRAPHS:
                return GRAPH_VALUE_SIMILARITY;
            default:
                return null;
        }
    }
}
