/*
* Copyright [2016-2020] [George Papadakis (gpapadis@yahoo.gr)]
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
package org.scify.jedai.textmodels;

import org.scify.jedai.utilities.enumerations.RepresentationModel;
import org.scify.jedai.utilities.enumerations.SimilarityMetric;
import com.esotericsoftware.minlog.Log;
import gnu.trove.iterator.TObjectIntIterator;
import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import java.util.HashSet;
import java.util.Set;

/**
 *
 * @author G.A.P. II
 */
public class CharacterNGramsWithGlobalWeights extends CharacterNGrams {

    protected final static TObjectIntMap<String>[] DOC_FREQ = new TObjectIntMap[2];

    public CharacterNGramsWithGlobalWeights(int did, int n, RepresentationModel model, SimilarityMetric simMetric, String iName) {
        super(did, n, model, simMetric, iName);
    }

    @Override
    public void finalizeModel() {
        if (DOC_FREQ[datasetId] == null) {
            DOC_FREQ[datasetId] = new TObjectIntHashMap<>();
        }

        for (String keyValue : itemsFrequency.keySet()) {
            if (!DOC_FREQ[datasetId].increment(keyValue)) {
                DOC_FREQ[datasetId].put(keyValue, 1);
            }
        }
    }

    protected float getARCSSimilarity(CharacterNGramsWithGlobalWeights oModel) {
        final Set<String> commonKeys = new HashSet(itemsFrequency.keySet());
        commonKeys.retainAll(oModel.getItemsFrequency().keySet());

        float similarity = 0;
        if (datasetId == DATASET_1 && datasetId == oModel.getDatasetId()) { // Dirty ER
            for (String key : commonKeys) {
                double frequency = DOC_FREQ[DATASET_1].get(key);
                similarity += 1.0 / (Math.log1p(frequency * (frequency - 1) / 2.0) / Math.log(2));
            }
        } else if (datasetId != oModel.getDatasetId()) { // Clean-Clean ER
            for (String key : commonKeys) {
                similarity += 1.0 / (Math.log1p(((double) DOC_FREQ[DATASET_1].get(key)) * DOC_FREQ[DATASET_2].get(key)) / Math.log(2));
            }
        } else {
            Log.error("Both models come from dataset 1!");
            System.exit(-1);
        }

        return similarity;
    }

    protected double getIdfWeight(String keyValue) {
        int frequency = DOC_FREQ[datasetId].get(keyValue);
        if (frequency == 0) {
            return 0;
        }

        if (NO_OF_DOCUMENTS[datasetId] < frequency) {
            Log.error("Error in the computation of IDF weights!!!");
            return 0;
        }
        
        return Math.log10(NO_OF_DOCUMENTS[datasetId] / (1 + frequency));
    }

    protected float getSigmaSimilarity(CharacterNGramsWithGlobalWeights oModel) {
        double totalTerms2 = oModel.getNoOfTotalTerms();
        final TObjectIntMap<String> itemVector2 = oModel.getItemsFrequency();

        double numerator = 0.0;
        for (TObjectIntIterator<String> iterator = itemsFrequency.iterator(); iterator.hasNext();) {
            iterator.advance();
            int frequency2 = itemVector2.get(iterator.key());
            if (0 < frequency2) {
                numerator += iterator.value() / noOfTotalTerms * getIdfWeight(iterator.key())
                           + frequency2 / totalTerms2 * oModel.getIdfWeight(iterator.key());
            }
        }

        final Set<String> allKeys = new HashSet<>(itemsFrequency.keySet());
        allKeys.addAll(itemVector2.keySet());
        double denominator = 0.0;
        for (String key : allKeys) {
            denominator += itemsFrequency.get(key) / noOfTotalTerms  * getIdfWeight(key) + 
                           itemVector2.get(key) / totalTerms2 * oModel.getIdfWeight(key);
        }

        return (float)(numerator / denominator);
    }

    @Override
    public float getSimilarity(ITextModel oModel) {
        switch (simMetric) {
            case ARCS_SIMILARITY:
                return getARCSSimilarity((CharacterNGramsWithGlobalWeights) oModel);
            case COSINE_SIMILARITY:
                return getTfIdfCosineSimilarity((CharacterNGramsWithGlobalWeights) oModel);
            case GENERALIZED_JACCARD_SIMILARITY:
                return getTfIdfGeneralizedJaccardSimilarity((CharacterNGramsWithGlobalWeights) oModel);
            case SIGMA_SIMILARITY:
                return getSigmaSimilarity((CharacterNGramsWithGlobalWeights) oModel);
            default:
                Log.error("The given similarity metric is incompatible with the bag representation model!");
                System.exit(-1);
                return -1;
        }
    }

    protected float getTfIdfCosineSimilarity(CharacterNGramsWithGlobalWeights oModel) {
        double totalTerms2 = oModel.getNoOfTotalTerms();
        final TObjectIntMap<String> itemVector2 = oModel.getItemsFrequency();

        double numerator = 0.0;
        for (TObjectIntIterator<String> iterator = itemsFrequency.iterator(); iterator.hasNext();) {
            iterator.advance();
            int frequency2 = itemVector2.get(iterator.key());
            if (0 < frequency2) {
                numerator += (iterator.value() / noOfTotalTerms) * getIdfWeight(iterator.key())
                           * (frequency2 / totalTerms2) * oModel.getIdfWeight(iterator.key());
            }
        }

        double denominator = getVectorMagnitude() * oModel.getVectorMagnitude();
        return (float)(numerator / denominator);
    }

    protected float getTfIdfGeneralizedJaccardSimilarity(CharacterNGramsWithGlobalWeights oModel) {
        double totalTerms2 = oModel.getNoOfTotalTerms();
        final TObjectIntMap<String> itemVector2 = oModel.getItemsFrequency();

        double numerator = 0.0;
        for (TObjectIntIterator<String> iterator = itemsFrequency.iterator(); iterator.hasNext();) {
            iterator.advance();
            int frequency2 = itemVector2.get(iterator.key());
            if (0 < frequency2) {
                numerator += Math.min(iterator.value() / noOfTotalTerms * getIdfWeight(iterator.key()),
                                      frequency2 / totalTerms2 * oModel.getIdfWeight(iterator.key()));
            }
        }

        final Set<String> allKeys = new HashSet<>(itemsFrequency.keySet());
        allKeys.addAll(itemVector2.keySet());
        double denominator = 0.0;
        for (String key : allKeys) {
            denominator += Math.max(itemsFrequency.get(key) / noOfTotalTerms  * getIdfWeight(key),
                                    itemVector2.get(key) / totalTerms2 * oModel.getIdfWeight(key));
        }

        return (float)(numerator / denominator);
    }

    @Override
    protected double getVectorMagnitude() {
        double magnitude = 0.0;
        for (TObjectIntIterator<String> iterator = itemsFrequency.iterator(); iterator.hasNext();) {
            iterator.advance();
            magnitude += Math.pow(iterator.value() * getIdfWeight(iterator.key()) / noOfTotalTerms, 2.0);
        }

        return Math.sqrt(magnitude);
    }
    
    public static void resetGlobalValues(int datasetId) {
        NO_OF_DOCUMENTS[datasetId] = 0;
        if (DOC_FREQ[datasetId] != null) {
            DOC_FREQ[datasetId].clear();
        }
    }
}
