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
public abstract class BagModel extends AbstractModel {

    protected double noOfTotalTerms;
    protected final TObjectIntMap<String> itemsFrequency;

    public BagModel(int dId, int n, RepresentationModel md, SimilarityMetric sMetric, String iName) {
        super(dId, n, md, sMetric, iName);

        itemsFrequency = new TObjectIntHashMap<>();
    }

    @Override
    public void finalizeModel() {
    }
    
    protected double getEnhancedJaccardSimilarity(BagModel oModel) {
        TObjectIntMap<String> itemVector1 = itemsFrequency;
        TObjectIntMap<String> itemVector2 = oModel.getItemsFrequency();
        if (itemVector2.size() < itemVector1.size()) {
            itemVector1 = oModel.getItemsFrequency();
            itemVector2 = itemsFrequency;
        }

        double numerator = 0.0;
        for (TObjectIntIterator<String> iterator = itemVector1.iterator(); iterator.hasNext();) {
            iterator.advance();
            numerator += Math.min(iterator.value(), itemVector2.get(iterator.key()));
        }

        double denominator = noOfTotalTerms + oModel.getNoOfTotalTerms() - numerator;
        return numerator / denominator;
    }
    
    @Override
    public double getEntropy(boolean normalized) {
        double entropy = 0.0;
        for (TObjectIntIterator<String> iterator = itemsFrequency.iterator(); iterator.hasNext();) {
            iterator.advance();
            double p_i = (iterator.value() / noOfTotalTerms);
            entropy -= (p_i * (Math.log10(p_i) / Math.log10(2.0d)));
        }
        
        if (normalized) {
            double maxEntropy = Math.log10(noOfTotalTerms) / Math.log10(2.0d);
            return entropy / maxEntropy;
        } 
            
        return entropy;
    }

    public TObjectIntMap<String> getItemsFrequency() {
        return itemsFrequency;
    }

    protected double getJaccardSimilarity(BagModel oModel) {
        final Set<String> commonKeys = new HashSet<>(itemsFrequency.keySet());
        commonKeys.retainAll(oModel.getItemsFrequency().keySet());

        double numerator = commonKeys.size();
        double denominator = itemsFrequency.size() + oModel.getItemsFrequency().size() - numerator;
        return numerator / denominator;
    }

    protected double getNoOfTotalTerms() {
        return noOfTotalTerms;
    }

    @Override
    public Set<String> getSignatures() {
        return itemsFrequency.keySet();
    }

    @Override
    public double getSimilarity(ITextModel oModel) {
        switch (simMetric) {
            case COSINE_SIMILARITY:
                return getTfCosineSimilarity((BagModel) oModel);
            case ENHANCED_JACCARD_SIMILARITY:
                return getEnhancedJaccardSimilarity((BagModel) oModel);
            case GENERALIZED_JACCARD_SIMILARITY:
                return getTfGeneralizedJaccardSimilarity((BagModel) oModel);
            case JACCARD_SIMILARITY:
                return getJaccardSimilarity((BagModel) oModel);
            default:
                Log.error("The given similarity metric is incompatible with the bag representation model!");
                System.exit(-1);
                return -1;
        }
    }

    protected double getTfCosineSimilarity(BagModel oModel) {
        double totalTerms2 = oModel.getNoOfTotalTerms();

        TObjectIntMap<String> itemVector1 = itemsFrequency;
        TObjectIntMap<String> itemVector2 = oModel.getItemsFrequency();
        if (itemVector2.size() < itemVector1.size()) {
            itemVector1 = oModel.getItemsFrequency();
            itemVector2 = itemsFrequency;
        }

        double numerator = 0.0;
        for (TObjectIntIterator<String> iterator = itemVector1.iterator(); iterator.hasNext();) {
            iterator.advance();
            numerator += iterator.value() * itemVector2.get(iterator.key()) / noOfTotalTerms / totalTerms2;
        }

        double denominator = getVectorMagnitude() * oModel.getVectorMagnitude();
        return numerator / denominator;
    }

    protected double getTfGeneralizedJaccardSimilarity(BagModel oModel) {
        double totalTerms1 = noOfTotalTerms;
        double totalTerms2 = oModel.getNoOfTotalTerms();
        TObjectIntMap<String> itemVector1 = itemsFrequency;
        TObjectIntMap<String> itemVector2 = oModel.getItemsFrequency();
        if (itemVector2.size() < itemVector1.size()) {
            itemVector1 = oModel.getItemsFrequency();
            itemVector2 = itemsFrequency;

            totalTerms1 = oModel.getNoOfTotalTerms();
            totalTerms2 = noOfTotalTerms;
        }

        double numerator = 0.0;
        for (TObjectIntIterator<String> iterator = itemVector1.iterator(); iterator.hasNext(); ) {
            iterator.advance();
            numerator += Math.min(iterator.value() / totalTerms1, itemVector2.get(iterator.key()) / totalTerms2);
        }

        final Set<String> allKeys = new HashSet<>(itemVector1.keySet());
        allKeys.addAll(itemVector2.keySet());
        double denominator = 0.0;
        for (String key : allKeys) {
            denominator += Math.max(itemVector1.get(key) / totalTerms1, itemVector2.get(key) / totalTerms2);
        }
        
        return numerator / denominator;
    }

    protected double getVectorMagnitude() {
        double magnitude = 0.0;
        for (TObjectIntIterator<String> iterator = itemsFrequency.iterator(); iterator.hasNext();) {
            iterator.advance();
            magnitude += Math.pow(iterator.value() / noOfTotalTerms, 2.0);
        }

        return Math.sqrt(magnitude);
    }
}
