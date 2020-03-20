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
package org.scify.jedai.similarityjoins;

import gnu.trove.iterator.TIntIterator;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;
import java.util.*;

import org.scify.jedai.datamodel.*;

/**
 *
 * @author mthanos
 */
public abstract class AbstractSimilarityJoin implements ISimilarityJoin {

    protected boolean isCleanCleanER;

    protected int datasetDelimiter;
    protected int noOfEntities;

    protected String attributeNameD1;
    protected String attributeNameD2;

    protected List<EntityProfile> profilesD1;
    protected List<EntityProfile> profilesD2;

    public int[] originalIdInBlock;

    public AbstractSimilarityJoin() {
    }

    protected abstract SimilarityPairs applyJoin(String name1, String name2, List<EntityProfile> dataset1, List<EntityProfile> dataset2);

    @Override
    public SimilarityPairs executeFiltering(String attributeName, List<EntityProfile> dataset) {
        return this.executeFiltering(attributeName, null, dataset, null);
    }

    @Override
    public SimilarityPairs executeFiltering(String name1, String name2, List<EntityProfile> dataset1, List<EntityProfile> dataset2) {
        attributeNameD1 = name1;
        attributeNameD2 = name2;
        profilesD1 = dataset1;
        profilesD2 = dataset2;
        isCleanCleanER = profilesD2 != null;
        datasetDelimiter = dataset2 != null ? profilesD1.size() : 0;
        noOfEntities = profilesD2 == null ? profilesD1.size() : profilesD1.size() + profilesD2.size();

        return applyJoin(name1, name2, dataset1, dataset2);
    }

    protected String getAttributeValue(String attributeName, EntityProfile profile) {
        final StringBuilder sb = new StringBuilder();
        for (Attribute attribute : profile.getAttributes()) {
            if (attribute.getName().toLowerCase().trim().equals(attributeName)) {
                final String[] tokens = attribute.getValue().toLowerCase().split("[\\W_]");
                for (String token : tokens) {
                    if (0 < token.trim().length()) {
                        sb.append(token).append(" ");
                    }
                }
            }
        }

        return sb.toString().trim();
    }

    protected Comparison getComparison(int entityId, int neighborId) {
        if (this.originalIdInBlock != null) {
            if (entityId < datasetDelimiter) {
                return new Comparison(isCleanCleanER, originalIdInBlock[entityId], originalIdInBlock[neighborId]);
            } else {
                return new Comparison(isCleanCleanER, originalIdInBlock[neighborId], originalIdInBlock[entityId]);
            }
        }
        if (!isCleanCleanER) {
            if (entityId < neighborId) {
                return new Comparison(isCleanCleanER, entityId, neighborId);
            } else {
                return new Comparison(isCleanCleanER, neighborId, entityId);
            }
        } else {
            if (entityId < datasetDelimiter) {
                return new Comparison(isCleanCleanER, entityId, neighborId - datasetDelimiter);
            } else {
                return new Comparison(isCleanCleanER, neighborId, entityId - datasetDelimiter);
            }
        }
    }

    public SimilarityPairs executeFilteringInBlocks(String name1, String name2, List<EntityProfile> dataset1, List<EntityProfile> dataset2, List<AbstractBlock> blocks) {
        attributeNameD1 = name1;
        attributeNameD2 = name2;
        isCleanCleanER = profilesD2 != null;

        int noOfCOmparisons = 0;
        final List<SimilarityPairs> ListOfSimilarityPairsFromJoin = new ArrayList<>();
        for (AbstractBlock block : blocks) {
            final TIntSet idsInBlockDataset1 = new TIntHashSet();
            final TIntSet idsInBlockDataset2 = new TIntHashSet();

            //get involved ids in comparisons
            if (isCleanCleanER) {
                for (Comparison comp : block.getComparisons()) {
                    idsInBlockDataset1.add(comp.getEntityId1());
                    idsInBlockDataset2.add(comp.getEntityId2());
                }
            } else {
                for (Comparison comp : block.getComparisons()) {
                    idsInBlockDataset1.add(comp.getEntityId1());
                    idsInBlockDataset1.add(comp.getEntityId2());
                }
            }
            datasetDelimiter = profilesD2 != null ? profilesD1.size() : 0;
            noOfEntities = profilesD2 == null ? profilesD1.size() : profilesD1.size() + profilesD2.size();

            
            //add entity profiles in the respective inBlockProfs
            originalIdInBlock = new int[noOfEntities];

            int counter = 0;
            profilesD1 = new ArrayList<>();
            TIntIterator tIterator = idsInBlockDataset1.iterator();
            while (tIterator.hasNext()) {
                int entityId = tIterator.next();
                profilesD1.add(dataset1.get(entityId));
                originalIdInBlock[counter++]=entityId;
            }
            profilesD2 = new ArrayList<>();
            tIterator = idsInBlockDataset2.iterator();
            while (tIterator.hasNext()) {
                int entityId = tIterator.next();
                profilesD2.add(dataset1.get(entityId));
                originalIdInBlock[counter++]=entityId;
            }
            
            SimilarityPairs simPairs = applyJoin(name1, name2, profilesD1, profilesD2);
            noOfCOmparisons += simPairs.getNoOfComparisons();
            ListOfSimilarityPairsFromJoin.add(simPairs);
        }

        final SimilarityPairs totalSimilarityPairs = new SimilarityPairs(isCleanCleanER, noOfCOmparisons);
        for (SimilarityPairs similarityPairs : ListOfSimilarityPairsFromJoin) {
            final Iterator<Comparison> iterator = similarityPairs.getPairIterator();
            while (iterator.hasNext()) {
                totalSimilarityPairs.addComparison(iterator.next());
            }
        }

        return totalSimilarityPairs;
    }

    protected SimilarityPairs getSimilarityPairs(List<Comparison> comparisons) {
        final SimilarityPairs simPairs = new SimilarityPairs(isCleanCleanER, comparisons.size());
        for (Comparison comparison : comparisons) {
            simPairs.addComparison(comparison);
        }
        return simPairs;
    }
}
