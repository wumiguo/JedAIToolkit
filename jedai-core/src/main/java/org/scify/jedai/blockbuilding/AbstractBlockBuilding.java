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
package org.scify.jedai.blockbuilding;

import org.scify.jedai.datamodel.AbstractBlock;
import org.scify.jedai.datamodel.Attribute;
import org.scify.jedai.datamodel.BilateralBlock;
import org.scify.jedai.datamodel.EntityProfile;
import org.scify.jedai.datamodel.UnilateralBlock;

import com.esotericsoftware.minlog.Log;

import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author gap2
 */
public abstract class AbstractBlockBuilding implements IBlockBuilding {

    protected double noOfEntitiesD1;
    protected double noOfEntitiesD2;

    protected final List<AbstractBlock> blocks;
    protected List<EntityProfile> entityProfilesD1;
    protected List<EntityProfile> entityProfilesD2;
    protected Map<String, TIntList> invertedIndexD1;
    protected Map<String, TIntList> invertedIndexD2;
            
    public AbstractBlockBuilding() {
        blocks = new ArrayList<>();
        entityProfilesD1 = null;
        entityProfilesD2 = null;
    }

    protected void buildBlocks() {
        indexEntities(invertedIndexD1, entityProfilesD1);

        if (invertedIndexD2 != null) {
            indexEntities(invertedIndexD2, entityProfilesD2);
        }
    }

    protected abstract Set<String> getBlockingKeys(String attributeValue);

    @Override
    public List<AbstractBlock> getBlocks(List<EntityProfile> profiles) {
        return this.getBlocks(profiles, null);
    }

    @Override
    public List<AbstractBlock> getBlocks(List<EntityProfile> profilesD1,
            List<EntityProfile> profilesD2) {
        Log.info("Applying " + getMethodName() + " with the following configuration : " + getMethodConfiguration());

        if (profilesD1 == null) {
            Log.error("First list of entity profiles is null! The first argument should always contain entities.");
            return null;
        }

        invertedIndexD1 = new HashMap<>();
        entityProfilesD1 = profilesD1;
        noOfEntitiesD1 = entityProfilesD1.size();
        if (profilesD2 != null) {
            invertedIndexD2 = new HashMap<>();
            entityProfilesD2 = profilesD2;
            noOfEntitiesD2 = entityProfilesD2.size();
        }

        buildBlocks();
        return readBlocks();
    }

    public double getBruteForceComparisons() {
        if (entityProfilesD2 == null) {
            return noOfEntitiesD1 * (noOfEntitiesD1 - 1) / 2;
        }
        return noOfEntitiesD1 * noOfEntitiesD2;
    }

    public double getTotalNoOfEntities() {
        if (entityProfilesD2 == null) {
            return noOfEntitiesD1;
        }
        return noOfEntitiesD1 + noOfEntitiesD2;
    }

    protected void indexEntities(Map<String, TIntList> index, List<EntityProfile> entities) {
        int counter = 0;
        for (EntityProfile profile : entities) {
            final Set<String> allKeys = new HashSet<>();
            for (Attribute attribute : profile.getAttributes()) {
                for (String key : getBlockingKeys(attribute.getValue().toLowerCase())) {
                    String normalizedKey = key.trim();
                    if (0 < normalizedKey.length()) {
                        allKeys.add(normalizedKey);
                    }
                }
            }

            for (String key : allKeys) {
                TIntList entityList = index.get(key);
                if (entityList == null) {
                    entityList = new TIntArrayList();
                    index.put(key, entityList);
                }
                entityList.add(counter);
            }
            counter++;
        }
    }

    protected void parseIndex() {
        invertedIndexD1.values().stream().filter((entityList) -> (1 < entityList.size())).forEachOrdered((entityList) -> {
            blocks.add(new UnilateralBlock(entityList.toArray()));
        });
    }

    protected void parseIndices() {
        invertedIndexD1.entrySet().forEach((entry) -> {
            final TIntList entityIdsD2 = invertedIndexD2.get(entry.getKey());
            if (entityIdsD2 != null && !entityIdsD2.isEmpty()) {
                blocks.add(new BilateralBlock(entry.getValue().toArray(), entityIdsD2.toArray()));
            }
        });
    }

    //read blocks from the inverted index
    public List<AbstractBlock> readBlocks() {
        if (entityProfilesD2 == null) { //Dirty ER
            parseIndex();
        } else { // Clean-Clean ER
            parseIndices();
        }

        return blocks;
    }
}
