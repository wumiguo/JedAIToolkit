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

import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import info.debatty.java.lsh.MinHash;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.jena.atlas.json.JsonArray;
import org.apache.jena.atlas.json.JsonObject;
import org.scify.jedai.configuration.gridsearch.IntGridSearchConfiguration;
import org.scify.jedai.configuration.randomsearch.IntRandomSearchConfiguration;
import org.scify.jedai.datamodel.AbstractBlock;
import org.scify.jedai.datamodel.Attribute;
import org.scify.jedai.datamodel.EntityProfile;

/**
 *
 * @author GAP2
 */
public class LSHBlocking extends AbstractBlockBuilding {

    protected int bandSize;
    protected int signatureSize;

    protected MinHash minhash;
    protected final IntGridSearchConfiguration gridBndSize;
    protected final IntGridSearchConfiguration gridSigSize;
    protected final IntRandomSearchConfiguration randomBndSize;
    protected final IntRandomSearchConfiguration randomSigSize;
    protected TObjectIntMap<String> vocabulary;
    
    public LSHBlocking() {
        this(5, 150);
    }

    public LSHBlocking(int bSize, int sigSize) {
        super();

        bandSize = bSize;
        signatureSize = sigSize;
        
        gridBndSize = new IntGridSearchConfiguration(10, 2, 1);
        gridSigSize = new IntGridSearchConfiguration(1000, 50, 50);
        randomBndSize = new IntRandomSearchConfiguration(10, 2);
        randomSigSize = new IntRandomSearchConfiguration(1000, 50);
    }

    protected int[][] buildSignatures(List<EntityProfile> profiles) {
        int profileCounter = 0;
        final int[][] signatureProfiles = new int[profiles.size()][];
        final Set<Integer> currentSignatures = new HashSet<>();
        for (EntityProfile profile : profiles) {
            currentSignatures.clear();
            for (Attribute attribute : profile.getAttributes()) {
                final String[] tokens = attribute.getValue().toLowerCase().split("[\\W_]");
                for (String token : tokens) {
                    currentSignatures.add(vocabulary.get(token));
                }
            }
            signatureProfiles[profileCounter] = minhash.signature(currentSignatures);
            profileCounter++;
        }
        return signatureProfiles;
    }

    protected void buildVocabulary(List<EntityProfile> profiles) {
        for (EntityProfile profile : profiles) {
            for (Attribute attribute : profile.getAttributes()) {
                final String[] tokens = attribute.getValue().toLowerCase().split("[\\W_]");
                for (String token : tokens) {
                    int vocabularySize = vocabulary.size();
                    vocabulary.putIfAbsent(token, vocabularySize);
                }
            }
        }
    }

    @Override
    public List<AbstractBlock> getBlocks(List<EntityProfile> profilesD1,
            List<EntityProfile> profilesD2) {
        vocabulary = new TObjectIntHashMap<>();
        buildVocabulary(profilesD1);
        if (profilesD2 != null) {
            buildVocabulary(profilesD2);
        }

        minhash = new MinHash(signatureSize, vocabulary.size());
        return super.getBlocks(profilesD1, profilesD2);
    }

    @Override
    protected Set<String> getBlockingKeys(String attributeValue) {
        throw new UnsupportedOperationException("Not supported by LSH, because it uses global information, not local (i.e., not a mere attribute value).");
    }

    @Override
    public int getNumberOfGridConfigurations() {
        return gridBndSize.getNumberOfConfigurations() * gridSigSize.getNumberOfConfigurations();
    }

    @Override
    protected void indexEntities(Map<String, TIntList> index, List<EntityProfile> entities) {
        int[][] signatureProfiles = buildSignatures(entities);

        for (int profileId = 0; profileId < signatureProfiles.length; profileId++) {
            final Set<String> allKeys = new HashSet<>();
            for (int i = 0; i < signatureSize - bandSize; i += bandSize) {
                StringBuilder band = new StringBuilder(Integer.toString(i));
                for (int j = 0; j < bandSize; j++) {
                    band.append("-").append(Integer.toString(signatureProfiles[profileId][i + j]));
                }
                allKeys.add(band.toString());
            }

            for (String key : allKeys) {
                TIntList entityList = index.get(key);
                if (entityList == null) {
                    entityList = new TIntArrayList();
                    index.put(key, entityList);
                }
                entityList.add(profileId);
            }
        }
    }

    @Override
    public void setNextRandomConfiguration() {
        bandSize = (Integer) randomBndSize.getNextRandomValue();
        signatureSize = (Integer) randomSigSize.getNextRandomValue();
    }

    @Override
    public void setNumberedGridConfiguration(int iterationNumber) {
        int bandSizeIteration = iterationNumber / gridSigSize.getNumberOfConfigurations();
        bandSize = (Integer) gridBndSize.getNumberedValue(bandSizeIteration);
        
        int msLengthIteration = iterationNumber - bandSizeIteration * gridSigSize.getNumberOfConfigurations();
        signatureSize = (Integer) gridSigSize.getNumberedValue(msLengthIteration);
    }

    @Override
    public void setNumberedRandomConfiguration(int iterationNumber) {
        bandSize = (Integer) randomBndSize.getNumberedRandom(iterationNumber);
        signatureSize = (Integer) randomSigSize.getNumberedRandom(iterationNumber);
    }

    @Override
    public String getMethodConfiguration() {
        return getParameterName(0) + "=" + bandSize + ",\t"
                + getParameterName(1) + "=" + signatureSize;
    }

    @Override
    public String getMethodInfo() {
        return getMethodName() + ": it creates one block for every band that appears in the signatures of at least two entities.";
    }

    @Override
    public String getMethodName() {
        return "LSH Blocking";
    }

    @Override
    public String getMethodParameters() {
        return getMethodName() + " involves two parameters:\n"
                + "1)" + getParameterDescription(0) + ".\n"
                + "2)" + getParameterDescription(1) + ".";
    }

    @Override
    public JsonArray getParameterConfiguration() {
        final JsonObject obj1 = new JsonObject();
        obj1.put("class", "java.lang.Integer");
        obj1.put("name", getParameterName(0));
        obj1.put("defaultValue", "5");
        obj1.put("minValue", "2");
        obj1.put("maxValue", "10");
        obj1.put("stepValue", "1");
        obj1.put("description", getParameterDescription(0));

        final JsonObject obj2 = new JsonObject();
        obj2.put("class", "java.lang.Integer");
        obj2.put("name", getParameterName(1));
        obj2.put("defaultValue", "150");
        obj2.put("minValue", "50");
        obj2.put("maxValue", "1000");
        obj2.put("stepValue", "50");
        obj2.put("description", getParameterDescription(1));

        final JsonArray array = new JsonArray();
        array.add(obj1);
        array.add(obj2);
        return array;
    }

    @Override
    public String getParameterDescription(int parameterId) {
        switch (parameterId) {
            case 0:
                return "The " + getParameterName(0) + " determines the number of token ids that are used as blocking key.";
            case 1:
                return "The " + getParameterName(1) + " determines the number of token ids that represent all attribute values of an entity.";
            default:
                return "invalid parameter id";
        }
    }

    @Override
    public String getParameterName(int parameterId) {
        switch (parameterId) {
            case 0:
                return "Band size";
            case 1:
                return "Signature size";
            default:
                return "invalid parameter id";
        }
    }
}
