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

import info.debatty.java.lsh.MinHash;
import java.util.HashSet;
import java.util.Set;
import org.scify.jedai.textmodels.ITextModel;
import org.scify.jedai.textmodels.MinHashUnigrams;

/**
 *
 * @author GAP2
 */
public class LSHMinHashBlocking extends LSHSuperBitBlocking {

    protected MinHash minhash;

    public LSHMinHashBlocking() {
        this(5, 30);
    }

    public LSHMinHashBlocking(int bSize, int bandsNo) {
        super();

        bandSize = bSize;
        bandsNumber = bandsNo;
    }

    @Override
    protected Set<String> getBlockingKeys(int datasetId, int profileId) {
        final MinHashUnigrams model = (MinHashUnigrams) models[datasetId][profileId];

        final Set<Integer> termIds = model.getTermIds();
        int[] signature = minhash.signature(termIds);

        final Set<String> allKeys = new HashSet<>();
        for (int i = 0; i < signature.length - bandSize; i += bandSize) {
            final StringBuilder band = new StringBuilder(Integer.toString(i));
            for (int j = 0; j < bandSize; j++) {
                band.append("-").append(signature[i + j]);
            }
            band.append("BND").append(i);
            allKeys.add(band.toString());
        }
        return allKeys;
    }

    @Override
    protected ITextModel getModel(String instanceName) {
        return new MinHashUnigrams(instanceName);
    }

    @Override
    protected void initializeLshFunctions() {
        System.out.println("Dimensionality\t:\t" + MinHashUnigrams.getCorpusDimensionality());
        minhash = new MinHash(bandSize * bandsNumber, MinHashUnigrams.getCorpusDimensionality());
    }

    @Override
    protected void resetModel() {
        MinHashUnigrams.resetGlobalValues(DATASET_1);
    }

    @Override
    public String getMethodName() {
        return "LSH MinHash Blocking";
    }
}
