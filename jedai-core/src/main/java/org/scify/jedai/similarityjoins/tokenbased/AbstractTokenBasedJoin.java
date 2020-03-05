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

package org.scify.jedai.similarityjoins.tokenbased;

import org.scify.jedai.similarityjoins.AbstractSimilarityJoin;

/**
 *
 * @author gap2
 */
public abstract class AbstractTokenBasedJoin extends AbstractSimilarityJoin {
    
    protected final double threshold;
    
    AbstractTokenBasedJoin(double thr) {
        super();
        threshold = thr;
    }
    
    protected double calcSimilarity(int l1, int l2, double overlap) {
        return overlap / (l1 + l2 - overlap) + 1e-6;
    }
 
    protected int indexLength(int l) {
        return (int) ((1 - 2 * threshold / (1 + threshold)) * l + 1 + 1e-6);
    }

    protected int maxPossibleLength(int l) {
        return (int) (l / threshold + 1e-6);
    }

    protected int minPossibleLength(int l) {
        return (int) Math.ceil(l * threshold - 1e-6);
    }
    
    protected int probeLength(int l) {
        if (l==0) return 0;
        return (int) ((1 - threshold) * l + 1 + 1e-6);
    }

    protected int requireOverlap(int l1, int l2) {
        return (int) Math.ceil(threshold / (1 + threshold) * (l1 + l2) - 1e-6);
    }

    protected int djbHash(String str) {
        int hash = 5381;
        int len = str.length();

        for (int k = 0; k < len; k++) {
            hash += (hash << 5) + str.charAt(k);
        }

        return (hash & 0x7FFFFFFF);
    }
}
