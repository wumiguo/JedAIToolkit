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

package BlockBuilding;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 *
 * @author gap2
 */
public class ExtendedQGrams extends QGramsBlocking {

    private final double threshold;
    private final static int MAX_Q_GRAMS = 15;

    public ExtendedQGrams(double t, int n) {
        super(n);
        threshold = t;
    }

    @Override
    protected Set<String> getBlockingKeys(String attributeValue) {
        final Set<String> keys = new HashSet<>();
        for (String token : getTokens(attributeValue)) {
            List<String> nGrams = getNGrams(nGramSize, token);
            if (nGrams.size() == 1) {
                keys.add(nGrams.get(0));
            } else {
                if (MAX_Q_GRAMS < nGrams.size()) {
                    nGrams = nGrams.subList(0, MAX_Q_GRAMS);
                }

                int minimumLength = (int) Math.max(1, Math.floor(nGrams.size() * threshold));
                for (int i = minimumLength; i <= nGrams.size(); i++) {
                    keys.addAll(getCombinationsFor(nGrams, i));
                }
            }
        }
        return keys;
    }
    
    protected Set<String> getCombinationsFor(List<String> sublists, int sublistLength) {
        if (sublistLength == 0 || sublists.size() < sublistLength) {
            return new HashSet<>();
        }

        List<String> remainingElements = new ArrayList<>(sublists);
        String lastSublist = remainingElements.remove(sublists.size() - 1);

        final Set<String> combinationsExclusiveX = getCombinationsFor(remainingElements, sublistLength);
        final Set<String> combinationsInclusiveX = getCombinationsFor(remainingElements, sublistLength - 1);

        final Set<String> resultingCombinations = new HashSet<>();
        resultingCombinations.addAll(combinationsExclusiveX);
        if (combinationsInclusiveX.isEmpty()) {
            resultingCombinations.add(lastSublist);
        } else {
            combinationsInclusiveX.stream().forEach((combination) -> {
                resultingCombinations.add(combination + lastSublist);
            });
        }
        return resultingCombinations;
    }
    
    @Override
    public String getMethodInfo() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public String getMethodParameters() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
}
