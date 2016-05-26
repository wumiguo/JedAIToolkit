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

public class QGramsBlocking extends StandardBlocking {

    protected final int nGramSize;

    public QGramsBlocking(int n) {
        nGramSize = n;
    }

    @Override
    protected Set<String> getBlockingKeys(String attributeValue) {
        final Set<String> nGrams = new HashSet<>();
        for (String token : getTokens(attributeValue)) {
            nGrams.addAll(getNGrams(nGramSize, token));
        }

        return nGrams;
    }
    
    @Override
    public String getMethodInfo() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public String getMethodParameters() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
    protected List<String> getNGrams(int n, String blockingKey) {
        final List<String> nGrams = new ArrayList<>();
        if (blockingKey.length() < n) {
            nGrams.add(blockingKey);
        } else {
            int currentPosition = 0;
            final int length = blockingKey.length() - (n - 1);
            while (currentPosition < length) {
                nGrams.add(blockingKey.substring(currentPosition, currentPosition + n));
                currentPosition++;
            }
        }
        return nGrams;
    }
}