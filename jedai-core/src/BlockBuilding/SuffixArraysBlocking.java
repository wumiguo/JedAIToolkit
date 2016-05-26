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

import DataModel.AbstractBlock;
import DataModel.EntityProfile;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 *
 * @author gap2
 */
public class SuffixArraysBlocking extends StandardBlocking {

    protected final int maximumBlockSize;
    protected final int minimumSuffixLength;
    
    public SuffixArraysBlocking(int maxSize, int minLength) {
        maximumBlockSize = maxSize;
        minimumSuffixLength = minLength;
    }

     @Override
    public List<AbstractBlock> getBlocks(List<EntityProfile> profilesD1,
            List<EntityProfile> profilesD2) {
        List<AbstractBlock> purgedBlocks = super.getBlocks(profilesD1, profilesD2);
        Iterator<AbstractBlock> blocksIterator = purgedBlocks.iterator();
        while (blocksIterator.hasNext()) {
            AbstractBlock block = (AbstractBlock) blocksIterator.next();
            if (maximumBlockSize < block.getTotalBlockAssignments()) {
                blocksIterator.remove();
            }
        }
        return purgedBlocks;
    }
    
    @Override
    protected Set<String> getBlockingKeys(String attributeValue) {
        final Set<String> suffixes = new HashSet<>();
        for (String token : getTokens(attributeValue)) {
            suffixes.addAll(getSuffixes(minimumSuffixLength, token));
        }
        return suffixes;
    }
    
    @Override
    public String getMethodInfo() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public String getMethodParameters() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
    public Set<String> getSuffixes(int minimumLength, String blockingKey) {
        final Set<String> suffixes = new HashSet<>();
        if (blockingKey.length() < minimumLength) {
            suffixes.add(blockingKey);
        } else {
            int limit = blockingKey.length() - minimumLength + 1;
            for (int i = 0; i < limit; i++) {
                suffixes.add(blockingKey.substring(i));
            }
        }
        return suffixes;
    }
}