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

package BlockProcessing;

import DataModel.AbstractBlock;

import com.esotericsoftware.minlog.Log;

import java.util.List;

/**
 *
 * @author G.A.P. II
 */

public abstract class AbstractBlockProcessing implements IBlockProcessing {
    
    protected void printOriginalStatistics(List<AbstractBlock> inputBlocks) {
        double comparisons = 0;
        for (AbstractBlock block : inputBlocks) {
            comparisons+= block.getNoOfComparisons();
        }
        
        Log.info("Original blocks\t:\t{0}" + inputBlocks.size());
        Log.info("Original comparisons\t:\t" + comparisons);
    }
}
