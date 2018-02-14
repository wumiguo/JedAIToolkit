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

package BlockProcessing.BlockCleaning;

import DataModel.AbstractBlock;
import DataModel.BilateralBlock;
import DataModel.UnilateralBlock;

import com.esotericsoftware.minlog.Log;

import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;

import java.util.List;

import org.apache.jena.atlas.json.JsonArray;
import org.apache.jena.atlas.json.JsonObject;

/**
 *
 * @author gap2
 */

public class SizeBasedBlockPurging extends AbstractBlockPurging {
    
    private boolean isCleanCleanER;
    private double purgingFactor;
    private double maxEntities;
    
    public SizeBasedBlockPurging() {
        this(0.005);
    }

    public SizeBasedBlockPurging(double pf) {
        purgingFactor = pf;
    }
    
    private int getMaxBlockSize(List<AbstractBlock> blocks) {
        final TIntSet entities = new TIntHashSet();
        for (AbstractBlock aBlock : blocks) {
            UnilateralBlock uBlock = (UnilateralBlock) aBlock;
            entities.addAll(uBlock.getEntities());
        }
        
        return (int) Math.round(entities.size()*purgingFactor);
    }
    
    private int getMaxInnerBlockSize(List<AbstractBlock> blocks) {
        final TIntSet d1Entities = new TIntHashSet();
        final TIntSet d2Entities = new TIntHashSet();
        for (AbstractBlock aBlock : blocks) {
            BilateralBlock bBlock = (BilateralBlock) aBlock;
            d1Entities.addAll(bBlock.getIndex1Entities());
            d2Entities.addAll(bBlock.getIndex2Entities());
        }
        
        return (int) Math.round(Math.min(d1Entities.size(), d2Entities.size())*purgingFactor);
    }
    
    @Override
    public String getMethodConfiguration() {
        return getParameterName(0) + "=" + purgingFactor;
    }
    
    @Override
    public String getMethodInfo() {
        return getMethodName() + ": it discards the blocks exceeding a certain number of entities.";
    }
    
    @Override
    public String getMethodName() {
        return "Size-based Block Purging";
    }

    @Override
    public String getMethodParameters() {
        return  getMethodName() + " involves a single parameter:\n"
                + "1)" + getParameterDescription(0) + ".\n";
    }

    @Override
    public JsonArray getParameterConfiguration() {
        final JsonObject obj = new JsonObject();
        obj.put("class", "java.lang.Double");
        obj.put("name", getParameterName(0));
        obj.put("defaultValue", "0.005");
        obj.put("minValue", "0.001");
        obj.put("maxValue", "0.100");
        obj.put("stepValue", "0.001");
        obj.put("description", getParameterDescription(0));
        
        final JsonArray array = new JsonArray();
        array.add(obj);
        
        return array;
    }
    
    @Override
    public String getParameterDescription(int parameterId) {
        switch(parameterId) {
            case 0:
                return "The " + getParameterName(0) + " determines indirectly the maximum number of entities per block.";
            default:
                return "invalid parameter id";
        }
    }
    
    @Override
    public String getParameterName(int parameterId) {
        switch(parameterId) {
            case 0:
                return "Purging Factor";
            default:
                return "invalid parameter id";
        }
    }
    
    @Override
    protected boolean satisfiesThreshold(AbstractBlock block) {
        if (isCleanCleanER) {
            BilateralBlock bBlock = (BilateralBlock) block;
            return Math.min(bBlock.getIndex1Entities().length, bBlock.getIndex2Entities().length) <= maxEntities;
        } 
        return block.getTotalBlockAssignments() <= maxEntities;
    }

    @Override
    protected void setThreshold(List<AbstractBlock> blocks) {
        if (blocks.get(0) instanceof UnilateralBlock) {
            isCleanCleanER = false;
            maxEntities = getMaxBlockSize(blocks);
            Log.info("Maximum entities per block\t:\t"+ maxEntities);
        } else {
            isCleanCleanER = true;
            maxEntities = getMaxInnerBlockSize(blocks);
            Log.info("Maximum inner block size per block\t:\t" + maxEntities);
        }
    }
}