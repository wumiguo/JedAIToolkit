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

import gnu.trove.map.TObjectIntMap;
import org.scify.jedai.datamodel.AbstractBlock;
import org.scify.jedai.datamodel.EntityProfile;
import org.scify.jedai.utilities.IDocumentation;

import java.util.List;

/**
 *
 * @author G.A.P. II
 */
public interface IBlockBuilding extends IDocumentation {

    int DATASET_1 = 0;
    int DATASET_2 = 1;

    String CLUSTER_PREFIX = "#$!cl";
    
    public List<AbstractBlock> getBlocks(List<EntityProfile> profiles);

    public List<AbstractBlock> getBlocks(List<EntityProfile> profilesD1, List<EntityProfile> profilesD2);

    public List<AbstractBlock> getBlocks(List<EntityProfile> profilesD1, List<EntityProfile> profilesD2, TObjectIntMap<String>[] schemaClusters);
    
    public int getNumberOfGridConfigurations();

    public void setNextRandomConfiguration();

    public void setNumberedGridConfiguration(int iterationNumber);

    public void setNumberedRandomConfiguration(int iterationNumber);
}
