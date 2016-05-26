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

package DataReader.GroundTruthReader;

import DataModel.IdDuplicates;
import DataReader.AbstractReader;
import java.util.Set;

/**
 *
 * @author G.A.P. II
 */
public class SerializationReader extends AbstractReader implements IGroundTruthReader {
    
    public SerializationReader(String filePath) {
        super(filePath);
    }

    @Override
    public Set<IdDuplicates> getEntityProfiles() {
        return (Set<IdDuplicates>) loadSerializedObject(inputFilePath);
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
