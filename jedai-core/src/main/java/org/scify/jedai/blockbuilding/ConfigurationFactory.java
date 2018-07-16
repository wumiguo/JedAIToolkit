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

import java.util.ArrayList;
import java.util.List;
import org.scify.jedai.datamodel.ConfigurationSetting;
import org.scify.jedai.utilities.enumerations.BlockBuildingMethod;

/**
 *
 * @author GAP2
 */
public class ConfigurationFactory {

    private static ConfigurationSetting getExtendedQGramsDefaultConfiguration() {
        final ConfigurationSetting currentCS = new ConfigurationSetting(0, 1, 0, 1);
        currentCS.setDoubleParameter(0, 0.95); // threshold
        currentCS.setIntegerParameter(0, 6); // q
        return currentCS;
    }

    private static List<ConfigurationSetting> getExtendedQGramsConfigurations() {
        final List<ConfigurationSetting> configurations = new ArrayList<>();
        for (int q = 2; q <= 6; q++) {
            for (double t = 0.8; t <= 0.95; t += 0.05) {
                ConfigurationSetting currentCS = new ConfigurationSetting(0, 1, 0, 1);
                currentCS.setDoubleParameter(0, t);
                currentCS.setIntegerParameter(0, q);
                configurations.add(currentCS);
            }
        }
        return configurations;
    }
    
    private static ConfigurationSetting getExtendedSortedNeighborhoodDefaultConfiguration() {
        final ConfigurationSetting currentCS = new ConfigurationSetting(0, 0, 0, 1);
        currentCS.setIntegerParameter(0, 2); // window size
        return currentCS;
    }
    
    private static List<ConfigurationSetting> getExtendedSortedNeighborhoodConfigurations() {
        final List<ConfigurationSetting> configurations = new ArrayList<>();
        for (int w = 2; w <= 10; w++) {
            ConfigurationSetting currentCS = new ConfigurationSetting(0, 0, 0, 1);
            currentCS.setIntegerParameter(0, w); // window size
            configurations.add(currentCS);
        }
        return configurations;
    }
    
    private static ConfigurationSetting getExtendedSuffixArraysDefaultConfiguration() {
        final ConfigurationSetting currentCS = new ConfigurationSetting(0, 0, 0, 2);
        currentCS.setIntegerParameter(0, 39); // maximumBlockSize
        currentCS.setIntegerParameter(1, 6); // minimumSuffixLength
        return currentCS;
    }

    private static List<ConfigurationSetting> getExtendedSuffixArraysConfigurations() {
        final List<ConfigurationSetting> configurations = new ArrayList<>();
        for (int maxBS = 2; maxBS <= 100; maxBS++) {
            for (int minSL = 2; minSL <= 6; minSL++) {
                ConfigurationSetting currentCS = new ConfigurationSetting(0, 0, 0, 1);
                currentCS.setIntegerParameter(0, maxBS); // maximumBlockSize
                currentCS.setIntegerParameter(1, minSL); // minimumSuffixLength
                configurations.add(currentCS);
            }
        }
        return configurations;
    }

    private static ConfigurationSetting getQGramsDefaultConfiguration() {
        final ConfigurationSetting currentCS = new ConfigurationSetting(0, 0, 0, 1);
        currentCS.setIntegerParameter(0, 6); // q-gram size
        return currentCS;
    }

    private static List<ConfigurationSetting> getQGramsConfigurations() {
        final List<ConfigurationSetting> configurations = new ArrayList<>();
        for (int q = 2; q <= 6; q++) {
            ConfigurationSetting currentCS = new ConfigurationSetting(0, 0, 0, 1);
            currentCS.setIntegerParameter(0, q); // q-gram size
            configurations.add(currentCS);
        }
        return configurations;
    }

    private static ConfigurationSetting getSortedNeighborhoodDefaultConfiguration() {
        final ConfigurationSetting currentCS = new ConfigurationSetting(0, 0, 0, 1);
        currentCS.setIntegerParameter(0, 4); // window size
        return currentCS;
    }
    
    private static List<ConfigurationSetting> getSortedNeighborhoodConfigurations() {
        final List<ConfigurationSetting> configurations = new ArrayList<>();
        for (int w = 2; w <= 100; w++) {
            ConfigurationSetting currentCS = new ConfigurationSetting(0, 0, 0, 1);
            currentCS.setIntegerParameter(0, w); // window size
            configurations.add(currentCS);
        }
        return configurations;
    }
    
    private static ConfigurationSetting getSuffixArraysDefaultConfiguration() {
        final ConfigurationSetting currentCS = new ConfigurationSetting(0, 0, 0, 2);
        currentCS.setIntegerParameter(0, 53); // maximumBlockSize
        currentCS.setIntegerParameter(1, 6); // minimumSuffixLength
        return currentCS;
    }

    private static List<ConfigurationSetting> getSuffixArraysConfigurations() {
        final List<ConfigurationSetting> configurations = new ArrayList<>();
        for (int maxBS = 2; maxBS <= 100; maxBS++) {
            for (int minSL = 2; minSL <= 6; minSL++) {
                ConfigurationSetting currentCS = new ConfigurationSetting(0, 0, 0, 2);
                currentCS.setIntegerParameter(0, maxBS); // maximumBlockSize
                currentCS.setIntegerParameter(1, minSL); // minimumSuffixLength
                configurations.add(currentCS);
            }
        }
        return configurations;
    }

    public static ConfigurationSetting getDefaultConfiguration(BlockBuildingMethod blbuMethod) {
        switch (blbuMethod) {
            case EXTENDED_Q_GRAMS_BLOCKING:
                return getExtendedQGramsDefaultConfiguration();
            case EXTENDED_SORTED_NEIGHBORHOOD:
                return getExtendedSortedNeighborhoodDefaultConfiguration();
            case EXTENDED_SUFFIX_ARRAYS:
                return getExtendedSuffixArraysDefaultConfiguration();
            case Q_GRAMS_BLOCKING:
                return getQGramsDefaultConfiguration();
            case SORTED_NEIGHBORHOOD:
                return getSortedNeighborhoodDefaultConfiguration();
            case SUFFIX_ARRAYS:
                return getSuffixArraysDefaultConfiguration();
            case STANDARD_BLOCKING:
            default:
                return new ConfigurationSetting(0, 0, 0, 0);
        }
    }

    public static List<ConfigurationSetting> getReasonableConfigurations(BlockBuildingMethod blbuMethod) {
        switch (blbuMethod) {
            case EXTENDED_Q_GRAMS_BLOCKING:
                return getExtendedQGramsConfigurations();
            case EXTENDED_SORTED_NEIGHBORHOOD:
                return getExtendedSortedNeighborhoodConfigurations();
            case EXTENDED_SUFFIX_ARRAYS:
                return getExtendedSuffixArraysConfigurations();
            case Q_GRAMS_BLOCKING:
                return getQGramsConfigurations();
            case SORTED_NEIGHBORHOOD:
                return getSortedNeighborhoodConfigurations();
            case SUFFIX_ARRAYS:
                return getSuffixArraysConfigurations();
            case STANDARD_BLOCKING:
            default:
                return new ArrayList<>();
        }
    }
}
