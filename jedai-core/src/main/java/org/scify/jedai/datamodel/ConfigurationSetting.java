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
package org.scify.jedai.datamodel;

import java.util.Enumeration;

/**
 *
 * @author GAP2
 */
public class ConfigurationSetting {

    private final boolean[] bParameters;
    private final double[] dParameters;
    private final int[] intParameters;
    private final Enumeration[] eParameters;

    public ConfigurationSetting(int bParams, int dParams, int eParams, int iParams) {
        bParameters = 0 < bParams ? new boolean[bParams] : null;
        dParameters = 0 < dParams ? new double[dParams] : null;
        eParameters = 0 < eParams ? new Enumeration[eParams] : null;
        intParameters = 0 < iParams ? new int[iParams] : null;
    }

    public Boolean getBooleanParameter(int bIndex) {
        if (bParameters == null) {
            return null;
        }

        if (bParameters.length <= bIndex) {
            return null;
        }

        return bParameters[bIndex];
    }

    public Double getDoubleParameter(int dIndex) {
        if (dParameters == null) {
            return null;
        }

        if (dParameters.length <= dIndex) {
            return null;
        }

        return dParameters[dIndex];
    }

    public Enumeration getEnumerationParameter(int eIndex) {
        if (eParameters == null) {
            return null;
        }

        if (eParameters.length <= eIndex) {
            return null;
        }

        return eParameters[eIndex];
    }

    public Integer getIntegerParameter(int iIndex) {
        if (intParameters == null) {
            return null;
        }

        if (intParameters.length <= iIndex) {
            return null;
        }

        return intParameters[iIndex];
    }

    public int setBooleanParameter(int bIndex, boolean bValue) {
        if (bParameters == null) {
            return -1;
        }

        if (bParameters.length <= bIndex) {
            return -2;
        }

        bParameters[bIndex] = bValue;
        return 1;
    }

    public int setDoubleParameter(int dIndex, double dValue) {
        if (dParameters == null) {
            return -1;
        }

        if (dParameters.length <= dIndex) {
            return -2;
        }

        dParameters[dIndex] = dValue;
        return 1;
    }

    public int setEnumerationParameter(int eIndex, Enumeration eValue) {
        if (eParameters == null) {
            return -1;
        }

        if (eParameters.length <= eIndex) {
            return -2;
        }
        
        eParameters[eIndex] = eValue;
        return 1;
    }
    
    public int setIntegerParameter(int iIndex, int iValue) {
        if (intParameters == null) {
            return -1;
        }

        if (intParameters.length <= iIndex) {
            return -2;
        }

        intParameters[iIndex] = iValue;
        return 1;
    }
}
