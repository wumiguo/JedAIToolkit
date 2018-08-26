/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.scify.jedai.configuration;

/**
 *
 * @author gap2
 */
public interface IConfiguration {
    
    public int getNumberOfGridConfigurations();

    public void setNextRandomConfiguration();

    public void setNumberedGridConfiguration(int iterationNumber);

    public void setNumberedRandomConfiguration(int iterationNumber);
    
}
