package org.scify.jedai.datamodel;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author GAP2
 */

public class AttributeProfile implements Serializable {

    private static final long serialVersionUID = 12235658453243447L;
    
    private int index;
    private int entityFrequency;
    
    private final String name;
    private final List<String> values;
    
    public AttributeProfile (String n) {
        name = n;
        values = new ArrayList<>();
    }

    public int getFrequency() {
        return entityFrequency;
    }
    
    public int getIndex() {
        return index;
    }
    
    public String getName() {
        return name;
    }

    public List<String> getValues() {
        return values;
    }
    
    public void setFrequency(int fr) {
        entityFrequency = fr;
    }
    
    public void setIndex(int id) {
        index = id;
    }
}