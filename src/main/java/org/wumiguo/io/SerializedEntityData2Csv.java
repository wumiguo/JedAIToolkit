package org.wumiguo.io;

import org.scify.jedai.datamodel.Attribute;
import org.scify.jedai.datamodel.EntityProfile;
import org.scify.jedai.datareader.entityreader.EntitySerializationReader;
import org.scify.jedai.datareader.entityreader.IEntityReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * Created by levinliu on 2020/6/5
 * GitHub: https://github.com/levinliu
 * (Change file header on Settings -> Editor -> File and Code Templates)
 */
public class SerializedEntityData2Csv {
    private static final Logger log = LoggerFactory.getLogger(SerializedEntityData2Csv.class);

    public static void serializedEntityProfileData2Csv(String serializedEntityProfileDataPath, String csvPath, boolean withLineNo) throws IOException {
        final IEntityReader entityReader = new EntitySerializationReader(serializedEntityProfileDataPath);
        final List<EntityProfile> profiles = entityReader.getEntityProfiles();
        if (profiles.isEmpty()) {
            throw new IllegalStateException("Empty entity profile");
        }
        Set<Attribute> attributeSet = profiles.get(0).getAttributes();
        int attrSize = attributeSet.size();
        final int length = withLineNo ? attrSize + 1 : attrSize;
        String[] headers = new String[length];
        Map<Integer, String> headerIndexMapping = new HashMap<>();
        int index = 0;
        if (withLineNo) {
            headers[index] = "LineNo";
            headerIndexMapping.put(index, "LineNo");
            index++;
        }
        for (Attribute attr : attributeSet) {
            headers[index] = attr.getName();
            headerIndexMapping.put(index, attr.getName());
            index++;
        }
        List<String[]> data = new LinkedList<>();
        int lineNo = 0;
        for (EntityProfile profile : profiles) {
            String[] values = new String[length];
            for (int i = 0; i < length; i++) {
                if (i == 0) {
                    values[i] = String.valueOf(lineNo);
                } else {
                    values[i] = getValue(headerIndexMapping.get(i), profile.getAttributes());
                }
            }
        }
        Data2Csv.data2csv(csvPath, headers, data);
    }

    private static String getValue(String headerName, Set<Attribute> attributes) {
        for (Attribute attr : attributes) {
            if (headerName.equals(attr.getName())) {
                return attr.getValue();
            }
        }
        log.warn("name {} out of attribute set {}", headerName, attributes);
        return "ValueNotFound-" + headerName;
    }

}
