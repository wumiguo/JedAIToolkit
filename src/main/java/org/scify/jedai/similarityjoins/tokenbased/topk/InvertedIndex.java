/*
* Copyright [2016-2020] [George Papadakis (gpapadis@yahoo.gr)]
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

package org.scify.jedai.similarityjoins.tokenbased.topk;

import java.util.HashMap;

public class InvertedIndex {

    private HashMap<Integer, ListHead> invlist = new HashMap<>();

    public void add(int key, int[] value) {
        ListHead ilist = invlist.getOrDefault(key, new ListHead());
        ilist.add(value);
        if (ilist.getInvlist().size() == 1)
            invlist.put(key, ilist);
    }

    public ListHead getHeader(int key) {
        return invlist.getOrDefault(key, new ListHead());
    }
}