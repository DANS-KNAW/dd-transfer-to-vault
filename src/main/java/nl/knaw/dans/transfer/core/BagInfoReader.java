/*
 * Copyright (C) 2025 DANS - Data Archiving and Networked Services (info@dans.knaw.nl)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nl.knaw.dans.transfer.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BagInfoReader {

    public Map<String, List<String>> readBagInfo(String bagInfoContent) {
        var bagInfoMap = new HashMap<String, List<String>>();
        var lines = bagInfoContent.split("\n");

        for (var line : lines) {
            var parts = line.split(":", 2);
            if (parts.length == 2) {
                var key = parts[0].trim();
                var value = parts[1].trim();

                bagInfoMap.computeIfAbsent(key, k -> new ArrayList<>()).add(value);
            }
        }

        return bagInfoMap;
    }
}
