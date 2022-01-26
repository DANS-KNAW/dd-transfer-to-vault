/*
 * Copyright (C) 2021 DANS - Data Archiving and Networked Services (info@dans.knaw.nl)
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
package nl.knaw.dans.ttv.core.config;

import com.fasterxml.jackson.databind.util.StdConverter;

import java.util.Locale;
import java.util.regex.Pattern;

public class StringByteSizeConverter extends StdConverter<String, Long> {

    @Override
    public Long convert(String s) {
        var pattern = Pattern.compile("(\\d+)([a-zA-Z])?", Pattern.CASE_INSENSITIVE);
        var matched = pattern.matcher(s);

        if (matched.matches()) {
            var number = Long.parseLong(matched.group(1));
            var suffix = matched.group(2);

            if (suffix != null) {
                switch (suffix.toUpperCase(Locale.ROOT)) {
                    case "G":
                        number *= (1024 * 1024 * 1024);
                        break;

                    case "M":
                        number *= (1024 * 1024);
                        break;

                    case "K":
                        number *= (1024);
                        break;

                    default:
                        throw new IllegalArgumentException(String.format("suffix %s is not a valid size, expecting one of G, M, or K", suffix));
                }
            }

            return number;
        }

        throw new NumberFormatException(String.format("value %s cannot be converted to a number", s));
    }
}
