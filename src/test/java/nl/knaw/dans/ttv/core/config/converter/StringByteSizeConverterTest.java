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
package nl.knaw.dans.ttv.core.config.converter;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class StringByteSizeConverterTest {

    @Test
    void testNoSuffix() {
        var value = new StringByteSizeConverter().convert("1024");
        assertEquals(1024L, value);
    }

    @Test
    void testKilobytes() {
        var value = new StringByteSizeConverter().convert("1024k");
        assertEquals(1024L * 1024L, value);
    }

    @Test
    void testMegabytes() {
        var value = new StringByteSizeConverter().convert("1024M");
        assertEquals(1024L * 1024L * 1024L, value);
    }

    @Test
    void testGigabytes() {
        var value = new StringByteSizeConverter().convert("1024G");
        assertEquals(1024L * 1024L * 1024L * 1024L, value);
    }

    @Test
    void testSpacesDontMatter() {
        var value = new StringByteSizeConverter().convert("   1024G    ");
        assertEquals(1024L * 1024L * 1024L * 1024L, value);
    }

    @Test
    void testInvalidSuffix() {
        assertThrows(IllegalArgumentException.class, () -> {
            new StringByteSizeConverter().convert("1024X");
        });
    }

    @Test
    void testMultipleSuffix() {
        assertThrows(IllegalArgumentException.class, () -> {
            new StringByteSizeConverter().convert("1024GGGG");
        });
    }

    @Test
    void testInvalidNumber() {
        assertThrows(NumberFormatException.class, () -> {
            new StringByteSizeConverter().convert("ABC");
        });
    }
}
