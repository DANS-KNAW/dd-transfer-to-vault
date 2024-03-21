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
package nl.knaw.dans.ttv.core.service;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class DveFileNameTest {

    @Test
    public void should_recognize_dataverse_dve_name_with_ordernumber() {
        var dveFileName = new DveFileName("123-doi-10-1234-5678-abcdev1.2.zip");
        assertThat(dveFileName.getOrderNumber()).isEqualTo(123L);
        assertThat(dveFileName.getDoi()).isEqualTo("doi-10-1234-5678-abcde");
        assertThat(dveFileName.getMajor()).isEqualTo(1);
        assertThat(dveFileName.getMinor()).isEqualTo(2);
        assertThat(dveFileName.getUuid()).isNull();
        assertThat(dveFileName.getOcflObjectVersionNr()).isNull();
    }

    @Test
    public void should_recognize_dataverse_dve_name_without_ordernumber() {
        var dveFileName = new DveFileName("doi-10-1234-5678-abcdev1.2.zip");
        assertThat(dveFileName.getOrderNumber()).isNull();
        assertThat(dveFileName.getDoi()).isEqualTo("doi-10-1234-5678-abcde");
        assertThat(dveFileName.getMajor()).isEqualTo(1);
        assertThat(dveFileName.getMinor()).isEqualTo(2);
        assertThat(dveFileName.getUuid()).isNull();
        assertThat(dveFileName.getOcflObjectVersionNr()).isNull();
    }

    @Test
    public void should_recognize_vaas_dve_name_with_ordernumber() {
        var dveFileName = new DveFileName("123-vaas-12345678-1234-1234-1234-1234567890ab-v1.zip");
        assertThat(dveFileName.getOrderNumber()).isEqualTo(123L);
        assertThat(dveFileName.getDoi()).isNull();
        assertThat(dveFileName.getMajor()).isNull();
        assertThat(dveFileName.getMinor()).isNull();
        assertThat(dveFileName.getUuid()).isEqualTo("12345678-1234-1234-1234-1234567890ab");
        assertThat(dveFileName.getOcflObjectVersionNr()).isEqualTo(1);
    }

    @Test
    public void should_recognize_vaas_dve_name_without_ordernumber() {
        var dveFileName = new DveFileName("vaas-12345678-1234-1234-1234-1234567890ab-v1.zip");
        assertThat(dveFileName.getOrderNumber()).isNull();
        assertThat(dveFileName.getDoi()).isNull();
        assertThat(dveFileName.getMajor()).isNull();
        assertThat(dveFileName.getMinor()).isNull();
        assertThat(dveFileName.getUuid()).isEqualTo("12345678-1234-1234-1234-1234567890ab");
        assertThat(dveFileName.getOcflObjectVersionNr()).isEqualTo(1);
    }
}
