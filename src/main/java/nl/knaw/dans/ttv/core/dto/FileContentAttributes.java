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
package nl.knaw.dans.ttv.core.dto;

public class FileContentAttributes {

    private String datasetVersion;
    private String bagId;
    private String nbn;
    private byte[] oaiOre;
    private byte[] pidMapping;

    public String getDatasetVersion() {
        return datasetVersion;
    }

    public void setDatasetVersion(String datasetVersion) {
        this.datasetVersion = datasetVersion;
    }

    public String getBagId() {
        return bagId;
    }

    public void setBagId(String bagId) {
        this.bagId = bagId;
    }

    public String getNbn() {
        return nbn;
    }

    public void setNbn(String nbn) {
        this.nbn = nbn;
    }

    public byte[] getOaiOre() {
        return oaiOre;
    }

    public void setOaiOre(byte[] oaiOre) {
        this.oaiOre = oaiOre;
    }

    public byte[] getPidMapping() {
        return pidMapping;
    }

    public void setPidMapping(byte[] pidMapping) {
        this.pidMapping = pidMapping;
    }

    @Override
    public String toString() {
        return "FileContentAttributes{" +
            "datasetVersion='" + datasetVersion + '\'' +
            ", bagId='" + bagId + '\'' +
            ", nbn='" + nbn + '\'' +
            '}';
    }
}
