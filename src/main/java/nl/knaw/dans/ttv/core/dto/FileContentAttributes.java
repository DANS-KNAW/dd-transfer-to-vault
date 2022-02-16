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
    private String otherId;
    private String otherIdVersion;
    private String swordToken;
    private String bagChecksum;

    public FileContentAttributes(String datasetVersion, String bagId, String nbn, byte[] oaiOre, byte[] pidMapping, String otherId, String otherIdVersion, String swordToken, String bagChecksum) {
        this.datasetVersion = datasetVersion;
        this.bagId = bagId;
        this.nbn = nbn;
        this.oaiOre = oaiOre;
        this.pidMapping = pidMapping;
        this.otherId = otherId;
        this.otherIdVersion = otherIdVersion;
        this.swordToken = swordToken;
        this.bagChecksum = bagChecksum;
    }

    public FileContentAttributes() {
    }

    public String getOtherId() {
        return otherId;
    }

    public void setOtherId(String otherId) {
        this.otherId = otherId;
    }

    public String getOtherIdVersion() {
        return otherIdVersion;
    }

    public void setOtherIdVersion(String otherIdVersion) {
        this.otherIdVersion = otherIdVersion;
    }

    public String getSwordToken() {
        return swordToken;
    }

    public void setSwordToken(String swordToken) {
        this.swordToken = swordToken;
    }

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
            ", otherId='" + otherId + '\'' +
            ", otherIdVersion='" + otherIdVersion + '\'' +
            ", swordToken='" + swordToken + '\'' +
            ", bagChecksum='" + bagChecksum + '\'' +
            '}';
    }

    public String getBagChecksum() {
        return bagChecksum;
    }

    public void setBagChecksum(String bagChecksum) {
        this.bagChecksum = bagChecksum;
    }
}
