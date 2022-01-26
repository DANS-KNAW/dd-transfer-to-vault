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

public class FilenameAttributes {

    private String dveFilePath;
    private String datasetPid;
    private int versionMajor;
    private int versionMinor;

    public String getDatasetPid() {
        return datasetPid;
    }

    public void setDatasetPid(String datasetPid) {
        this.datasetPid = datasetPid;
    }

    public int getVersionMajor() {
        return versionMajor;
    }

    public void setVersionMajor(int versionMajor) {
        this.versionMajor = versionMajor;
    }

    public int getVersionMinor() {
        return versionMinor;
    }

    public void setVersionMinor(int versionMinor) {
        this.versionMinor = versionMinor;
    }

    public String getDveFilePath() {
        return dveFilePath;
    }

    public void setDveFilePath(String dveFilePath) {
        this.dveFilePath = dveFilePath;
    }

    @Override
    public String toString() {
        return "FilenameAttributes{" +
            "dveFilePath='" + dveFilePath + '\'' +
            ", datasetPid='" + datasetPid + '\'' +
            ", versionMajor=" + versionMajor +
            ", versionMinor=" + versionMinor +
            '}';
    }
}
