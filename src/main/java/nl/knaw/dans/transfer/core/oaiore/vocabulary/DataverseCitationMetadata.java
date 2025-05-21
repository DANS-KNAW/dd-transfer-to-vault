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
package nl.knaw.dans.transfer.core.oaiore.vocabulary;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;

public class DataverseCitationMetadata {
    public static final String NS = "https://dataverse.org/schema/citation/";
    private static final Model m = ModelFactory.createDefaultModel();
    public static final Property author = m.createProperty(NS, "author");
    public static final Property authorName = m.createProperty(NS, "authorName");
    public static final Property authorAffiliation = m.createProperty(NS, "authorAffiliation");
    public static final Property otherId = m.createProperty(NS, "otherId");
    public static final Property otherIdValue = m.createProperty(NS, "otherIdValue");
    public static final Property otherIdAgency = m.createProperty(NS, "otherIdAgency");

    public static final Property datasetContact = m.createProperty(NS, "datasetContact");
    public static final Property datasetContactName = m.createProperty(NS, "datasetContactName");
    public static final Property datasetContactAffiliation = m.createProperty(NS, "datasetContactAffiliation");
    public static final Property datasetContactEmail = m.createProperty(NS, "datasetContactEmail");
    public static final Property dsDescription = m.createProperty(NS, "dsDescription");
    public static final Property dsDescriptionValue = m.createProperty(NS, "dsDescriptionValue");
    public static final Property dsDescriptionDate = m.createProperty(NS, "dsDescriptionDate");
    public static final Property keyword = m.createProperty(NS, "keyword");
    public static final Property keywordValue = m.createProperty(NS, "keywordValue");
    public static final Property keywordVocabulary = m.createProperty(NS, "keywordVocabulary");
    public static final Property keywordVocabularyURI = m.createProperty(NS, "keywordVocabularyURI");
    public static final Property productionDate = m.createProperty(NS, "productionDate");
    public static final Property contributor = m.createProperty(NS, "contributor");
    public static final Property contributorType = m.createProperty(NS, "contributorType");
    public static final Property contributorName = m.createProperty(NS, "contributorName");
    public static final Property grantNumberAgency = m.createProperty(NS, "grantNumberAgency");
    public static final Property grantNumberValue = m.createProperty(NS, "grantNumberValue");
    public static final Property distributor = m.createProperty(NS, "distributor");
    public static final Property distributorName = m.createProperty(NS, "distributorName");
    public static final Property distributionDate = m.createProperty(NS, "distributionDate");
    public static final Property dateOfCollection = m.createProperty(NS, "dateOfCollection");
    public static final Property dateOfCollectionStart = m.createProperty(NS, "dateOfCollectionStart");
    public static final Property dateOfCollectionEnd = m.createProperty(NS, "dateOfCollectionEnd");
    public static final Property series = m.createProperty(NS, "series");
    public static final Property seriesName = m.createProperty(NS, "seriesName");
    public static final Property seriesInformation = m.createProperty(NS, "seriesInformation");
}
