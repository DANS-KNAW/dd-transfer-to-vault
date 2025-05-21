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
import org.apache.jena.rdf.model.Resource;

public class OaiOreMetadata {
    public static final String NS = "http://www.openarchives.org/ore/terms/";
    private static final Model m = ModelFactory.createDefaultModel();
    public static final Property describes = m.createProperty(NS, "describes");
    public static final Resource AggregatedResource = m.createProperty(NS, "AggregatedResource");
    public static final Resource Aggregation = m.createProperty(NS, "Aggregation");
    public static final Resource ResourceMap = m.createProperty(NS, "ResourceMap");
    public static final Property aggregates = m.createProperty(NS, "aggregates");
}
