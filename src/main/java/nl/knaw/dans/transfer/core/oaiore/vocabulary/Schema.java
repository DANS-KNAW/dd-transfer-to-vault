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

public class Schema {
    public static final String NS = "http://schema.org/"; // Dataverse uses http instead of https, so we cannot use SchemaDO
    private static final Model m = ModelFactory.createDefaultModel();
    public static final org.apache.jena.rdf.model.Property name = m.createProperty(NS, "name");
    public static final org.apache.jena.rdf.model.Property version = m.createProperty(NS, "version");
}
