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
package nl.knaw.dans.ttv.core.oaiore.vocabulary;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;

public class DansDVMetadata {
    public static final String NS = "https://dar.dans.knaw.nl/schema/dansDataVaultMetadata#";
    private static final Model m = ModelFactory.createDefaultModel();
    public static final Property dansDataversePid = m.createProperty(NS, "dansDataversePid");
    public static final Property dansDataversePidVersion = m.createProperty(NS, "dansDataversePidVersion");
    public static final Property dansBagId = m.createProperty(NS, "dansBagId");
    public static final Property dansNbn = m.createProperty(NS, "dansNbn");
    public static final Property dansOtherId = m.createProperty(NS, "dansOtherId");
    public static final Property dansOtherIdVersion = m.createProperty(NS, "dansOtherIdVersion");
    public static final Property dansSwordToken = m.createProperty(NS, "dansSwordToken");
    public static final Property dansDataSupplier = m.createProperty(NS, "dansDataSupplier");
}
