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
package nl.knaw.dans.ttv.core.oaiore;

import nl.knaw.dans.ttv.core.domain.FileContentAttributes;
import nl.knaw.dans.ttv.core.oaiore.vocabulary.DataverseCitationMetadata;
import nl.knaw.dans.ttv.core.oaiore.vocabulary.DansDataVaultMetadata;
import nl.knaw.dans.ttv.core.oaiore.vocabulary.OaiOreMetadata;
import org.apache.commons.lang3.StringUtils;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.sparql.vocabulary.FOAF;
import org.apache.jena.vocabulary.DCTerms;
import org.apache.jena.vocabulary.RDF;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;

public class OaiOreMetadataReader {

    public FileContentAttributes readMetadata(String json) {
        var model = ModelFactory.createDefaultModel();
        model.read(new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)), null, "JSON-LD");

        var builder = FileContentAttributes.builder();
        var aggregations = model.listStatements(null, RDF.type, OaiOreMetadata.Aggregation);

        if (aggregations.hasNext()) {
            var resource = aggregations.next().getSubject();

            builder.bagId(getRDFProperty(resource, DansDataVaultMetadata.dansBagId));
            builder.nbn(getRDFProperty(resource, DansDataVaultMetadata.dansNbn));
            builder.swordToken(getRDFProperty(resource, DansDataVaultMetadata.dansSwordToken));
            builder.dataSupplier(getRDFProperty(resource, DansDataVaultMetadata.dansDataSupplier));
            builder.dataversePid(getRDFProperty(resource, DansDataVaultMetadata.dansDataversePid));
            builder.dataversePidVersion(getRDFProperty(resource, DansDataVaultMetadata.dansDataversePidVersion));
            builder.otherId(getEmbeddedRDFProperty(resource, DataverseCitationMetadata.otherId, DataverseCitationMetadata.otherIdValue));
            builder.otherIdVersion(getRDFProperty(resource, DansDataVaultMetadata.dansOtherIdVersion));
            builder.title(getRDFProperty(resource, DCTerms.title));
        }

        var resourceMap = model.listStatements(null, RDF.type, OaiOreMetadata.ResourceMap);

        if (resourceMap.hasNext()) {
            var resource = resourceMap.next().getSubject();
            // TODO this is not used, instead the value in config.yml is used as the datastation
            // verify how this should work
            var creator = getRDFProperty(resource, DCTerms.creator);

            if (creator == null) {
                creator = getEmbeddedRDFProperty(resource, DCTerms.creator, FOAF.name);
            }

            builder.datastation(creator);
        }

        return builder.build();
    }

    private String swordTokenToUrnUuid(String swordToken) {
        if (StringUtils.isBlank(swordToken)) {
            return null;
        }

        var parts = swordToken.split(":");

        if (parts.length != 2) {
            return null;
        }

        return "urn:uuid:" + parts[1];
    }

    private String getEmbeddedRDFProperty(Resource resource, Property parent, Property child) {
        var results = new HashSet<String>();

        resource.listProperties(parent)
            .forEachRemaining(item -> {
                var value = getRDFProperty(item.getObject().asResource(), child);

                if (value != null) {
                    results.add(value);
                }
            });

        if (results.isEmpty()) {
            return null;
        }

        // ensure we get deterministic results
        var list = new ArrayList<>(results);
        list.sort(String::compareTo);

        return StringUtils.join(list, "; ");
    }

    private String getRDFProperty(Resource resource, Property name) {
        var results = new HashSet<String>();

        resource.listProperties(name).forEachRemaining(item -> {
            if (item.getObject().isLiteral()) {
                results.add(item.getObject().asLiteral().getString());
            }
        });

        if (results.isEmpty()) {
            return null;
        }

        // ensure we get deterministic results
        var list = new ArrayList<>(results);
        list.sort(String::compareTo);

        return StringUtils.join(list, "; ");
    }

}
