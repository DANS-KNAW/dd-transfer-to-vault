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
package nl.knaw.dans.transfer.core.oaiore;

import nl.knaw.dans.transfer.core.DveMetadata;
import nl.knaw.dans.transfer.core.oaiore.vocabulary.DansDataVaultMetadata;
import nl.knaw.dans.transfer.core.oaiore.vocabulary.DvCore;
import nl.knaw.dans.transfer.core.oaiore.vocabulary.OaiOreMetadata;
import nl.knaw.dans.transfer.core.oaiore.vocabulary.Schema;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.vocabulary.DCTerms;
import org.apache.jena.vocabulary.RDF;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;

public class OaiOreMetadataReader {

    public DveMetadata readMetadata(String json) {
        var model = ModelFactory.createDefaultModel();
        model.read(new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)), null, "JSON-LD");

        var builder = DveMetadata.builder();
        var aggregations = model.listStatements(null, RDF.type, OaiOreMetadata.Aggregation);

        if (aggregations.hasNext()) {
            var theAggregation = aggregations.next().getSubject();

            builder.bagId(getSingleValueProperty(theAggregation, DansDataVaultMetadata.dansBagId));
            builder.nbn(getSingleValueProperty(theAggregation, DansDataVaultMetadata.dansNbn));
            builder.swordToken(getSingleValueProperty(theAggregation, DansDataVaultMetadata.dansSwordToken));
            builder.dataSupplier(getSingleValueProperty(theAggregation, DansDataVaultMetadata.dansDataSupplier));
            builder.dataversePid(getSingleValueProperty(theAggregation, DansDataVaultMetadata.dansDataversePid));
            builder.dataversePidVersion(getSingleValueProperty(theAggregation, DansDataVaultMetadata.dansDataversePidVersion));
            builder.otherId(getSingleValueProperty(theAggregation, DansDataVaultMetadata.dansOtherId));
            builder.otherIdVersion(getSingleValueProperty(theAggregation, DansDataVaultMetadata.dansOtherIdVersion));
            builder.title(getSingleValueProperty(theAggregation, DCTerms.title));
            builder.metadata(json);
        }

        var resourceMaps = model.listStatements(null, RDF.type, OaiOreMetadata.ResourceMap);

        if (resourceMaps.hasNext()) {
            var theResouceMap = resourceMaps.next().getSubject();
            builder.exporter(getEmbeddedSingleValueProperty(theResouceMap, DvCore.generatedBy, Schema.name));
            builder.exporterVersion(getEmbeddedSingleValueProperty(theResouceMap, DvCore.generatedBy, Schema.version));
        }

        return builder.build();
    }

    private String getSingleValueProperty(Resource resource, Property name) {
        var results = new HashSet<String>();

        resource.listProperties(name).forEachRemaining(item -> {
            if (item.getObject().isLiteral()) {
                results.add(item.getObject().asLiteral().getString());
            }
        });

        if (results.isEmpty()) {
            return null;
        }
        else if (results.size() > 1) {
            throw new IllegalArgumentException("Expected a single value for property " + name + ", but found: " + results);
        }

        return results.iterator().next();
    }

    private String getEmbeddedSingleValueProperty(Resource resource, Property parent, Property child) {
        var results = new HashSet<String>();

        resource.listProperties(parent)
            .forEachRemaining(item -> {
                var value = getSingleValueProperty(item.getObject().asResource(), child);
                if (value != null) {
                    results.add(value);
                }
            });

        if (results.isEmpty()) {
            return null;
        }
        else if (results.size() > 1) {
            throw new IllegalArgumentException("Expected a single value for property " + parent + ", but found: " + results);
        }

        return results.iterator().next();
    }

}
