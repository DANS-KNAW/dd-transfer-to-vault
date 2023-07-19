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

import lombok.Builder;
import lombok.Value;
import nl.knaw.dans.ttv.core.oaiore.vocabulary.DVCitation;
import nl.knaw.dans.ttv.core.oaiore.vocabulary.DansDVMetadata;
import nl.knaw.dans.ttv.core.oaiore.vocabulary.ORE;
import org.apache.commons.lang3.StringUtils;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.vocabulary.RDF;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;

public class OaiOreMetadataReader {

    public Metadata readMetadata(String json) {
        var model = ModelFactory.createDefaultModel();
        model.read(new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)), null, "JSON-LD");

        var builder = Metadata.builder();
        var aggregations = model.listStatements(null, RDF.type, ORE.Aggregation);

        if (aggregations.hasNext()) {
            var resource = aggregations.next().getSubject();

            builder.nbn(getRDFProperty(resource, DansDVMetadata.dansNbn));
            builder.pidVersion(getRDFProperty(resource, DansDVMetadata.dansDataversePidVersion));
            builder.bagId(getRDFProperty(resource, DansDVMetadata.dansBagId));
            builder.otherId(getEmbeddedRDFProperty(resource, DVCitation.otherId, DVCitation.otherIdValue));
            builder.otherIdVersion(getRDFProperty(resource, DansDVMetadata.dansOtherIdVersion));
            builder.dataSupplier(getRDFProperty(resource, DansDVMetadata.dansDataSupplier));
            builder.swordToken(getRDFProperty(resource, DansDVMetadata.dansSwordToken));

            var pid = getRDFProperty(resource, DansDVMetadata.dansDataversePid);

            if (pid == null) {
                pid = getRDFProperty(resource, DansDVMetadata.dansOtherId);
            }

            builder.pid(pid);
        }

        return builder.build();
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

        if (results.size() == 0) {
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
            results.add(item.getObject().asLiteral().getString());
        });

        if (results.size() == 0) {
            return null;
        }

        // ensure we get deterministic results
        var list = new ArrayList<>(results);
        list.sort(String::compareTo);

        return StringUtils.join(list, "; ");
    }

    @Value
    @Builder
    public static class Metadata {
        String nbn;
        String pid;
        String pidVersion;
        String bagId;
        String otherId;
        String otherIdVersion;
        String dataSupplier;
        String swordToken;
    }
}
