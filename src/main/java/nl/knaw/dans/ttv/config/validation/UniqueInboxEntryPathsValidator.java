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
package nl.knaw.dans.ttv.config.validation;

import nl.knaw.dans.ttv.config.CollectConfig;
import nl.knaw.dans.ttv.config.CollectConfig.InboxEntry;

import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;
import java.util.List;
import java.util.stream.Collectors;

public class UniqueInboxEntryPathsValidator implements ConstraintValidator<UniqueInboxEntryPaths, List<InboxEntry>> {
    @Override
    public boolean isValid(List<InboxEntry> inboxEntries, ConstraintValidatorContext constraintValidatorContext) {
        var inboxPaths = inboxEntries.stream()
            .collect(Collectors.groupingBy(CollectConfig.InboxEntry::getPath))
            .values()
            .stream().filter(entries -> entries.size() > 1)
            .collect(Collectors.toList());

        return inboxPaths.size() <= 0;
    }
}
