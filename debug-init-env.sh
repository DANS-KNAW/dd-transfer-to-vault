#!/usr/bin/env bash
#
# Copyright (C) 2025 DANS - Data Archiving and Networked Services (info@dans.knaw.nl)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

echo -n "Pre-creating log..."
TEMPDIR=data
TRANSFER_INBOX=$TEMPDIR/01_transfer-inbox
EXTRACT_METADATA=$TEMPDIR/02_extract-metadata
SEND_TO_VAULT=$TEMPDIR/03_send-to-vault
DD_REGISTER_NBN=$TEMPDIR/dd-register-nbn
DATA_VAULT=$TEMPDIR/04_data-vault
touch $TEMPDIR/dd-transfer-to-vault.log
echo "OK"
echo -n "Creating working directories..."
mkdir -p $TRANSFER_INBOX/inbox
mkdir -p $TRANSFER_INBOX/failed
mkdir -p $EXTRACT_METADATA/inbox
mkdir -p $EXTRACT_METADATA/outbox/rejected
mkdir -p $EXTRACT_METADATA/outbox/failed
mkdir -p $DD_REGISTER_NBN/inbox
mkdir -p $DD_REGISTER_NBN/processed
mkdir -p $DD_REGISTER_NBN/failed
mkdir -p $SEND_TO_VAULT/inbox
mkdir -p $SEND_TO_VAULT/work
mkdir -p $SEND_TO_VAULT/outbox/failed
mkdir -p $SEND_TO_VAULT/outbox/processed
mkdir -p $DATA_VAULT/inbox
echo "OK"

