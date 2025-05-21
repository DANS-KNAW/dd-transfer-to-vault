DESCRIPTION
===========

Overview
--------

This service is responsible for taking dataset version exports (DVE for short), cataloging them and transferring them to the DANS data vault. If the dataset
version export is the first version of a dataset, an [NBN persistent identifier]{:target=_blank} is minted for the dataset and registered in the NBN database.
For more information about the context of this service, see the [Data Station architecture overview]{:target="_blank"}.

[Data Station architecture overview]: {{ data_station_architecture_overview }}
[NBN persistent identifier]: {{ nbn_url }}

Interfaces
----------

![Interfaces](img/overview.png)

### Provided

#### Inbox

* _Protocol type_: Shared filesystem
* _Internal or external_: **internal**
* _Purpose_: to receive DVEs from the Data Stations and other services

#### Admin console

* _Protocol type_: HTTP
* _Internal or external_: **internal**
* _Purpose_: application monitoring and management

### Consumed

#### Data Vault Catalog

* _Protocol type_: HTTP
* _Internal or external_: **internal**
* _Purpose_: to maintain information about the datasets and their versions that are stored in the DANS data vault

#### NBN Database

* _Protocol type_: HTTP
* _Internal or external_: **external**
* _Purpose_: to mint and register NBN persistent identifiers for datasets

#### Data Vault import inbox

* _Protocol type_: Shared filesystem
* _Internal or external_: **internal**
* _Purpose_: to import DVEs into the DANS data vault

###### Data Vault API

* _Protocol type_: HTTP
* _Internal or external_: **internal**
* _Purpose_: to issue commands to the DANS data vault and retrieve information from it

Processing
----------
This service is best viewed as a processing pipeline for DVEs. It connects a source that produces DVEs to a target DANS
[Data Vault Storage Root]{:target=_blank}, which stores the DVEs as OCFL object versions. A source can be a Data Station or a Vault as a Service client. The
service takes care of cataloging the DVEs and ensuring that the dataset is resolvable via the NBN persistent identifier. It attempts to do this in an efficient
way, by processing multiple DVEs in parallel, while ensuring that the order of the dataset version exports is preserved. Furthermore, the service will attempt
to resume processing of DVEs that were left unfinished in the event of a crash or restart.

[Data Vault Storage Root]: {{ data_vault_storage_root }}

### Inbox

The inbox is a directory into which DVEs are dropped. When a DVE is detected the inbox will determine what the NBN of the target dataset is. DVEs for the same
dataset version are processed in order, but DVEs for different dataset versions can be processed in parallel, except for the transfer to the vault (see below).

### Validation

The first step in the processing pipeline is to validate the DVE. Currently, the only layout that is supported is the [bagpack] layout. If the DVE is not a
bagpack, it will be rejected. Any other DVEs for the same dataset version will be blocked from processing until the problem is resolved.

### Metadata extraction

The next step is to extract the metadata from the DVE and to create or update the dataset version in the DANS data vault catalog. The main source of metadata is
the `metadata/oai-ore.jsonld` file in the DVE.

### NBN registration

After the Vault Catalog has been updated, the NBN persistent identifier is minted and scheduled for registration in the NBN database. This is done in a separate
background thread which uses a database table as a queue, so that the registration can be retried in case of a restart or crash.

### Transfer to vault and layer management

Finally, the DVE is extracted to the current DANS data vault import inbox batch for this instance of `dd-transfer-to-vault`. If the size of the batch exceeds a
configured threshold, the service will do two things:

1. Check if a new layer is needed. If so, it will issue a command to the DANS data vault to create a new layer.
2. Issue a command to the DANS data vault to import the current batch of DVEs.

This step is executed on a single dedicated thread, so that determining the size of the batch can be done reliably. The import command will return a tracking
URL which can be used to monitor the progress of the import. A confirmation of archiving task is scheduled to check the status of the import and to confirm that
the DVE has been archived in the DANS data vault.

### Confirmation of archiving

This step is also performed in a separate background thread, similar to the NBN registration. When `dd-data-vault` confirms that the DVE has been archived, the
Vault Catalog is updated to mark the dataset version as archived.







