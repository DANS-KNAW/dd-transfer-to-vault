DESCRIPTION
===========

Overview
--------

This service is responsible for taking dataset version exports, cataloging them and transferring them to the DANS data vault. If the dataset version export is
the first version of a dataset, an NBN persistent identifier is minted for the dataset and registered in the NBN database. For more information about the
context of this service, see the [Data Station architecture overview]{:target="_blank"}.

[Data Station architecture overview]: https://dans-knaw.github.io/dans-datastation-architecture/datastation/

Interfaces
----------

![Interfaces](img/overview.png)

### Provided

#### Inbox directories

* _Protocol type_: Shared filesystem
* _Internal or external_: **internal**
* _Purpose_: to receive dataset version exports from the Data Stations and other services

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
* _Internal or external_: **internal**
* _Purpose_: to mint and register NBN persistent identifiers for datasets

#### Data Vault import inbox

* _Protocol type_: Shared filesystem
* _Internal or external_: **internal**
* _Purpose_: to import dataset version exports into the DANS data vault

###### Data Vault API

* _Protocol type_: HTTP
* _Internal or external_: **internal**
* _Purpose_: to issue commands to the DANS data vault and retrieve information from it

Processing
----------
This service is best viewed as a processing pipeline for dataset version exports. A dataset version export is a zip file containing the metadata and files of a
dataset version. It is created as a long term preservation copy of a dataset version. The processing pipeline consists of the following steps:

### COLLECT

The service monitors a number of configured inbox directories for new dataset version exports. The COLLECT step is responsible for detecting new exports and
creating a transfer items in the service's database.

### EXTRACT-METADATA

The metadata of a dataset version export is extracted. An NBN persistent identifier is minted for the dataset if it is the first version of a dataset and
registered in the NBN database. The metadata, including the NBN is registered in the Data Vault Catalog.

At this point, the dataset version export is ready to be transferred to the DANS data vault. It is moved the current import batch for the vault collection that
this dataset version export must be added to. The import batch is a directory on the shared filesystem that is used to transfer dataset version exports to the
DANS data vault. It has the following structure:

```text
import-batch/
    urn:nbn:nl:ui:13-4-abc/
        1/ 
           /unzipped contents of dataset-1-version-1.zip 
        2/
           /unzipped contents of dataset-1-version-2.zip
    urn:nbn:nl:ui:13-4-def/
        1/
           /unzipped contents of dataset-2-version-1.zip
```

Each directory in the batch is an object import directory, that is to say it targets a specific OCFL object in the DANS data vault. An OCFL object is the
container for the files and metadata of a dataset. The name of the directory is the URN:NBN of the dataset. Each subdirectory of an object import directory is
a version import directory. The name of the directory is an integer. The versions are imported in ascending order.

### SEND-TO-DATA-VAULT

Each time a dataset version export is moved to the import batch, the service checks if the total size of the import batch exceeds a configured threshold. If it
does, the service sends a command to the Data Vault API to start the import of the import batch (after first creating a new empty import batch for subsequent
dataset version exports).

After confirmation from the Data Vault API that the import has finished successfully, the service checks the size of the current top layer in the DANS Data
Vault. If it exceeds a configured threshold, the service sends a command to the Data Vault API to create a new top layer.

### CONFIRM-ARCHIVED

<!-- TODO: remove this and instead redefine archived as "added to the vault" ?-->

When the request has been sent to archive the import batch, the service waits for the Data Vault API to confirm that the import batch has been archived. It then
updates the status of all the dataset version exports in the import batch to `ARCHIVED` in the Vault Catalog.
