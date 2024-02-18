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

