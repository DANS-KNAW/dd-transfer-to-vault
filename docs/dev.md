Development
===========

General information about developing DANS modules can be found [here](https://dans-knaw.github.io/dans-datastation-architecture/dev/){:target=_blank}.

Local testing
-------------
Local testing uses the same [set-up]{:target=_blank} as other DANS microservices.

[set-up]: https://dans-knaw.github.io/dans-datastation-architecture/dev/#local-testing

However, this service requires some other services to be running:

* [dd-validate-bagpack]{:target=_blank}
* [dd-vault-catalog]{:target=_blank}
* [dd-data-vault]{:target=_blank}

[dd-validate-bagpack]: {{ dd_validate_bagpack_url }}
[dd-vault-catalog]: {{ dd_vault_catalog_url }}
[dd-data-vault]: {{ dd_data_vault_url }}

What follows is a step-by-step instruction for how to process one test-deposit. This is not the only way you can do it, but it is a good starting point.

1. Start `dd-validate-bagpack`. Make sure that `validation.baseFolder` is set to the root folder of the `dd-transfer-to-vault` module. If you are using the
   `config.yml` copied by `start-env.sh`, the `validation.baseFolder` assumes that the `dd-transfer-to-vault` module is located in the same parent directory.
2. Start `dd-vault-catalog`. The default `config.yml` assumes that you have created a database named `dd_vault_catalog_local_test` on a machine called
   `dev.transfer.dans-data.nl` which is the case if you are using the DANS vagrant box for the transfer server. (Otherwise, you will have to create a database
   yourself and adjust the JDBC-config.)
3. Start `dd-data-vault`. The default `config.yml` assumes that you have created a database named `dd_data_vault_local_test` on a machine called
   `dev.transfer.dans-data.nl` which is the case if you are using the DANS vagrant box for the transfer server. (Otherwise, you will have to create a database
   yourself and adjust the JDBC-config.)
4. Start `dd-transfer-to-vault` itself.

The working directories of the service are located under `<dd-transfer-to-vault-root>/data/`:

```text
data
├── 01_transfer-inbox
│   ├── failed
│   └── inbox
├── 02_extract-metadata
│   ├── inbox
│   └── outbox
│       ├── failed
│       └── rejected
├── 03_send-to-vault
│   ├── inbox
│   ├── outbox
│   │   ├── failed
│   │   └── processed
│   └── work
├── 04_data-vault
│   └── inbox
├── dd-register-nbn
│   ├── failed
│   ├── inbox
│   └── processed
└── dd-transfer-to-vault.log
```

Once the service is running, you can copy test-DVEs to `01_transfer-inbox/inbox` and follow in the logs (or with the debugger) what happens. In a happy-case
scenario the DVE is collected from the inbox and placed in a directory named after the dataset's NBN in `02_extract-metadata/inbox`. The extract-metadata task
will then pick it up and process it, placing it in `03_send-to-vault/inbox`, also in a directory named after the dataset's NBN. It will also place a request to
register the NBN in `dd-register-nbn/inbox`, if the DVE is the first one for this dataset. 

Note that each step performs a ready check before starting to ensure that the services it depends on are available. For the send-to-vault step this means that
the `dd-data-vault` service API must be available. If you don't want `dd-data-vault` to pick up the result of send-to-vault, just configure it to look in a
different inbox directory (so **not** in `04_data-vault/inbox`).

### VaaS deposits require a skeleton record in the Vault Catalog
In the Vault-as-a-Service pipeline a skeleton-record is created for the DVE as soon as it arrives. This then also assigns an OCFL object version number to the 
DVE by including it in the file name. The fact that the OCFL object version number is included in the name signals to `dd-transfer-to-vault` to update an 
existing record rather than creating a new one. Therefore, before put a VaaS-type DVE in the inbox, you must add a skeleton record for it in the Data Vault.
This can be done with the `dd-vault-catalog-cli`:

```



```

