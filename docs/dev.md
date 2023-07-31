Development
===========
This page contains information for developers about how to contribute to this project.

Set-up
------
This project can be used in combination with  [dans-dev-tools]{:target=_blank}. Before you can start it as a service
some dependencies must first be started:

### Environment

Open a separate terminal tab for `dd-transfer-to-vault` and one for `dd-vault-catalog`:

```commandline
start-env.sh
```

Change DB port (e.g. to 9002) for one of the services

| file | value |
|------|-------|
| `etc/config.yml` | `url: jdbc:hsqldb:hsql://localhost:9002/dd-transfer-to-vault` |
| `etc/db.properties` | `server.port=9002` |

### HSQL database

Open a separate terminal tab for dd-transfer-to-vault and one for dd-vault-catalog:

```commandline
start-hsqldb-server.sh
```

### start services

Return to the terminals used under [Environment](#environment) do the following for each:

```commandline
start-service.sh
```

## Prepare and start an ingest

Once the dependencies and services are started you can ingest a single deposit by moving
(not copy) a deposit (such as a zip of `src/test/resources/bags`) into `inboxes` configured in  

    dd-transfer-to-vault/etc/config.yml

[dans-dev-tools]: https://github.com/DANS-KNAW/dans-dev-tools#dans-dev-tools
