Development
===========
This page contains information for developers about how to contribute to this project.

Set-up
------
This project can be used in combination with  [dans-dev-tools]{:target=_blank}. Before you can start it as a service
some dependencies must first be started:

### Environment

Open a separate terminal tab for `dd-transfer-to-vault` and one for `dd-vault-catalog` and run:

```commandline
start-env.sh
```

### Database

Change DB port (e.g. to 9002) for one of the services

| file | value |
|------|-------|
| `etc/config.yml` | `url: jdbc:hsqldb:hsql://localhost:9002/dd-transfer-to-vault` |
| `etc/db.properties` | `server.port=9002` |

Run in both terminals:

```commandline
start-hsqldb-server.sh
```

### Start services

Open new terminals for both services and run:

```commandline
start-service.sh
```

## Prepare and start an ingest

To start an ingest, copy a zipped bag (e.g. from `src/test/resources/bags`) into one of the `transfer-inboxes` configured in  

    dd-transfer-to-vault/etc/config.yml

[dans-dev-tools]: https://github.com/DANS-KNAW/dans-dev-tools#dans-dev-tools
