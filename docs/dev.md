Development
===========
This page contains information for developers about how to contribute to this project.

Set-up
------
This project can be used in combination with  [dans-dev-tools]{:target=_blank}. Before you can start it as a service
some dependencies must first be started:

### Environment

Open a separate terminal tab for `dd-transfer-to-vault` and one for its dependency `dd-vault-catalog` and run:

```commandline
start-env.sh
```

### Database

Change DB port (e.g. to 9002) for one of the services

| file | value |
|------|-------|
| `etc/config.yml` | `url: jdbc:hsqldb:hsql://localhost:9002/dd-transfer-to-vault` |
| `etc/db.properties` | `server.port=9002` |

You may have to remove the directory `data/database` for a fresh up-to-date DB.

Run in both terminals:

```commandline
start-hsqldb-server.sh
```

To examine the database with a GUI you can run `start-hsqldb-client.sh`.
Copy the url from the table above into `file` - `connect` - `url`
The names of the tables should appear in the tree view panel.


### Start services

Open new terminals for both services and run:

```commandline
start-service.sh
```

## Start an ingest

To start an ingest, copy a zipped bag (e.g. from `src/test/resources/bags`) into one of the `transfer-inboxes` configured in  

    dd-transfer-to-vault/etc/config.yml

The zip will reappear with `-ttv1.zip` in `ocfl-tar-inbox` and a row will be added to the table `transfer_queue`.
To repeat with the same zip you will have to delete the row from the table.

[dans-dev-tools]: https://github.com/DANS-KNAW/dans-dev-tools#dans-dev-tools
