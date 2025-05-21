INSTALLATION 
============

Currently, this project is built as an RPM package for OSes compatible with RHEL8 and later. The RPM will install the binaries to
`/opt/dans.knaw.nl/dd-transfer-to-vault` and the configuration files to `/etc/opt/dans.knaw.nl/dd-transfer-to-vault`.

For installation on systems that do no support RPM and/or systemd:

1. Build the tarball (see next section).
2. Extract it to some location on your system, for example `/opt/dans.knaw.nl/dd-transfer-to-vault`.
3. Start the service with the following command
   ```
   /opt/dans.knaw.nl/dd-transfer-to-vault/bin/dd-transfer-to-vault server /opt/dans.knaw.nl/dd-transfer-to-vault/cfg/config.yml 
   ```

BUILDING FROM SOURCE
====================
Prerequisites:

* Java 17 or higher
* Maven 3.6.3 or higher
* RPM

Steps:

    git clone https://github.com/DANS-KNAW/dd-transfer-to-vault.git
    cd dd-transfer-to-vault 
    mvn clean install

If the `rpm` executable is found at `/usr/local/bin/rpm`, the build profile that includes the RPM packaging will be activated. If `rpm` is available, but at a
different path, then activate it by using Maven's `-P` switch: `mvn -Pprm install`.

Alternatively, to build the tarball execute:

    mvn clean install assembly:single
