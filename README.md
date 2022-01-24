# xml-to-postgres
A fast tool to convert XML files with repeating element sets into PostgreSQL dump format.

To use this tool you need to create a simple YAML configuration file that describes how to turn repeating element sets in an XML document into row-based data for importing into PostgreSQL. For efficiency, the data is output in PostgreSQL dump format, suitable for importing with the COPY command. This tool processes one row at a time and does not need to keep the whole XML DOM in memory, so it has a very low memory footprint and can be used to convert datasets larger than the available RAM. The tool can split out further repeating fields into extra tables with a one-to-many relationship (with foreign key) to the main table.

## Features

 * XPath-like selection of column values
 * Very low runtime memory requirements
 * Read column values from XML attributes
 * Apply search-and-replace on values
 * Filter the output with regex
 * Write extra tables with a foreign key to the main table
 * Operate in a pipeline to avoid on-disk intermediary steps

## Compiling

This project uses the Rust 2021 Edition, which means you need at a minimum to have the Rust 1.56 toolchain installed. The project uses only stable features and will only add dependencies that can compile on stable. It's a normal Rust project managed by Cargo, so you can compile with this simple command:

    cargo build --release

The debug build really hurts performance, so unless you're doing a deep dive in the code it is recommended to compile for release.

## Running

Basic usage:

    xml-to-postgres <config.yml> [data.xml]

So the YAML configuration file is a required argument. The XML input file can be passed in as the second argument or can be sent to stdin if omitted.

Example invocation:

    xml-to-postgres config.yml data.xml > data.dump

Within a pipeline:

    unzip -p xml.zip | xml-to-postgres config.yml | psql <database> -c '\copy <table> from stdin'

Within a database transaction:

    xml-to-postgres config.yml data.xml | psql <database> -c 'BEGIN' -c 'TRUNCATE <table>' -c '\copy <table> from stdin' -c 'COMMIT'

## Configuration

See [documentation for the configuration file](https://github.com/bartnv/xml-to-postgres/wiki/Configuration-options) in the wiki.
