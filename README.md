# xml-to-postgres
A fast tool to convert XML files with repeating element sets into PostgreSQL dump format.

To use this tool you need to create a simple YAML configuration file that describes how to turn repeating element sets in an XML document into row-based data for importing into PostgreSQL. For efficiency, the data is output in PostgreSQL dump format, suitable for importing with the COPY command. This tool processes one row at a time and does not need to keep the whole XML DOM in memory, so it has a very low memory footprint and can be used to convert datasets much larger than the available RAM. The tool can split out further repeating fields into extra tables with a one-to-many relationship (with foreign key) to the main table.

## Features

 * XPath-like selection of column values
 * Very low runtime memory requirements
 * Read column values from XML attributes
 * Apply search-and-replace on values
 * Filter the output with regex
 * Write extra tables with a foreign key to the main table
