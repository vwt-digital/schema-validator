# schema-validator
This repository contains the code for the schema validator. It contains three main functions: compare-messages-to-schema, consume-data-catalog, consume-schema.

## consume-data-catalog
This function gets a data catalog from a topic and uploads it to the storage bucket defined in its config variables. The function is explained in more detail on [its page](https://github.com/vwt-digital/schema-validator/tree/develop/functions/consume_data_catalog).

## consume-schema
This function gets a schema from a topic and uploads it to the storage bucket defined in its config variables. The function is explained in more detail on [its page](https://github.com/vwt-digital/schema-validator/tree/develop/functions/consume_schema).

## compare-messages-to-schema
Every topic has a history bucket where the messages that have crossed the topic go to. This function compares the messages of said history bucket against the schema of the topic and creates JIRA tickets if there are messages that are not conform the schema. The function is explained in more detail on [its page](https://github.com/vwt-digital/schema-validator/tree/develop/functions/compare-messages-to-schema).
