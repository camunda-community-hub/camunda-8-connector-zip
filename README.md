# camunda-8-connector-zip

Connector to execute ZIP and UNZIP operation



# Development

## Start locally the connector

Execute the class [LocalConnectorRuntime.java](src/test/java/io/camunda/zip/LocalConnectorRuntime.java). 
It starts a local connector, using the [application.yaml](src/test/resources/application.yaml) file to connect to the Zeebe server.


## Generate Element Template
Execute the class [ElementTemplateGenerator.java](src/test/java/io/camunda/csv/ElementTemplateGenerator.java) to generate the Element template.

The connector uses the Cherry generator to create a rich element template. 

## Generate documentation
Execute the class [DocumentationGenerator.java](src/test/java/io/camunda/csv/DocumentationGenerator.java) to generate the documentation to describe each function.

The connector uses the Cherry principle to create this documentation.