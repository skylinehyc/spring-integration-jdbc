This project shows how to use spring integration dsl to load a csv into database.

It demonstrates the usage of following frameworks - 

1. Spring Integraton DSL
  a. Service Activator Example
  b. Executor Channel - Multithreading
  c. Splitter - Split payload into batches.
  d. Message Handler Bean Example

2. Flyway

Performance 

Infrastructure - Windows 7 on dual core
Loads 30000 csv records in 2.5 seconds. Batch Size of 5000 using 4 threads.

You can configure filepath, filename, batch size, number of threads in application.yml

To compile, download the project and give following command in root folder 
mvn install
To run, give following command in root folder
java -jar target/spring-integration-jdbc-0.0.1.jar
mvn install 
