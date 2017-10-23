# PysparkTestingExample

## Purpose:
The goal of this repo is to provide a set of examples on how spark code can be
composed for testability.  

The primary goal here is to build data pipelines, however the examples should also 
work for ML and other spark code purposes.  

With that in mind the primary purpose is providing a framework to produce testable pipeline stages.

We do not discuss orchestrating anything between those stages as that would be a cross domain concern.

The goal for local development should generally be for writing discrete pipeline code that primarily executes unit tests

We have provided the ability to load a local development environment for limited local execution, however
in any realistic situation most data loads would never succeed locally due to network and memory constraints. The next 
step of the deployment pipeline would be to deliver the code to a CI server that will then
execute the tests and then ideally, on test success, deploy the artifact to a test environment where it could be
tested with production data volumes.  

One of the more interesting caveats has been the need for generic pyspark code to be able to find 
a spark to attach too.  To this end we expect some significant effort to be put into the `jobs/libs/bootstrap.py`
file in order to be able to handle multiple executiong environments.  IE local development will bootstrap
differently from production.  In an ideal world CI would bootstrap identically to production however that may not be 
easily possible depending on your environment.

## Layout:
Our experience is that a set of related jobs will live in a single repository that 
represents a single bounded context.  IE an integration such as Salesforce will have it's 
data pipeline components living together inside of a given repository.

Our suggested layout looks like this:
* Root directory:
  * local_dev_env:
    * We put our local development environment variables here in the format of 
      * `export SPARK_MASTER=local[*]`
      * When the make file executes it will source all files found in ~/env/*.env file into the shell of the executing process
      * The .gitignore file has `env/secrets.env` to hopefully prevent checking in of secrets.
  * jobs:
    * libs: Shared library code, we would expect to see connection string building code and various utility functions here.
      * tests: Tests on libs code
      * bootstrap.py: 
        * We expect to contain the code that finds a spark context.  It will need to be aware of the various environment
        the code may be executing in.  
          * In local development it should rely on `pip install pyspark` generally speaing
          * In CI it should be able to find or install pyspark / spark as needed.
          * For production it should be configured to load appropriately based on your infra.
    * <job_folders>: This is where we expect to put the code for our individual pipelines.  We have an opinionated layout for 
    this code.  However it's still just an opinion and should be treated with skepticism when the layout does not match
    your use case.
      * tests: Tests on pipeline code.
        * We generally expect to see tests on ALL sections of the pipeline.  IE there should be tests
        on orchestration.py, sources.py and transformations.py.
        * The sample_job will include examples of tests on all components (eventually)
      * orchestration.py: The main module that expresses the pipeline flow.  IE it orchestrates 
      data from sources through a set of transformations and ultimately to it's destination.
      * destinations.py: output destinations for completed pipelines.  Could be parquet files on local filesystems, 
      parquet on S3 (or some other networked location) or JDBC writers. We could also have an event
      stream as a destination here.  
      * sources.py: input sources for pipeline data.  Expected sources could be: S3 buckets (parquet, JSON)
      JDBC DB connections.  In theory we could have an event stream here as well, (todo: Add sample for inbound 
      event streams)
## Components:
* Makefile 
  * `make test` will run all tests.
    * `make test ./job/<job_directory>` will run just the tests in the given path (eventually)
  * `make setup` will install requirements
  * `make execute ./<path>` will execute the given orchestration