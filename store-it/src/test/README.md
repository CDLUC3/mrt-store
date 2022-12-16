## Test data

There are 3 checkm files in source-it/src/test/resources.

These checkm files reference content that can be served up from the docker image mock-merritt-it defined in the merritt-docker repository.

## To test with maven

The maven plugin will assign random high port numbers to the containers that have been started.
```
mvn verify
```

## To run from the command line or in a debugger

Make sure that the war is up to date
```
mvn install -Ddocker.skip -DskipITs -Dmaven.test.skip=true
```

If running on a desktop
```
ECR_REGISTRY=it-docker-registry
```

Launch Containers
```
MDIR=$(pwd) docker-compose -f store-it/src/test/docker/docker-compose.yml up -d
```

Run the junit test in VSCode

Stop Containers
```
MDIR=$(pwd) docker-compose -f store-it/src/test/docker/docker-compose.yml down
```
