# CPSC 6820 - Big Data project
## Overview
This codebase was built for the final project in a big data course.

## Instructions
The easiest way to get this project up and running is to create a Docker container. The app.dockerfile has all of the necessary  information to build a container. The recommended approach for building is using `docker-compose` and so a `docker-compose.yml` file has also been provided.

### Building the container
1. Assuming you have a Docker engine already installed on the machine you are wishing to deploy on, run `docker-compose up --build`.
1. Once all of the services are built you can enter the docker container with `docker-compose exec app /bin/bash`.
1. Once within the container, you can treat this a virtual machine that is mapped to directory where you isntalled this code. From here youc an issue your python commands such as `python main.py recommend-customer A1GIL64QK68WKL`.

**Note:** The Docker recipe includes support for a Python Flask project, but we do not use. We simply use the Flask server to keep the Docker container from closing so we can use it as a sandbox and less of an ad-hoc machine.

### Recalling the container
To recall the container in a different work session just issue `docker-compose up --build`. The image will be cached locally so the setup is a few seconds.
