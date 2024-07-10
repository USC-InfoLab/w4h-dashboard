
# Docker Getting Started Guide

## Installation

Docker is available for Linux, Mac, and Windows. Install the appropriate version of Docker Desktop from [here](https://www.docker.com/products/docker-desktop).

## Installing the Image in Docker Desktop

1. Start the Docker Desktop application.
2. In Docker Desktop, search for `uscimsc/w4h` and select the preview-tagged image.
3. Click "pull" to download the image.

## Starting the Container

### Running the Container from Docker Desktop

When you load an image into memory, it becomes a running container. To run a container in Docker Desktop:Click on the "run" icon under "Actions" next to the image name.
![alt text](/static/docker_readme.png)
Click on "Optional Settings" and fill in the form as follows:
 ![alt text](/static/docker_readme2.png)
Finally, click the "Run" button to start the container.

### Running the Container from the Command Line as a Background Process

Alternatively, you can run a Docker image from the CLI, which is the preferred method for running Docker images.

**NOTE:** To use the Docker Command Line Interface (CLI) you may need to add `$HOME/.docker/bin` to your PATH (see note under Settings > Advanced in docker desktop).

When you start the container for the first time, docker will “pull” the image from Docker Hub and place a copy on your system. 

To start the container as a background process, execute:

```bash
docker run -p 5432:5432 -e POSTGRES_DB=admin -e POSTGRES_USER=admin -e POSTGRES_PASSWORD=admin -e hostname=db -dp 8501:8501 -p 8888:8888 --name w4h uscimsc/w4h:preview
```

### Running the Container from the Command Line in Verbose Mode

To start the container in verbose mode, execute in a terminal:

```bash
docker run -p 5432:5432 -e POSTGRES_DB=admin -e POSTGRES_USER=admin -e POSTGRES_PASSWORD=admin -e hostname=db -p 8501:8501 -p 8888:8888 --name w4h uscimsc/w4h:preview
```
The following is the truncated output:
```bash
docker run -p 5432:5432 -e POSTGRES_USER=admin -e POSTGRES_DB=admin -e POSTGRES_PASSWORD=admin -e hostname=db -p 8501:8501 uscimsc/w4h:sample
Unable to find image 'uscimsc/w4h:sample' locally
sample: Pulling from uscimsc/w4h
09f376ebb190: Already exists
276709cbedc1: Already exists
e5c23cad8c0c: Already exists
a56c5f373d66: Already exists
52a924435656: Already exists
44287a1b4544: Pull complete
faeff104f7d4: Pull complete
8348408b99d4: Pull complete
42f17385f3e6: Pull complete
f664f8d57e6f: Pull complete
fc8b5d5b936d: Pull complete
f0f33a64bd6a: Pull complete
d0ecff9210b4: Pull complete
fc27e1e5de2b: Pull complete
4a3bc7281b03: Pull complete
Digest: sha256:6475ddd657e2264751081ee7567367455deb876975ad3e862dbf2446a27d0eb0
Status: Downloaded newer image for uscimsc/w4h:sample
WARNING: The requested image's platform (linux/amd64) does not match the detected host platform (linux/arm64/v8) and no specific platform was requested
Starting PostgreSQL 15 database server: main.
...
Collecting usage statistics. To deactivate, set browser.gatherUsageStats to False.

 * Serving Flask app 'stream_sim' (lazy loading)
 * Environment: production
   WARNING: This is a development server. Do not use it in a production deployment.
   Use a production WSGI server instead.
 * Debug mode: off
WARNING: This is a development server. Do not use it in a production deployment. Use a production WSGI server instead.
 * Running on http://127.0.0.1:9977
Press CTRL+C to quit

  You can now view your Streamlit app in your browser.

  Network URL: http://172.17.0.2:8501
  External URL: http://64.98.216.199:8501
```
You can reach the dashboard at http://localhost:8501.
## Container Management

### Show All Running Containers

To show all currently running containers on your system, execute:

```bash
docker ps
```
The following is an example output:
```bash
CONTAINER ID   IMAGE                COMMAND                  CREATED        STATUS        PORTS                                            NAMES
8e1812539aa6   uscimsc/w4h:sample   "./inituser_and_star…"   23 hours ago   Up 23 hours   0.0.0.0:5432->5432/tcp, 0.0.0.0:8501->8501/tcp   kind_cohen
```

### Show All Stopped Containers

To show all stopped containers on your system, execute:

```bash
docker ps -filter "status=exited"
```
The following is an example output:
```bash
CONTAINER ID   IMAGE                COMMAND                  CREATED        STATUS        PORTS                                            NAMES
8e1812539aa6   uscimsc/w4h:sample   "./inituser_and_star…"   23 hours ago   Up 23 hours   0.0.0.0:5432->5432/tcp, 0.0.0.0:8501->8501/tcp   kind_cohen
```
### Stopping the Container

To stop a running container, execute:

```bash
docker stop <container id>
```

### Starting a Stopped Container

To restart a stopped container, execute:

```bash
docker start <container id>
```

### Removing a Container

To remove a Docker container from your system, execute:

```bash
docker rm <container id>
```

### Starting a Shell on a Running Container

To open a shell session on a running container, execute:

```bash
docker exec -it <container ID> sh
```

## Image Management

### Show All Images in Local Repository

To see all images on your local system, execute:

```bash
docker images
```
The following is an example of the output:
```bash
REPOSITORY    TAG       IMAGE ID       CREATED        SIZE
uscimsc/w4h   sample    9b5e51e6d1b8   25 hours ago   1.66GB
```

### Remove Image from Local Repository

To remove an image from your system, run:

```bash
docker rmi <image name>
```

## How to login dashboard

Then access `http://{your_server_ip}:8501/` to see the dashboard.

username: admin 
passward: admin

more details please check [Use W4h dashboard to track your patients' health data](https://github.com/USC-InfoLab/w4h-documentation/blob/main/docs/getting-started/how_to_start.md)