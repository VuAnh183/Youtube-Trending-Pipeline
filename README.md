# Youtube-Trending-Pipeline


## Table of contents

* [Introduction](#introduction)
* [Installation](#installation)
* [Quick start](#quick-start)
* [Usage](#usage)



## Introduction
A pipeline to fetch daily trending YouTube videos, clean & store the data, and visualize trends

![Alt text](System_Architect.png)
Quick summary of the project:
- **ELT Pipeline**:
    + **Extraction:** Ingesting raw data(json) from **Youtube API**.
    + **Transform**: Utilize python libraries such as **pandas**, **numpy** to transform data
    + **Load**: Create corresponding schema and store transformed data into **PostgreSQL**
    + **Visualize**: Visualizing processed data using **Metabase**
- **Containerization**:
    + This project will be running inside a **Docker** container


## Installation
For this project, you need to have **Docker Dekstop** and **Python** installed on your device.

Go to this link [https://docs.docker.com/get-started/get-docker/] and follow the official instruction to download Docker

## Quick start
Pull the repo to your local machine:

```sh
git pull https://github.com/VuAnh183/Youtube-Trending-Pipeline
```

Next, install all the required Python libraries using this command:

```sh
pip install -r requirements.txt
```

To run start the project, open **Docker Desktop** and run this command:
```sh
docker compose --env-file $(ENV_FILE)
```