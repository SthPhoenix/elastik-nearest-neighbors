#!/bin/bash

docker run --rm -v "$PWD":/home/gradle/project -w /home/gradle/project gradle:5.5.1-jdk12 gradle assemble
