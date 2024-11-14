#!/bin/bash

# * ROOT_USER=1 used to make chrome being able to run with ROOT
sudo docker run -it --env ROOT_USER=1 \
     --name fable fable
