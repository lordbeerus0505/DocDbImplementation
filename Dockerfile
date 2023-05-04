FROM ubuntu:22.04
# LABEL about the custom image
LABEL maintainer="abhiram.natarajan@gmail.com"
LABEL version="0.1"
LABEL description="Simple docker image"
RUN apt-get update
RUN apt-get install -y python3
RUN apt-get install -y python3-pip
# Python installed, next install dependencies of the code base - 
RUN pip3 install pymongo
# Now copy the files - In the real world you'd want to git clone but thats not the case here as we are just shipping the image
COPY . /home/
WORKDIR "/home/"
CMD ["python3", "interface/cli_runner.py"]
