FROM ubuntu:18.04
RUN apt update && apt install -y software-properties-common
RUN add-apt-repository ppa:deadsnakes/ppa
RUN apt update && apt install -y python3.8 python3-distutils curl wget net-tools unzip zip vim
RUN wget https://bootstrap.pypa.io/get-pip.py
RUN python3.8 get-pip.py
RUN pip3.8 "install snowflake-connector-python==3.1.0"
RUN echo "3"
COPY . /
RUN mkdir /logs
CMD ["python3.8", "/test.py"]
