FROM ubuntu:latest
COPY . .
WORKDIR /app
RUN sudo apt-get upadate && sudo apt-get install -y\
    sudo apt-get -y dist-upgrade\
    sudo apt-get -y install -y vim wget unzip ssh openssh-* net-tools
RUN pip install -r requirements.txt
ENTRYPOINT [ "python3", "main.py" ]
#CMD [ "main.py" ]