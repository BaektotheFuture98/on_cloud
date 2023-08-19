FROM ubuntu:latest
COPY . .
WORKDIR /app
RUN sudo apt-get upadate && sudo apt-get install -y upgrade
RUN pip install -r requirements.txt
ENTRYPOINT [ "python3", "main.py" ]
#CMD [ "main.py" ]