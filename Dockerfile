FROM golang:latest

RUN apt-get update && apt-get upgrade -y 

COPY . .