FROM gcc:12
WORKDIR /app
COPY . /app
RUN make
CMD ["./bank"]