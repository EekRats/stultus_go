FROM docker.io/library/debian

ARG DATABASE_URL=""
RUN apt update
RUN apt install -y git

RUN git clone --depth 1 https://github.com/ThisIsNotANamepng/search_engine.git

RUN apt install -y python3-pip python3 python3-venv
RUN python3 -m venv env
RUN /env/bin/pip install -r search_engine/requirments.txt

RUN mv /search_engine/* .
RUN ls
RUN mv /crawl /bin/crawl

#CMD ["/env/bin/python3", "scrape.py"]
CMD ["crawl"]