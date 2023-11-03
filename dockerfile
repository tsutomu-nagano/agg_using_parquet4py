FROM python:3.10

# RUN apt-get -y update
# RUN apt-get install -y tzdata && apt-get clean && rm -rf /var/lib/apt/lists/*
# ENV TZ Asia/Tokyo

# RUN apt-get -y update && apt-get install -y wget

# RUN apt-get update && apt-get install -y language-pack-ja && \
#     update-locale LANG=ja_JP.UTF-8 && rm -rf /var/lib/apt/lists/*
# ENV LANG ja_JP.UTF-8
# ENV LANGUAGE ja_JP:ja
# ENV LC_ALL ja_JP.UTF-8


WORKDIR /workspaces

