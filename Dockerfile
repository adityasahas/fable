FROM continuumio/anaconda3:2020.11

RUN mkdir -p /home/fable/deps
COPY . /home/fable
WORKDIR /home/fable

RUN mkdir -p /home/fable/logs && \
    chmod 777 /home/fable/logs

ENV PYTHONPATH=/home/fable

# Prepare
RUN mkdir -p /usr/share/man/man1

RUN echo "deb [trusted=yes] http://archive.debian.org/debian buster main" > /etc/apt/sources.list && \
    echo "Acquire::Check-Valid-Until false;" > /etc/apt/apt.conf.d/99no-check && \
    apt-get clean && \
    apt-get update --allow-unauthenticated
    
# Install Java and other basic tools
RUN apt-get install -y --no-install-recommends \
    wget \
    curl \
    openjdk-11-jdk \
    gcc g++ \
    net-tools sudo procps \
    dbus \
    dbus-x11 \
    libdbus-1-3 \
    libdbus-glib-1-2

RUN mkdir -p /var/run/dbus && \
    chown messagebus:messagebus /var/run/dbus && \
    chmod 755 /var/run/dbus && \
    dbus-uuidgen > /var/machine-id

# Install Node.js 14.x (more stable than 12.x)
RUN curl -sL https://deb.nodesource.com/setup_14.x | bash - && \
    apt-get install -y nodejs

# Install npm packages with specific versions
RUN npm install chrome-remote-interface@0.31.3 chrome-launcher@0.15.0 yargs@17.0.1
RUN npm install -g http-server

# Install Chrome
RUN wget -q https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb && \
    apt-get install -y ./google-chrome-stable_current_amd64.deb && \
    rm google-chrome-stable_current_amd64.deb

# Now set up conda environment
RUN conda config --set changeps1 false 
RUN conda create -n fable_env python=3.8 -y && \
    conda clean -a -y

# Add symlinks to make Node.js accessible in conda environment
RUN ln -s /usr/bin/node /opt/conda/envs/fable_env/bin/node && \
    ln -s /usr/bin/npm /opt/conda/envs/fable_env/bin/npm

# Activate conda environment
SHELL ["conda", "run", "-n", "fable_env", "/bin/bash", "-c"]

# Install required Python packages first
RUN pip install "lxml[html_clean]>=4.9.0"

# Install python dependencies
RUN pip install -r requirements.txt
# to use mongo srv urls
RUN pip install dnspython>=2.3.0 pymongo[srv,tls] certifi

# Install boilerpipe
RUN git clone https://github.com/misja/python-boilerpipe.git deps/python-boilerpipe && \
    pip install -e deps/python-boilerpipe

# Set Java environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

RUN node --version && \
    npm --version

ENV PORT=8080
EXPOSE 8080

COPY entrypoint.sh .
RUN chmod +x entrypoint.sh
ENTRYPOINT ["./entrypoint.sh"]