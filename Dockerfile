FROM continuumio/anaconda3:2020.11

RUN mkdir -p /home/fable/deps
COPY . /home/fable
WORKDIR /home/fable
ENV PYTHONPATH=${PYTHONPATH}:/home/fable

# Prepare
RUN mkdir -p /usr/share/man/man1

# Install Java and other basic tools first
RUN echo 'Acquire::Check-Valid-Until "false";' > /etc/apt/apt.conf.d/99no-check && \
    apt-get update --allow-releaseinfo-change && \
    apt-get install -y \
    wget \
    curl \
    openjdk-11-jdk \
    gcc g++ \
    net-tools sudo procps

# Install Node.js 14.x (more stable than 12.x)
RUN curl -fsSL https://deb.nodesource.com/setup_14.x | bash - && \
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

# Activate conda environment
SHELL ["conda", "run", "-n", "fable_env", "/bin/bash", "-c"]

# Install required Python packages first
RUN pip install "lxml[html_clean]>=4.9.0"

# Install python dependencies
RUN pip install -r requirements.txt

# Install boilerpipe
RUN git clone https://github.com/misja/python-boilerpipe.git deps/python-boilerpipe && \
    pip install -e deps/python-boilerpipe

# Set Java environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

EXPOSE 8000

COPY entrypoint.sh .
RUN chmod +x entrypoint.sh
ENTRYPOINT ["./entrypoint.sh"]