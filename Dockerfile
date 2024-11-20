FROM continuumio/anaconda3:latest

WORKDIR /home/fable
ENV PYTHONPATH=${PYTHONPATH}:/home/fable

# Install system dependencies
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    wget \
    curl \
    default-jdk \
    gcc \
    g++ \
    net-tools \
    sudo \
    procps \
    python3-dev \
    libxml2-dev \
    libxslt-dev \
    libjpeg-dev \
    zlib1g-dev \
    libpng-dev \
    libssl-dev \
    build-essential \
    libstdc++6 \
    libffi-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install Node.js
RUN curl -fsSL https://deb.nodesource.com/setup_18.x | bash - && \
    apt-get update && \
    apt-get install -y nodejs && \
    apt-get clean

# Install Chrome
RUN wget -q https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb && \
    apt-get update && \
    apt-get install -y ./google-chrome-stable_current_amd64.deb && \
    rm google-chrome-stable_current_amd64.deb && \
    apt-get clean

# Create conda environment
RUN conda create -n fable_env python=3.8 -y && \
    conda clean -a -y

# Install dependencies in the conda environment
SHELL ["conda", "run", "-n", "fable_env", "/bin/bash", "-c"]

# First install core packages
RUN pip install numpy==1.23.5 pandas==1.5.3 scikit-learn==0.23.2

# Try alternative methods for reppy
RUN pip install --no-deps robotexclusionrulesparser && \
    pip install requests-robotstxt

# Copy requirements and modify them
COPY requirements.txt /tmp/requirements.txt
RUN sed -i '/reppy/d' /tmp/requirements.txt && \
    pip install -r /tmp/requirements.txt

# Install npm packages
RUN npm install chrome-remote-interface chrome-launcher yargs && \
    npm install -g http-server

# Install boilerpipe
RUN mkdir -p deps && \
    git clone --depth 1 https://github.com/misja/python-boilerpipe.git deps/python-boilerpipe && \
    pip install -e deps/python-boilerpipe

# Copy the rest of the application
COPY . .

EXPOSE 8000

CMD ["conda", "run", "-n", "fable_env", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]