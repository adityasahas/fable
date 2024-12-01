FROM continuumio/anaconda3:2020.11

WORKDIR /home/fable
ENV PYTHONPATH=${PYTHONPATH}:/home/fable

# Install system dependencies with debian fix
RUN apt-get clean && \
    rm -rf /var/lib/apt/lists/* && \
    echo 'Acquire::Check-Valid-Until "false";' > /etc/apt/apt.conf.d/99no-check && \
    apt-get update -o Acquire::AllowInsecureRepositories=true && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --allow-unauthenticated --no-install-recommends \
    gcc \
    g++ \
    python3-dev \
    build-essential \
    libxml2-dev \
    libxslt-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Create conda environment
RUN conda create -n fable_env python=3.8 -y && \
    conda clean -a -y

# Activate conda environment
SHELL ["conda", "run", "-n", "fable_env", "/bin/bash", "-c"]

# Install lxml first
RUN pip install "lxml[html_clean]>=4.9.0"

# Copy and install requirements
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy application code
COPY . .

EXPOSE 8000

CMD ["conda", "run", "-n", "fable_env", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]