# Use the official Jupyter Data Science Notebook as the base image
FROM jupyter/datascience-notebook:latest

# Switch to root to install system dependencies (if needed)
USER root

# Install system dependencies if required (uncomment if needed)
# RUN apt-get update && apt-get install -y \
#     package-name

# Switch back to the default Jupyter user
USER ${NB_UID}

# Install Python packages
RUN pip install confluent-kafka protobuf fastavro

# Set the working directory
WORKDIR /home/jovyan/work

# Expose the Jupyter port
EXPOSE 8888

# Start Jupyter Notebook
CMD ["jupyter", "notebook", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root"]