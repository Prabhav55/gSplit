FROM 963544309227.dkr.ecr.ap-south-1.amazonaws.com/base-img:glazedonut
WORKDIR /app_dev
RUN groupadd -r appgroup
RUN useradd -r -g appgroup spreadingUser
RUN usermod -aG sudo spreadingUser
COPY . /app_dev/
WORKDIR /app_dev/app
RUN sudo pip install --no-cache-dir -r ./requirements.txt
RUN sudo apt-get update \
    && sudo apt-get install poppler-utils=22.02.0-2ubuntu0.4 -y \
    && rm -rf /var/lib/apt/lists/*
RUN sudo chmod +x /app_dev/app/docker_entrypoint.sh
EXPOSE 9097
RUN chown -R spreadingUser /app_dev
RUN chown -R spreadingUser /home
RUN pip3 install --upgrade huggingface_hub==0.23.2
RUN pip install PyPDF2==3.0.1
RUN huggingface-cli login --token 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
USER spreadingUser
ENTRYPOINT ["/app_dev/app/docker_entrypoint.sh"]