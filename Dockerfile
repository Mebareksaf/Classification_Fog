FROM djx339/docker-python-opencv-ffmpeg:py3-cv3.4.0

WORKDIR /app

ADD ./requirements.txt ./ 

RUN wget https://github.com/edenhill/librdkafka/archive/refs/tags/v1.7.0.tar.gz && \
tar -xf v1.7.0.tar.gz && \
cd librdkafka-1.7.0/ && \
./configure --prefix=/usr && \  
make -j && \ 
make install
RUN cd ..

RUN pip install setuptools && \
pip install opencv-python==4.0.0.21 && \
pip install -r requirements.txt

ADD ./ ./

CMD ["python3", "consumer.py"] 
