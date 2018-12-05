FROM quay.io/jkremser/openshift-spark:2.3-latest
COPY deps.zip app.py /

CMD ["bash"]
