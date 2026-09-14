# cluster-agent — alert-triggered reconciler that argues back (docs: README.md)
FROM python:3.13-slim

# No kubectl. This process stopped making Kubernetes API calls when the tool
# surface moved to llm-gateway, and an image that carries a cluster CLI it never
# invokes is a 50MB invitation.

WORKDIR /srv
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app/ app/

EXPOSE 8080
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]
