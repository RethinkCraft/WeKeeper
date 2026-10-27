FROM node:22-alpine AS frontend
WORKDIR /frontend
COPY frontend/package*.json ./
RUN npm ci
COPY frontend/ ./
ARG WEKEEPER_BASE_PATH=/
RUN npm run build -- --base=${WEKEEPER_BASE_PATH}

FROM python:3.11-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 WEKEEPER_DATA_DIR=/data WEKEEPER_STATIC_DIR=/app/web
WORKDIR /app
COPY deploy/dashboard/requirements.lock /app/requirements.lock
RUN pip install --no-cache-dir -r requirements.lock
COPY chatlog_keeper/ /app/chatlog_keeper/
COPY --from=frontend /frontend/dist /app/web
RUN useradd --uid 10001 --create-home wekeeper && mkdir /data && chown wekeeper:wekeeper /data
USER 10001:10001
EXPOSE 8000
ENTRYPOINT ["python", "-m", "chatlog_keeper.dashboard.cli"]
CMD ["serve", "--host", "0.0.0.0"]
