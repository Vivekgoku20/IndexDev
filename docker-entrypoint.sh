#!/bin/bash
set -e

# Wait for Redis to be ready
echo "Waiting for Redis..."
while ! nc -z ${REDIS_HOST:-redis} ${REDIS_PORT:-6379}; do
  sleep 1
done
echo "Redis is ready!"

FILENAME="stocks.db"
# Check if the file exists
if [ ! -f "$FILENAME" ]; then
  # If the file does not exist, create it using touch
  touch "$FILENAME"
  echo "File '$FILENAME' created."
else
  echo "File '$FILENAME' already exists."
fi

# Run the actual command passed to the script
exec "$@"
