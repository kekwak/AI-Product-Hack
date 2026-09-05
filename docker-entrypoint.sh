#!/bin/sh
set -eu

python /app/backend/manage.py migrate --noinput
python /app/backend/manage.py collectstatic --noinput

exec "$@"
