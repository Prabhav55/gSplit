#!/bin/bash

set -e

exec python3 cext1.py &
exec python3 cext2.py &
exec python3 cext3.py &
exec python3 csplit1.py &
exec python3 csplit2.py &
exec python3 csplit3.py &
exec python3 ttl_zcache.py