#!/usr/bin/env bash
pkill -f "volc-redis-item-cache.jar" && echo "app stopped"
redis-cli -p 6390 shutdown nosave 2>/dev/null && echo "redis stopped"
exit 0
