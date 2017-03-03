all: qless.lua qless-lib.lua

qless-lib.lua: base.lua config.lua job.lua queue.lua recurring.lua worker.lua
	echo "-- Current SHA: `git rev-parse HEAD`" > qless-lib.lua
	echo "-- This is a generated file" >> qless-lib.lua
	cat base.lua config.lua job.lua queue.lua recurring.lua worker.lua >> qless-lib.lua

qless.lua: qless-lib.lua api.lua
	# Cat these files out, but remove all the comments from the source
	echo "-- Current SHA: `git rev-parse HEAD`" > qless.lua
	echo "-- This is a generated file" >> qless.lua
	cat qless-lib.lua api.lua | \
		egrep -v '^[[:space:]]*--[^\[]' | \
		egrep -v '^--$$' >> qless.lua

REDIS_VERSION ?= stable
REDIS_DIR = redis-$(REDIS_VERSION)
REDIS_TAR = redis-$(REDIS_VERSION).tar.gz
REDIS_BIN = $(REDIS_DIR)/src/redis-server

.PHONY: clean test redis
clean:
	rm -rf qless.lua qless-lib.lua $(REDIS_TAR) $(REDIS_DIR)

test: qless.lua *.lua
	nosetests --exe -v

$(REDIS_TAR):
	curl -O http://download.redis.io/releases/$(REDIS_TAR)

$(REDIS_DIR): $(REDIS_TAR)
	tar xvf $(REDIS_TAR)

$(REDIS_BIN): $(REDIS_DIR)
	cd $(REDIS_DIR) && make

redis: $(REDIS_BIN)
	$(REDIS_BIN) --daemonize yes
