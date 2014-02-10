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

clean:
	rm -f qless.lua qless-lib.lua

.PHONY: test
test: qless.lua *.lua
	nosetests --exe -v
