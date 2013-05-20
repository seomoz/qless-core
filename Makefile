all: qless.lua qless-lib.lua

qless-lib.lua: base.lua config.lua job.lua queue.lua recurring.lua worker.lua
	cat {base,config,job,queue,recurring,worker}.lua > qless-lib.lua

qless.lua: qless-lib.lua api.lua
	# Cat these files out, but remove all the comments from the source
	cat {qless-lib,api}.lua | \
		egrep -v '^[[:space:]]*--[^\[]' | \
		egrep -v '^--$$' > qless.lua

clean:
	rm -f qless{,-lib}.lua
