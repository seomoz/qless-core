all: qless.lua

qless-lib.lua: base.lua config.lua job.lua queue.lua recurring.lua
	cat {base,config,job,queue,recurring}.lua > qless.lua

qless.lua: base.lua config.lua job.lua queue.lua recurring.lua api.lua
	# Cat these files out, but remove all the comments from the source
	cat {base,config,job,queue,recurring,api}.lua > qless.lua

clean:
	rm qless.lua
