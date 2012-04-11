require "qless/core/version"

module Qless
  module Core
    extend self
    LUA_SCRIPT_DIR = File.expand_path("../core/lua_scripts", __FILE__)

    def script_contents(name)
      File.read(File.join(LUA_SCRIPT_DIR, "#{name}.lua"))
    end
  end
end

