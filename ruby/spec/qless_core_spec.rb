require 'minitest/autorun'
require 'minitest/spec'
$LOAD_PATH.unshift "lib"
require 'qless/core'

module Qless
  describe Core do
    it "can read the lua files" do
      Qless::Core.script_contents("put").must_include "Put"
    end
  end
end

