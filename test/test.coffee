
Mocha = require("mocha")

mocha = new Mocha();
mocha.addFile("test/tests/disker.coffee");

mocha.run();