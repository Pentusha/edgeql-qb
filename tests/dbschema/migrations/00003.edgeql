CREATE MIGRATION m1uxg5xjqqpae7pukq74ioelhvstsyxppago6tqopk2p32zcttrwjq
    ONTO m16o3zi7bwy6zdcuq4nexyyctephv5ghqtrqanmywux6o5woutdlza
{
  CREATE FUNCTION default::exclamation(word: std::str) ->  std::str USING ((word ++ '!'));
};
