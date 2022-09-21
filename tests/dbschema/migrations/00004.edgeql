CREATE MIGRATION m1dbwkvwhtzknevbddmswqrmbwkoecg6mwmcyfg45wl2jmmuif3u3a
    ONTO m1uxg5xjqqpae7pukq74ioelhvstsyxppago6tqopk2p32zcttrwjq
{
  CREATE TYPE default::WithConstraints {
      CREATE PROPERTY composite1 -> std::str;
      CREATE PROPERTY composite2 -> std::str;
      CREATE CONSTRAINT std::exclusive ON ((.composite1, .composite2));
      CREATE REQUIRED PROPERTY name -> std::str {
          CREATE CONSTRAINT std::exclusive;
      };
  };
};
