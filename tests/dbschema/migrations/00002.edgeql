CREATE MIGRATION m16o3zi7bwy6zdcuq4nexyyctephv5ghqtrqanmywux6o5woutdlza
    ONTO m13n3l3xj2fzmqa5dqaoe32wl3bo3djbbxuou7d2vjocoeotgaj7gq
{
  CREATE TYPE default::Nested3 {
      CREATE PROPERTY name -> std::str;
  };
  CREATE TYPE default::Nested2 {
      CREATE LINK nested3 -> default::Nested3;
      CREATE PROPERTY name -> std::str;
  };
  CREATE TYPE default::Nested1 {
      CREATE LINK nested2 -> default::Nested2;
      CREATE PROPERTY name -> std::str;
  };
};
