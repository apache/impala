-- foobar
/*this is a comment*/
SELECT 1+1 -- This comment continues to the end of line
;
SELECT 1 /* this is an in-line comment */ + 1;
SELECT 1+
/*
  this is a
  multiple-line comment
*/
1;
SELECT /* This comment block
is OK */ 2;
/* Trailing comment -1 */
-- Trailing comment -2
/*
-- query inside a comment
select 2;
/* nested comments end */
