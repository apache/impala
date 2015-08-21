-- Test substitution of command line argument
SELECT 'foo_number=' as name, 123${var:foo} as result;
SELECT 'foo_string=${var:foo}' as result;
-- Show set variables
SET;
-- Invalid variable reference
SELECT 'invalid_ref=${random_name}' as result;
-- Set variable
SET Var:myvar=foo123;
-- Use variable
SELECT 'var_test=${VAR:MYVAR}' as result;
-- Reference non-existing variables and options
SELECT 'missing_var_test=${var:foo1}${VAR:foo2}' as result;
-- Multiple replacements of the same variable
SELECT 'multi_test=${hivevar:BAR}_${var:Foo}_${var:BaR}_${HIVEvar:FOO}' as result;
-- Escaping variable substitution
SELECT 'This should be not replaced: \${VAR:foo} \${HIVEVAR:bar}';
-- Show set variables
SET;
-- Unset variables
UNSET VAR:foo;
UNSET VAR:BAR;
UNSET VAR:MyVar;
UNSET VAR:NonExistent;
-- Verify that all variables were unset
SET;
-- Test dash-dash comment removal
-- Multiple comments
-- multiple lines
SET var:comment_type1=ok;
/* Test removal
   of this type of
   comments */ SET var:comment_type2=ok;
-- Test the
   /* -- removal */ -- of 
   -- multiple comments */
set var:comment_type3=ok;
-- Check values
SET;
quit;
