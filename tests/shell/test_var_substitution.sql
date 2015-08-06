-- Test substitution of command line argument
SELECT 'foo_number=' as name, 123${var:foo} as result;
SELECT 'foo_string=${var:foo}' as result;
-- Show set variables
SELECT 'discard-comments';
SET;
-- Set and use option
SELECT 'discard-comments';
-- Invalid variable reference
SELECT 'invalid_ref=${random_name}' as result;
-- Reference non-existing variables and options
SELECT 'missing_var_test=${var:foo1}${VAR:foo2}' as result;
-- Multiple replacements of the same variable
SELECT 'multi_test=${hivevar:BAR}_${var:Foo}_${var:BaR}_${HIVEvar:FOO}' as result;
-- Escaping variable substitution
SELECT 'This should be not replaced: \${VAR:foo} \${HIVEVAR:bar}';
quit;
