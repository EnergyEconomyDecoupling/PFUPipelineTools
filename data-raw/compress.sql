-- This function compresses versions in the Mexer database.
-- It is used with PFUPipelineTools::create_compress_function()
CREATE OR REPLACE PROCEDURE compress(target TEXT, version_from_col TEXT, version_to_col TEXT)
LANGUAGE plpgsql
AS $$
  DECLARE
query text;
cols text;
BEGIN
-- quoting all identifiers for safety
SELECT STRING_AGG(QUOTE_IDENT(column_name), ',')
INTO cols
FROM information_schema.COLUMNS
WHERE table_schema = 'public' AND table_name = target
AND column_name NOT IN (version_from_col, version_to_col);

-- execute in one transaction for atomicity
BEGIN
-- ensure table is locked to prevent race condition
query := FORMAT('LOCK TABLE %I IN ACCESS EXCLUSIVE MODE; ' ||

                  'CREATE TEMPORARY TABLE compressed_set AS (' ||
                  'SELECT MIN(%I), MAX(%I), %s ' ||
                  'FROM %I GROUP BY %s); ' ||

                  'TRUNCATE TABLE %I; ' ||

                  'INSERT INTO %I (%I, %I, %s) ' ||
                  'SELECT * FROM compressed_set; ' ||

                  'DROP TABLE compressed_set;',
                target,
                version_from_col, version_to_col, cols,
                target, cols,
                target,
                target, version_from_col, version_to_col, cols);

EXECUTE query;

-- catch exceptions in transaction and revert for safety
EXCEPTION WHEN OTHERS THEN
ROLLBACK;
RAISE;
END;
COMMIT;
END $$;
