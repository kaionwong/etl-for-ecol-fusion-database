SELECT * FROM public.oracle_collisions
ORDER BY id ASC 




--- Get all column name and field type for a table
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'oracle_cl_objects'
order by column_name




--- Warning: Deletion
--- This command deletes the public schema and all objects (tables, views, functions, etc.) contained within it. Then it recreates the public schema as an empty schema.
DROP SCHEMA public CASCADE;
CREATE SCHEMA public;