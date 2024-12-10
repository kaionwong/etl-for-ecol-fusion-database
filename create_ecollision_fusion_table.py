# This creates all the tables for eCollision Fusion DB. The specification and formats of these tables will be the same as eCollision Analytics DB's.
# For the Collisions table, 1) use vw_valid_collision_from_oracle, 2) apply this as a filter to include only those collisions that are on the valid list, 
# 3) format fields to match that of eCollision Analytics (according to supplementary/column_mapping_btw_analytics_and_oracle_tables.xlsx), 
# 4) import into Fusion's Collisions table
# For all other tables, do the same except there is no need to apply the filter on the Collisions table

